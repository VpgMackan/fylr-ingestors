import os
import pika
import sys
import json
import boto3
import requests
from botocore.config import Config
from langchain_text_splitters import RecursiveCharacterTextSplitter
from dotenv import load_dotenv

from handlers import HandlerManager
from database import get_db_session, Source, DocumentVector

load_dotenv()

ROUTING_KEYS_STR = os.getenv("INGESTOR_ROUTING_KEYS")
QUEUE_NAME = os.getenv("INGESTOR_QUEUE_NAME")
EXCHANGE_NAME = "file-processing-exchange"
EVENTS_EXCHANGE_NAME = "fylr-events"

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_PORT = os.getenv("S3_PORT")
S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_REGION = os.getenv("S3_REGION")
S3_BUCKET_USER_FILE = os.getenv("S3_BUCKET_USER_FILE")

AI_GATEWAY_URL = os.getenv("AI_GATEWAY_URL")

if not all([ROUTING_KEYS_STR, QUEUE_NAME, S3_BUCKET_USER_FILE, AI_GATEWAY_URL]):
    sys.exit("Error: Missing one or more required environment variables.")

ROUTING_KEYS = [key.strip() for key in ROUTING_KEYS_STR.split(",")]
handler_manager = HandlerManager()

s3 = boto3.resource(
    "s3",
    aws_access_key_id=S3_KEY_ID,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
    endpoint_url=f"http://{S3_ENDPOINT}:{S3_PORT}",
    config=Config(s3={"addressing_style": "path"}),
)
s3_bucket = s3.Bucket(S3_BUCKET_USER_FILE)

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000, chunk_overlap=200, add_start_index=True
)


def publish_status(channel, job_key, stage, message, error=False):
    """Publishes a status update to the fylr-events exchange."""
    payload = {"stage": stage, "message": message, "error": error}
    routing_key = f"job.{job_key}.status"
    channel.basic_publish(
        exchange=EVENTS_EXCHANGE_NAME,
        routing_key=routing_key,
        body=json.dumps({"eventName": "jobStatusUpdate", "payload": payload}),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"[{job_key}] Status: {stage} - {message}")


def get_embeddings(chunks: list[str], model: str) -> list[list[float]]:
    """Calls the AI Gateway to get embeddings."""
    response = requests.post(
        f"{AI_GATEWAY_URL}/v1/embeddings",
        json={"provider": "jina", "model": model, "input": chunks},
    )
    response.raise_for_status()
    return [item["embedding"] for item in response.json()["data"]]


def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials
        )
    )
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE_NAME, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        exchange=EVENTS_EXCHANGE_NAME, exchange_type="topic", durable=True
    )
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for rk in ROUTING_KEYS:
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=rk)
        print(f"Binding queue '{QUEUE_NAME}' to routing key '{rk}'")

    print(f"Ingestor online. Listening on queue '{QUEUE_NAME}'...")

    def callback(ch, method, properties, body):
        message = json.loads(body)
        source_id = message.get("sourceId")
        s3_key = message.get("s3Key")
        mime_type = message.get("mimeType")
        job_key = message.get("jobKey")
        embedding_model = message.get("embeddingModel")

        try:
            publish_status(ch, job_key, "STARTING", "Processing started.")

            # 1. Download from S3
            publish_status(ch, job_key, "FETCHING", "Downloading file from storage.")
            obj = s3_bucket.Object(s3_key)
            file_content = obj.get()["Body"].read()

            # 2. Extract Text
            publish_status(ch, job_key, "PARSING", f"Parsing {mime_type} file.")
            text = handler_manager.process_data(mime_type, file_content)
            if not text or not text.strip():
                raise ValueError("No text could be extracted from the file.")

            # 3. Chunk Text
            docs = text_splitter.create_documents([text])
            chunks = [doc.page_content for doc in docs]
            publish_status(
                ch, job_key, "VECTORIZING", f"Split text into {len(chunks)} chunks."
            )

            # 4. Get Embeddings
            embeddings = get_embeddings(chunks, embedding_model)
            if len(embeddings) != len(chunks):
                raise Exception("Mismatch between number of chunks and embeddings.")

            # 5. Save to Database
            with get_db_session() as db:
                db.query(DocumentVector).filter(
                    DocumentVector.file_id == source_id
                ).delete()

                vectors = []
                for i, doc in enumerate(docs):
                    vectors.append(
                        DocumentVector(
                            id=f"vec_{source_id}_{i}",
                            file_id=source_id,
                            content=doc.page_content,
                            embedding=embeddings[i],
                            chunk_index=doc.metadata.get("start_index", i),
                        )
                    )

                db.add_all(vectors)

                source = db.query(Source).filter(Source.id == source_id).one()
                source.status = "COMPLETED"
                source.ingestor_type = "text-python"
                source.ingestor_version = "1.0.0"

            publish_status(
                ch, job_key, "COMPLETED", "Processing finished successfully."
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(
                f"Error processing message for source {source_id}: {e}", file=sys.stderr
            )
            publish_status(ch, job_key, "FAILED", str(e), error=True)
            with get_db_session() as db:
                source = db.query(Source).filter(Source.id == source_id).first()
                if source:
                    source.status = "FAILED"
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    channel.start_consuming()


if __name__ == "__main__":
    main()
