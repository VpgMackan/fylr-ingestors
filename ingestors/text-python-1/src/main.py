import os
import pika
import sys
import json
import boto3
from sentence_transformers import SentenceTransformer
from handlers import HandlerManager

ROUTING_KEYS_STR = os.getenv("INGESTOR_ROUTING_KEYS")
QUEUE_NAME = os.getenv("INGESTOR_QUEUE_NAME")
EXCHANGE_NAME = "file-processing-exchange"
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

if not ROUTING_KEYS_STR or not QUEUE_NAME:
    sys.exit("Error: INGESTOR_ROUTING_KEYS and INGESTOR_QUEUE_NAME must be set.")

if not S3_BUCKET:
    sys.exit("Error: S3_BUCKET must be set.")

ROUTING_KEYS = [key.strip() for key in ROUTING_KEYS_STR.split(",")]
handler_manager = HandlerManager()

# Initialize S3 client
s3_client = boto3.client("s3", region_name=AWS_REGION)

# Initialize embedding model
print(f"Loading embedding model: {EMBEDDING_MODEL}")
embedding_model = SentenceTransformer(EMBEDDING_MODEL)
print("Embedding model loaded successfully")


def download_from_s3(s3_key: str) -> bytes:
    """Download file from S3."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        return response["Body"].read()
    except Exception as e:
        raise Exception(f"Failed to download file from S3: {e}")


def publish_status(channel, source_id: str, status: str, message: str = ""):
    """Publish status update to RabbitMQ."""
    status_message = {"sourceId": source_id, "status": status, "message": message}
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key="status.update",
        body=json.dumps(status_message),
    )


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
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for routing_key in ROUTING_KEYS:
        channel.queue_bind(
            exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=routing_key
        )
        print(f"Binding queue '{QUEUE_NAME}' to routing key '{routing_key}'")

    print(f"Ingestor online. Listening for jobs on queue '{QUEUE_NAME}'...")

    def callback(ch, method, properties, body):
        routing_key = method.routing_key
        message = json.loads(body)
        mime_type = message.get("mimeType")
        source_id = message.get("sourceId")
        s3_key = message.get("s3Key")

        print(
            f"Received message with routing key '{routing_key}' for source {source_id}"
        )

        try:
            # Update status to PROCESSING
            publish_status(ch, source_id, "PROCESSING", "Starting text extraction")

            # 1. Download file from S3
            print(f"Downloading file from S3: {s3_key}")
            file_content = download_from_s3(s3_key)

            # 2. Extract text using appropriate handler
            print(f"Extracting text using handler for mime type: {mime_type}")
            extracted_text = handler_manager.process_data(mime_type, file_content)

            if not extracted_text:
                raise ValueError("No text content extracted from file")

            print(f"Extracted {len(extracted_text)} characters of text")

            # 3. Generate embeddings
            print("Generating embeddings...")
            publish_status(ch, source_id, "PROCESSING", "Generating embeddings")

            # Split text into chunks (simple approach - you might want more sophisticated chunking)
            chunk_size = 500
            chunks = []
            for i in range(0, len(extracted_text), chunk_size):
                chunk = extracted_text[i : i + chunk_size]
                if chunk.strip():
                    chunks.append(chunk)

            print(f"Split text into {len(chunks)} chunks")

            # Generate embeddings for each chunk
            embeddings = embedding_model.encode(chunks)
            print(f"Generated {len(embeddings)} embeddings")

            # 4. Save to database (placeholder - you'll need to implement your DB logic)
            # For now, we'll just log what would be saved
            print(
                f"Would save {len(chunks)} chunks with embeddings to database for source {source_id}"
            )
            # Example structure:
            # for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            #     save_to_db(source_id, idx, chunk, embedding.tolist())

            # Update status to COMPLETED
            publish_status(
                ch,
                source_id,
                "COMPLETED",
                f"Successfully processed {len(chunks)} text chunks",
            )
            print(f"Successfully processed message for mime_type: {mime_type}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            error_msg = f"Error processing message: {e}"
            print(error_msg)
            publish_status(ch, source_id, "FAILED", str(e))
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    channel.start_consuming()


if __name__ == "__main__":
    main()
