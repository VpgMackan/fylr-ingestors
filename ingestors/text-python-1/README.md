# Text Python Ingestor v1

A Python-based ingestor for processing text-based files (PDF, Markdown, Plain Text) in the fylr ingestors system.

## Features

- **Multiple Format Support**: Handles PDF, Markdown, and plain text files
- **Text Extraction**: Extracts text content from supported file formats
- **Embedding Generation**: Creates vector embeddings using Sentence Transformers
- **RabbitMQ Integration**: Listens for jobs and publishes status updates
- **S3 Integration**: Downloads files from S3 for processing
- **Chunking**: Splits large texts into manageable chunks for embedding

## Supported MIME Types

- `application/pdf` - PDF documents
- `text/markdown` - Markdown files
- `text/plain` - Plain text files

## Architecture

The ingestor follows this processing flow:

1. **Listen**: Consumes messages from RabbitMQ queue based on configured routing keys
2. **Download**: Retrieves the file from S3 using the provided key
3. **Extract**: Uses appropriate handler to extract text from the file
4. **Chunk**: Splits text into chunks (default 500 characters)
5. **Embed**: Generates vector embeddings for each chunk
6. **Store**: Saves chunks and embeddings to database (implementation needed)
7. **Notify**: Publishes status updates throughout the process

## Configuration

Copy `.env.example` to `.env` and configure the following variables:

### RabbitMQ Settings
- `RABBITMQ_HOST`: RabbitMQ server host (default: localhost)
- `RABBITMQ_PORT`: RabbitMQ server port (default: 5672)
- `RABBITMQ_USER`: RabbitMQ username (default: guest)
- `RABBITMQ_PASSWORD`: RabbitMQ password (default: guest)

### Ingestor Settings
- `INGESTOR_ROUTING_KEYS`: Comma-separated routing keys to listen to
- `INGESTOR_QUEUE_NAME`: Queue name for this ingestor

### AWS Settings
- `AWS_REGION`: AWS region (default: us-east-1)
- `S3_BUCKET`: S3 bucket name for file storage (required)

### Model Settings
- `EMBEDDING_MODEL`: Sentence Transformer model name (default: all-MiniLM-L6-v2)

## Installation

### Using Docker

```bash
docker build -t text-python-1-ingestor .
docker run --env-file .env text-python-1-ingestor
```

### Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .

# Run the ingestor
python src/main.py
```

## Message Format

The ingestor expects messages with the following structure:

```json
{
  "sourceId": "unique-source-identifier",
  "mimeType": "application/pdf",
  "s3Key": "path/to/file/in/s3.pdf"
}
```

## Status Updates

The ingestor publishes status updates to the exchange with routing key `status.update`:

```json
{
  "sourceId": "unique-source-identifier",
  "status": "PROCESSING|COMPLETED|FAILED",
  "message": "Human-readable status message"
}
```

## Handlers

### PDF Handler (`handlers/pdf.py`)
- Uses `pypdf` library to extract text from PDF files
- Extracts text from all pages and concatenates them

### Markdown/Text Handler (`handlers/markdown.py`)
- Handles both markdown and plain text files
- Supports UTF-8 and Latin-1 encodings
- Removes null bytes and normalizes whitespace

## Extending

To add support for new file types:

1. Create a new handler in `src/handlers/`
2. Implement a function that takes `bytes` and returns extracted text
3. Register the handler in `HandlerManager` (`src/handlers/__init__.py`)
4. Add the corresponding routing key to your configuration

## Database Integration

The current implementation includes a placeholder for database operations. To complete the integration:

1. Add your database client library to `pyproject.toml`
2. Initialize the database connection in `main.py`
3. Implement the `save_to_db` function to store chunks and embeddings
4. Update the callback function to call your database save logic

## Development

### Project Structure

```
text-python-1/
├── Dockerfile
├── pyproject.toml
├── .env.example
├── README.md
└── src/
    ├── main.py              # Main application logic
    └── handlers/
        ├── __init__.py      # HandlerManager
        ├── markdown.py      # Markdown/text handler
        └── pdf.py           # PDF handler
```

## License

[Your License Here]
