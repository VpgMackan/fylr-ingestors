from typing import Dict, Callable
from .markdown import handle_markdown
from .pdf import handle_pdf


class HandlerManager:
    """Manages different file type handlers."""

    def __init__(self):
        self.handlers: Dict[str, Callable] = {
            "text/markdown": handle_markdown,
            "text/plain": handle_markdown,
            "application/pdf": handle_pdf,
        }

    def process_data(self, mime_type: str, file_content: bytes, **kwargs) -> str:
        """
        Process file content based on mime type.

        Args:
            mime_type: The MIME type of the file
            file_content: The raw file content as bytes
            **kwargs: Additional arguments to pass to the handler

        Returns:
            Extracted text content as string

        Raises:
            ValueError: If mime type is not supported
        """
        handler = self.handlers.get(mime_type)
        if not handler:
            raise ValueError(f"No handler found for mime type: {mime_type}")

        return handler(file_content, **kwargs)

    def supports(self, mime_type: str) -> bool:
        """Check if a mime type is supported."""
        return mime_type in self.handlers
