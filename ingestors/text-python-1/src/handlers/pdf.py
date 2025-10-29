"""Handler for PDF files."""
import io
from pypdf import PdfReader


def handle_pdf(file_content: bytes, **kwargs) -> str:
    """
    Extract text from PDF files.
    
    Args:
        file_content: Raw PDF file content as bytes
        **kwargs: Additional arguments (unused for PDF files)
        
    Returns:
        Extracted text as string
    """
    try:
        pdf_file = io.BytesIO(file_content)
        reader = PdfReader(pdf_file)
        
        text_parts = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                text_parts.append(text)
        
        full_text = '\n\n'.join(text_parts)
        return full_text.strip()
    except Exception as e:
        raise ValueError(f"Failed to extract text from PDF: {e}")
