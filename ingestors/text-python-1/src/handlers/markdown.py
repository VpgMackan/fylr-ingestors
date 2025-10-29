"""Handler for markdown and plain text files."""


def handle_markdown(file_content: bytes, **kwargs) -> str:
    """
    Extract text from markdown/plain text files.
    
    Args:
        file_content: Raw file content as bytes
        **kwargs: Additional arguments (unused for text files)
        
    Returns:
        Extracted text as string
    """
    try:
        # Try UTF-8 first, fall back to latin-1 if that fails
        try:
            text = file_content.decode('utf-8')
        except UnicodeDecodeError:
            text = file_content.decode('latin-1')
        
        # Remove any null bytes and normalize whitespace
        text = text.replace('\x00', '')
        
        return text.strip()
    except Exception as e:
        raise ValueError(f"Failed to extract text from markdown/text file: {e}")
