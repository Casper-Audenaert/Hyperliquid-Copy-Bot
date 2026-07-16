import sys
from pathlib import Path
from loguru import logger

def setup_logger(log_file: str = "./logs/trading.log", log_level: str = "INFO"):
    """
    Setup loguru logger with file and console output
    """
    # Create logs directory if it doesn't exist
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove default handler
    logger.remove()

    # BUG FIX: Windows' console defaults to the cp1252 codepage, which can't
    # encode characters like "→" — any log line containing one previously
    # crashed loguru's stdout sink (UnicodeEncodeError) instead of just
    # printing it. errors="replace" makes an unencodable character degrade to
    # "?" instead of taking down logging entirely.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")

    # Add console handler with colors
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level=log_level,
        colorize=True
    )
    
    # Add file handler
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
        level=log_level,
        rotation="100 MB",
        retention="30 days",
        compression="zip"
    )
    
    return logger
