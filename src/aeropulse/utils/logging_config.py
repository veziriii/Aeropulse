import logging
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()


def setup_logger(log_file: str):
    """
    Configure logging for the application.

    - Logs all levels (DEBUG and above) to a file.
    - Logs WARNING and above to the terminal.

    Args:
        log_file (str): Name of the log file.
            A `.log` extension is recommended.

    Returns:
        logging.Logger: Configured root logger instance.
    """

    log_dir = os.getenv("LOG_DIR")
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = Path(log_dir) / log_file
    fmt = "%(asctime)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M")

    # file_handler configuration
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(10)

    # stream_handler configuration
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(20)

    logging.basicConfig(
        level=logging.DEBUG, handlers=[file_handler, stream_handler], force=True
    )

    return logging.getLogger()
