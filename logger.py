import os
import logging
import sys
from datetime import datetime
from tqdm import tqdm

# Create logs directory if it doesn't exist
LOGS_DIR = 'logs'
os.makedirs(LOGS_DIR, exist_ok=True)

# Configure the main logger
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = os.path.join(LOGS_DIR, f'crypto_analyzer_{timestamp}.log')

# Setup the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Create file handler
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Create console handler with a higher log level (ONLY errors)
console_handler = logging.StreamHandler(sys.stderr)
console_handler.setLevel(logging.ERROR)

# Create formatters and add them to the handlers
file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_format = logging.Formatter('%(levelname)s: %(message)s')
file_handler.setFormatter(file_format)
console_handler.setFormatter(console_format)

# Add the handlers to the logger
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

# Create a special tqdm-compatible logger that won't interfere with progress bars
class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            # Only emit ERROR or higher messages to the console
            if record.levelno >= logging.ERROR:
                msg = self.format(record)
                tqdm.write(msg)
                self.flush()
        except Exception:
            self.handleError(record)

# Configure the tqdm-compatible logger
tqdm_handler = TqdmLoggingHandler()
tqdm_handler.setFormatter(console_format)
tqdm_handler.setLevel(logging.ERROR)  # Only show ERROR and above
root_logger.addHandler(tqdm_handler)

# Helper functions to get module-specific loggers
def get_logger(name):
    return logging.getLogger(name)

# Log the start of a run
root_logger.info(f"Starting crypto analyzer logging session at {timestamp}")
root_logger.info(f"Log file: {log_file}")