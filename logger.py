import os
import logging
import sys
from datetime import datetime
import io

LOGS_DIR = 'logs'
os.makedirs(LOGS_DIR, exist_ok=True)

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = os.path.join(LOGS_DIR, f'crypto_analyzer_{timestamp}.log')
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_format)
root_logger.addHandler(file_handler)

# UTF-8 encoding for Windows
class EncodingStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None, encoding='utf-8'):
        self.encoding = encoding
        super().__init__(stream)
        
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            
            try:
                if self.encoding:
                    msg = msg.encode(self.encoding, errors='backslashreplace').decode(self.encoding)
            except (UnicodeError, AttributeError):
                pass
                
            stream.write(msg)
            stream.write(self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

# Custom handler that manages encoding
console_handler = EncodingStreamHandler(sys.stderr)
console_handler.setLevel(logging.ERROR)
console_format = logging.Formatter('%(levelname)s: %(message)s')
console_handler.setFormatter(console_format)
root_logger.addHandler(console_handler)

def get_logger(name):
    logger = logging.getLogger(name)
    return logger

root_logger.info(f"Starting analyzer logging session at {timestamp}")
root_logger.info(f"Log file: {log_file}")