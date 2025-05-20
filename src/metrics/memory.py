import gc
import psutil
import time
from logger import get_logger

logger = get_logger(__name__)

def get_memory_usage():
    """Get current memory usage as percentage."""
    return psutil.virtual_memory().percent

def check_memory_pressure(threshold=85):
    memory_percent = get_memory_usage()
    if memory_percent > threshold:
        logger.warning(f"Memory pressure detected during metrics calculation: {memory_percent}% used")
        gc.collect()
        return True
    return False

def wait_for_memory_availability(memory_limit=85):
    if get_memory_usage() > memory_limit:
        logger.warning(f"Waiting for memory to free up. Current usage: {get_memory_usage()}%")
        
        while get_memory_usage() > memory_limit - 5:
            time.sleep(2)
            gc.collect()
        
        logger.debug(f"Memory available again. Current usage: {get_memory_usage()}%")