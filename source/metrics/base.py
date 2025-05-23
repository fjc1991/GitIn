from ..logger import get_logger
from abc import ABC, abstractmethod

logger = get_logger(__name__)

class BaseMetric(ABC):
    """
    Base class for all metrics.
    Defines the common interface and functionality for all metric types.
    """
    
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
    
    @abstractmethod
    def process_commit(self, commit):
        """Process a single commit and update metrics"""
        pass
    
    @abstractmethod
    def process_modified_file(self, filename, modified_file, author_name, commit_date):
        """Process a single modified file and update metrics"""
        pass
    
    @abstractmethod
    def get_metrics(self):
        """Get the calculated metrics"""
        pass
        
    @staticmethod
    @abstractmethod
    def merge_metrics(metrics_list):
        """Merge multiple metrics results into one"""
        pass
