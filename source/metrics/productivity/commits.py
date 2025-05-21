from ...logger import get_logger
from .base import BaseMetric
from pydriller import ModificationType

logger = get_logger(__name__)

class CommitsMetric(BaseMetric):
    def __init__(self):
        super().__init__()
        self.commits_by_file = {}
        self.renamed_files = {}  # Track renamed files
    
    def process_commit(self, commit):
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file, commit.author.name, commit.committer_date)
        return self
    
    def process_modified_file(self, modified_file, author_name, commit_date):
        filepath = self.renamed_files.get(modified_file.new_path, modified_file.new_path)
        
        if modified_file.change_type == ModificationType.RENAME:
            self.renamed_files[modified_file.old_path] = filepath
        
        if filepath not in self.commits_by_file:
            self.commits_by_file[filepath] = 0
        self.commits_by_file[filepath] += 1
        return self
    
    def get_metrics(self):
        """Get the calculated metrics"""
        return self.commits_by_file
    
    @staticmethod
    def merge_metrics(metrics_list):
        """Merge multiple metrics results into one"""
        if not metrics_list:
            return {}
        
        merged_metrics = {}
        
        # Combine file commit counts from all metrics
        for metrics in metrics_list:
            for filename, count in metrics.items():
                if filename not in merged_metrics:
                    merged_metrics[filename] = 0
                merged_metrics[filename] += count
        
        return merged_metrics
