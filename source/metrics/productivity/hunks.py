from ...logger import get_logger
from .base import BaseMetric
from pydriller import ModificationType
from statistics import median

logger = get_logger(__name__)

class HunksMetric(BaseMetric):
    def __init__(self):
        super().__init__()
        self.hunks_by_file = {}
        self.renamed_files = {}
    
    def process_commit(self, commit):
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file, commit.author.name, commit.committer_date)
        return self
    
    def process_modified_file(self, modified_file, author_name, commit_date):
        filepath = self.renamed_files.get(modified_file.new_path, modified_file.new_path)
        
        if modified_file.change_type == ModificationType.RENAME:
            self.renamed_files[modified_file.old_path] = filepath
        
        if modified_file.diff:
            is_hunk = False
            hunks = 0
            
            for line in modified_file.diff.splitlines():
                if line.startswith('+') or line.startswith('-'):
                    if not is_hunk:
                        is_hunk = True
                        hunks += 1
                else:
                    is_hunk = False
            
            if filepath not in self.hunks_by_file:
                self.hunks_by_file[filepath] = []
            self.hunks_by_file[filepath].append(hunks)
            
        return self
    
    def get_metrics(self):
        result = {}
        for filepath, hunks_list in self.hunks_by_file.items():
            if hunks_list:  # Ensure list is not empty
                result[filepath] = median(hunks_list)
        return result
    
    @staticmethod
    def merge_metrics(metrics_list):
        if not metrics_list:
            return {}
        
        merged_metrics = {}
        
        # Combine file hunks counts from all metrics
        for metrics in metrics_list:
            for filename, count in metrics.items():
                if filename not in merged_metrics:
                    merged_metrics[filename] = 0
                merged_metrics[filename] += count
        
        return merged_metrics
