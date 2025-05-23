from ...logger import get_logger
from ..base import BaseMetric
from collections import defaultdict

logger = get_logger(__name__)

class ChangeSetMetric(BaseMetric):
    def __init__(self):
        super().__init__()
        self.file_changes = {}
        self.commits_file_count = []
    
    def process_commit(self, commit):
        """Process a single commit and update metrics"""
        modified_files_count = len(commit.modified_files)
        self.commits_file_count.append(modified_files_count)
        
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file.filename, modified_file, commit.author.name, commit.committer_date)
            
        return self
    
    def process_modified_file(self, filename, modified_file, author_name, commit_date):
        """Process a single modified file and update metrics"""
        if filename not in self.file_changes:
            self.file_changes[filename] = 0
        self.file_changes[filename] += 1
        
        return self
    
    def get_metrics(self):
        """Get the calculated metrics"""
        if not self.commits_file_count:
            return {"max": 0, "avg": 0}
        
        return {
            "max": max(self.commits_file_count) if self.commits_file_count else 0,
            "avg": sum(self.commits_file_count) / len(self.commits_file_count) if self.commits_file_count else 0
        }
    
    @staticmethod
    def merge_metrics(metrics_list):
        """Merge multiple metrics results into one"""
        if not metrics_list:
            return {"max": 0, "avg": 0}
        
        max_val = max(m.get("max", 0) for m in metrics_list)
        # Calculating weighted average based on original data size
        total_sum = sum(m.get("avg", 0) * m.get("_count", 1) for m in metrics_list)
        total_count = sum(m.get("_count", 1) for m in metrics_list)
        
        return {
            "max": max_val,
            "avg": total_sum / total_count if total_count > 0 else 0
        }
    
    def extract_file_changes(self):
        """Get the file changes data"""
        return self.file_changes