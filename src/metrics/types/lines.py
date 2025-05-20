from ...logger import get_logger
from .base import BaseMetric

logger = get_logger(__name__)

class LinesMetric(BaseMetric):
    def __init__(self):
        super().__init__()
        self.lines_added_by_file = {}
        self.lines_removed_by_file = {}
    
    def process_commit(self, commit):
        """Process a single commit and update metrics"""
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file.filename, modified_file, commit.author.name, commit.committer_date)
        return self
    
    def process_modified_file(self, filename, modified_file, author_name, commit_date):
        """Process a single modified file and update metrics"""
        # Update lines added
        if filename not in self.lines_added_by_file:
            self.lines_added_by_file[filename] = 0
        self.lines_added_by_file[filename] += modified_file.added_lines
        
        # Update lines removed
        if filename not in self.lines_removed_by_file:
            self.lines_removed_by_file[filename] = 0
        self.lines_removed_by_file[filename] += modified_file.deleted_lines
        
        return self
    
    def get_metrics(self):
        """Get the calculated metrics"""
        return {
            "added": {
                "total": sum(self.lines_added_by_file.values()),
                "max": max(self.lines_added_by_file.values()) if self.lines_added_by_file else 0,
                "avg": sum(self.lines_added_by_file.values()) / len(self.lines_added_by_file) if self.lines_added_by_file else 0
            },
            "removed": {
                "total": sum(self.lines_removed_by_file.values()),
                "max": max(self.lines_removed_by_file.values()) if self.lines_removed_by_file else 0,
                "avg": sum(self.lines_removed_by_file.values()) / len(self.lines_removed_by_file) if self.lines_removed_by_file else 0
            }
        }
    
    @staticmethod
    def merge_metrics(metrics_list):
        """Merge multiple metrics results into one"""
        if not metrics_list:
            return {
                "added": {
                    "total": 0,
                    "max": 0,
                    "avg": 0
                },
                "removed": {
                    "total": 0,
                    "max": 0,
                    "avg": 0
                }
            }
        
        # Initialize counters
        total_added = 0
        max_added = 0
        avg_added_sum = 0
        avg_added_count = 0
        
        total_removed = 0
        max_removed = 0
        avg_removed_sum = 0
        avg_removed_count = 0
        
        # Calculate merged values
        for metrics in metrics_list:
            added_metrics = metrics.get("added", {})
            removed_metrics = metrics.get("removed", {})
            
            # Added lines
            total_added += added_metrics.get("total", 0)
            max_added = max(max_added, added_metrics.get("max", 0))
            
            # For averages, use weighted approach
            count = added_metrics.get("_count", 1)
            avg_added_sum += added_metrics.get("avg", 0) * count
            avg_added_count += count
            
            # Removed lines
            total_removed += removed_metrics.get("total", 0)
            max_removed = max(max_removed, removed_metrics.get("max", 0))
            
            # For averages, use weighted approach
            count = removed_metrics.get("_count", 1)
            avg_removed_sum += removed_metrics.get("avg", 0) * count
            avg_removed_count += count
        
        return {
            "added": {
                "total": total_added,
                "max": max_added,
                "avg": avg_added_sum / avg_added_count if avg_added_count > 0 else 0
            },
            "removed": {
                "total": total_removed,
                "max": max_removed,
                "avg": avg_removed_sum / avg_removed_count if avg_removed_count > 0 else 0
            }
        }
