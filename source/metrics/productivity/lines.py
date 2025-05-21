from ...logger import get_logger
from .base import BaseMetric
from pydriller import ModificationType
import statistics

logger = get_logger(__name__)

class LinesMetric(BaseMetric):
    def __init__(self):
        super().__init__()
        self.lines_added_by_file = {}
        self.lines_removed_by_file = {}
        self.renamed_files = {}  # Track renamed files
    
    def process_commit(self, commit):
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file, commit.author.name, commit.committer_date)
        return self
    
    def process_modified_file(self, modified_file, author_name, commit_date):

        filepath = self.renamed_files.get(modified_file.new_path, modified_file.new_path)
        
        if modified_file.change_type == ModificationType.RENAME:
            self.renamed_files[modified_file.old_path] = filepath
        
        if filepath not in self.lines_added_by_file:
            self.lines_added_by_file[filepath] = []
        self.lines_added_by_file[filepath].append(modified_file.added_lines)
        
        if filepath not in self.lines_removed_by_file:
            self.lines_removed_by_file[filepath] = []
        self.lines_removed_by_file[filepath].append(modified_file.deleted_lines)
        
        return self
    
    def get_metrics(self):
        added_total = {}
        added_max = {}
        added_avg = {}
        
        removed_total = {}
        removed_max = {}
        removed_avg = {}
        
        for path, lines_list in self.lines_added_by_file.items():
            if lines_list:
                added_total[path] = sum(lines_list)
                added_max[path] = max(lines_list)
                added_avg[path] = round(statistics.mean(lines_list))
        
        for path, lines_list in self.lines_removed_by_file.items():
            if lines_list:
                removed_total[path] = sum(lines_list)
                removed_max[path] = max(lines_list)
                removed_avg[path] = round(statistics.mean(lines_list))
        
        return {
            "added": {
                "total": added_total,
                "max": added_max,
                "avg": added_avg
            },
            "removed": {
                "total": removed_total,
                "max": removed_max,
                "avg": removed_avg
            }
        }
    
    @staticmethod
    def merge_metrics(metrics_list):
        if not metrics_list:
            return {
                "added": {
                    "total": {},
                    "max": {},
                    "avg": {}
                },
                "removed": {
                    "total": {},
                    "max": {},
                    "avg": {}
                }
            }
        
        # Initialize counters
        total_added = {}
        max_added = {}
        avg_added_sum = {}
        avg_added_count = {}
        
        total_removed = {}
        max_removed = {}
        avg_removed_sum = {}
        avg_removed_count = {}
        
        # Calculate merged values
        for metrics in metrics_list:
            added_metrics = metrics.get("added", {})
            removed_metrics = metrics.get("removed", {})
            
            # Added lines
            for path, total in added_metrics.get("total", {}).items():
                if path not in total_added:
                    total_added[path] = 0
                total_added[path] += total
            
            for path, max_val in added_metrics.get("max", {}).items():
                if path not in max_added:
                    max_added[path] = 0
                max_added[path] = max(max_added[path], max_val)
            
            for path, avg in added_metrics.get("avg", {}).items():
                if path not in avg_added_sum:
                    avg_added_sum[path] = 0
                    avg_added_count[path] = 0
                avg_added_sum[path] += avg * added_metrics.get("_count", {}).get(path, 1)
                avg_added_count[path] += added_metrics.get("_count", {}).get(path, 1)
            
            # Removed lines
            for path, total in removed_metrics.get("total", {}).items():
                if path not in total_removed:
                    total_removed[path] = 0
                total_removed[path] += total
            
            for path, max_val in removed_metrics.get("max", {}).items():
                if path not in max_removed:
                    max_removed[path] = 0
                max_removed[path] = max(max_removed[path], max_val)
            
            for path, avg in removed_metrics.get("avg", {}).items():
                if path not in avg_removed_sum:
                    avg_removed_sum[path] = 0
                    avg_removed_count[path] = 0
                avg_removed_sum[path] += avg * removed_metrics.get("_count", {}).get(path, 1)
                avg_removed_count[path] += removed_metrics.get("_count", {}).get(path, 1)
        
        return {
            "added": {
                "total": total_added,
                "max": max_added,
                "avg": {path: avg_added_sum[path] / avg_added_count[path] if avg_added_count[path] > 0 else 0 for path in avg_added_sum}
            },
            "removed": {
                "total": total_removed,
                "max": max_removed,
                "avg": {path: avg_removed_sum[path] / avg_removed_count[path] if avg_removed_count[path] > 0 else 0 for path in avg_removed_sum}
            }
        }
