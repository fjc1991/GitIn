from ...logger import get_logger
from .base import BaseMetric
from pydriller import ModificationType
import statistics
import re

logger = get_logger(__name__)

class LinesMetric(BaseMetric):
    def __init__(self):
        super().__init__()
        self.lines_added_by_file = {}
        self.lines_removed_by_file = {}
        self.renamed_files = {}  # Track renamed files
        # Add tracking for no-op operations
        self.noop_added_by_file = {}  # No-ops added (whitespace, blank lines)
        self.noop_removed_by_file = {}  # No-ops removed
    
    def process_commit(self, commit):
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file, commit.author.name, commit.committer_date)
        return self
    
    def process_modified_file(self, modified_file, author_name, commit_date):
        filepath = self.renamed_files.get(modified_file.new_path, modified_file.new_path)
        
        if modified_file.change_type == ModificationType.RENAME:
            self.renamed_files[modified_file.old_path] = filepath
        
        # Initialize counters if this is a new file path
        if filepath not in self.lines_added_by_file:
            self.lines_added_by_file[filepath] = []
        if filepath not in self.lines_removed_by_file:
            self.lines_removed_by_file[filepath] = []
        if filepath not in self.noop_added_by_file:
            self.noop_added_by_file[filepath] = []
        if filepath not in self.noop_removed_by_file:
            self.noop_removed_by_file[filepath] = []
        
        # Calculate regular line changes
        added_lines = modified_file.added_lines
        removed_lines = modified_file.deleted_lines
        
        # Calculate no-op changes
        noop_added = 0
        noop_removed = 0
        
        # Only process diffs if we have a valid modification type that would have a diff
        if modified_file.change_type in [ModificationType.ADD, ModificationType.DELETE, 
                                         ModificationType.MODIFY, ModificationType.RENAME]:
            try:
                # Get diff_parsed which gives us modified lines
                diff = modified_file.diff_parsed
                
                # Check added lines for no-ops (blank or whitespace only)
                for line in diff.get('added', []):
                    content = line[1]
                    if self._is_noop_line(content):
                        noop_added += 1
                
                # Check removed lines for no-ops
                for line in diff.get('deleted', []):
                    content = line[1]
                    if self._is_noop_line(content):
                        noop_removed += 1
                
            except Exception as e:
                logger.debug(f"Could not analyze diff for no-ops in {filepath}: {str(e)}")
        
        # Record the statistics
        self.lines_added_by_file[filepath].append(added_lines)
        self.lines_removed_by_file[filepath].append(removed_lines)
        self.noop_added_by_file[filepath].append(noop_added)
        self.noop_removed_by_file[filepath].append(noop_removed)
        
        return self
    
    # Simple approach to check if a line is a no-op > In reality, depending on the language, this could be more complex.
    def _is_noop_line(self, line_content):
        if not line_content or line_content.strip() == "":
            return True

        if re.match(r'^\s+$', line_content):
            return True
            
        return False
    
    def get_metrics(self):
        added_total = {}
        added_max = {}
        added_avg = {}
        
        removed_total = {}
        removed_max = {}
        removed_avg = {}
        
        noop_added_total = {}
        noop_added_max = {}
        noop_added_avg = {}
        
        noop_removed_total = {}
        noop_removed_max = {}
        noop_removed_avg = {}
        
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
        
        # Process no-op statistics
        for path, noop_list in self.noop_added_by_file.items():
            if noop_list:
                noop_added_total[path] = sum(noop_list)
                noop_added_max[path] = max(noop_list)
                noop_added_avg[path] = round(statistics.mean(noop_list))
        
        for path, noop_list in self.noop_removed_by_file.items():
            if noop_list:
                noop_removed_total[path] = sum(noop_list)
                noop_removed_max[path] = max(noop_list)
                noop_removed_avg[path] = round(statistics.mean(noop_list))
        
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
            },
            "noop_added": {
                "total": noop_added_total,
                "max": noop_added_max,
                "avg": noop_added_avg
            },
            "noop_removed": {
                "total": noop_removed_total,
                "max": noop_removed_max,
                "avg": noop_removed_avg
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
                },
                "noop_added": {
                    "total": {},
                    "max": {},
                    "avg": {}
                },
                "noop_removed": {
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
        
        total_noop_added = {}
        max_noop_added = {}
        avg_noop_added_sum = {}
        avg_noop_added_count = {}
        
        total_noop_removed = {}
        max_noop_removed = {}
        avg_noop_removed_sum = {}
        avg_noop_removed_count = {}
        
        # Calculate merged values
        for metrics in metrics_list:
            added_metrics = metrics.get("added", {})
            removed_metrics = metrics.get("removed", {})
            noop_added_metrics = metrics.get("noop_added", {})
            noop_removed_metrics = metrics.get("noop_removed", {})
            
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
            
            # No-op added lines
            for path, total in noop_added_metrics.get("total", {}).items():
                if path not in total_noop_added:
                    total_noop_added[path] = 0
                total_noop_added[path] += total
            
            for path, max_val in noop_added_metrics.get("max", {}).items():
                if path not in max_noop_added:
                    max_noop_added[path] = 0
                max_noop_added[path] = max(max_noop_added[path], max_val)
            
            for path, avg in noop_added_metrics.get("avg", {}).items():
                if path not in avg_noop_added_sum:
                    avg_noop_added_sum[path] = 0
                    avg_noop_added_count[path] = 0
                avg_noop_added_sum[path] += avg * noop_added_metrics.get("_count", {}).get(path, 1)
                avg_noop_added_count[path] += noop_added_metrics.get("_count", {}).get(path, 1)
            
            # No-op removed lines
            for path, total in noop_removed_metrics.get("total", {}).items():
                if path not in total_noop_removed:
                    total_noop_removed[path] = 0
                total_noop_removed[path] += total
            
            for path, max_val in noop_removed_metrics.get("max", {}).items():
                if path not in max_noop_removed:
                    max_noop_removed[path] = 0
                max_noop_removed[path] = max(max_noop_removed[path], max_val)
            
            for path, avg in noop_removed_metrics.get("avg", {}).items():
                if path not in avg_noop_removed_sum:
                    avg_noop_removed_sum[path] = 0
                    avg_noop_removed_count[path] = 0
                avg_noop_removed_sum[path] += avg * noop_removed_metrics.get("_count", {}).get(path, 1)
                avg_noop_removed_count[path] += noop_removed_metrics.get("_count", {}).get(path, 1)
        
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
            },
            "noop_added": {
                "total": total_noop_added,
                "max": max_noop_added,
                "avg": {path: avg_noop_added_sum[path] / avg_noop_added_count[path] if avg_noop_added_count[path] > 0 else 0 for path in avg_noop_added_sum}
            },
            "noop_removed": {
                "total": total_noop_removed,
                "max": max_noop_removed,
                "avg": {path: avg_noop_removed_sum[path] / avg_noop_removed_count[path] if avg_noop_removed_count[path] > 0 else 0 for path in avg_noop_removed_sum}
            }
        }
