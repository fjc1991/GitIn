from ...logger import get_logger
from ..base import BaseMetric
from pydriller import ModificationType
import re

logger = get_logger(__name__)

class BugsMetric(BaseMetric):
    def __init__(self):
        super().__init__()
        self.bug_fixing_changed_lines = {}
        self.total_changed_lines = {}
        self.renamed_files = {}
        
        # Default bug commit message patterns
        self.bug_patterns = [
            r'fix(?:e[ds])?(?:\s+for)?\s+(?:bug|issue|problem)',
            r'bug\s+fix(?:e[ds])?',
            r'resolv(?:e[ds]?|ing)\s+(?:bug|issue|problem)',
            r'\#\d+',
            r'bug\s+\#?\d+',
            r'fix(?:e[ds])?\s+\#\d+',
            r'patch(?:e[ds])?',
            r'defect',
            r'debug'
        ]
    
    def set_bug_patterns(self, patterns):
        self.bug_patterns = patterns
        return self
    
    def is_bug_fix(self, commit):
        message = commit.msg.lower()
        
        # Check against words previously defined in bug patterns
        for pattern in self.bug_patterns:
            if re.search(pattern, message, re.IGNORECASE):
                return True
                
        if hasattr(commit, 'issue_tracker_ticket') and commit.issue_tracker_ticket:
            if 'bug' in commit.issue_tracker_ticket.labels.lower():
                return True
                
        return False
    
    def process_commit(self, commit):
        is_bug_fix = self.is_bug_fix(commit)
        
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file, is_bug_fix)
            
        return self
    
    def process_modified_file(self, modified_file, is_bug_fix):
        filepath = self.renamed_files.get(modified_file.new_path, modified_file.new_path)
        
        if modified_file.change_type == ModificationType.RENAME:
            self.renamed_files[modified_file.old_path] = filepath
        
        changed_lines = modified_file.added_lines + modified_file.deleted_lines
        
        if filepath not in self.total_changed_lines:
            self.total_changed_lines[filepath] = 0
        self.total_changed_lines[filepath] += changed_lines
        
        if is_bug_fix:
            if filepath not in self.bug_fixing_changed_lines:
                self.bug_fixing_changed_lines[filepath] = 0
            self.bug_fixing_changed_lines[filepath] += changed_lines
        
        return self
    
    def get_metrics(self):
        """Calculate the metrics according to GitClear"""
        bug_work_percent = {}
        
        # Calculate bug work percent for each file
        for filepath, total_lines in self.total_changed_lines.items():
            if total_lines > 0:
                bug_lines = self.bug_fixing_changed_lines.get(filepath, 0)
                bug_work_percent[filepath] = (bug_lines / total_lines) * 100
            else:
                bug_work_percent[filepath] = 0
        
        # Calculate overall bug work percent
        total_bug_lines = sum(self.bug_fixing_changed_lines.values())
        total_lines = sum(self.total_changed_lines.values())
        overall_percent = (total_bug_lines / total_lines) * 100 if total_lines > 0 else 0
        
        return {
            "bug_work_percent_by_file": bug_work_percent,
            "overall_bug_work_percent": overall_percent,
            "total_bug_lines": total_bug_lines,
            "total_lines": total_lines
        }
    
    @staticmethod
    def merge_metrics(metrics_list):
        if not metrics_list:
            return {
                "bug_work_percent_by_file": {},
                "overall_bug_work_percent": 0,
                "total_bug_lines": 0,
                "total_lines": 0
            }
        
        merged_bug_percent = {}
        total_bug_lines = 0
        total_lines = 0
        
        # File metrics
        for metrics in metrics_list:
            total_bug_lines += metrics.get("total_bug_lines", 0)
            total_lines += metrics.get("total_lines", 0)
            
            # Percentages
            file_percentages = metrics.get("bug_work_percent_by_file", {})
            for filepath, percent in file_percentages.items():
                if filepath not in merged_bug_percent:
                    merged_bug_percent[filepath] = []
                merged_bug_percent[filepath].append(percent)
        
        # Average bug percent for each file
        for filepath, percentages in merged_bug_percent.items():
            merged_bug_percent[filepath] = sum(percentages) / len(percentages)
        overall_percent = (total_bug_lines / total_lines) * 100 if total_lines > 0 else 0
        
        return {
            "bug_work_percent_by_file": merged_bug_percent,
            "overall_bug_work_percent": overall_percent,
            "total_bug_lines": total_bug_lines,
            "total_lines": total_lines
        }
