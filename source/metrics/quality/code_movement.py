from ...logger import get_logger
from ..productivity.base import BaseMetric
from pydriller import ModificationType
import re
from collections import defaultdict
import difflib

logger = get_logger(__name__)

class CodeMovementMetric(BaseMetric):
    """
    Tracks moved and copy-pasted code percentages similar to GitClear's metrics.
    Moved code: Code that was moved from one file to another without significant changes
    Copy-pasted code: Code that was duplicated from one location to another
    """
    def __init__(self):
        super().__init__()
        self.renamed_files = {}
        self.total_changed_lines = 0
        self.moved_lines = 0
        self.copy_pasted_lines = 0
        self.removed_lines_by_commit = {}
        self.added_lines_by_commit = {}
        
    def process_commit(self, commit):
        commit_id = commit.hash
        self.removed_lines_by_commit[commit_id] = {}
        self.added_lines_by_commit[commit_id] = {}
        
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file.filename, modified_file, commit.author.name, commit.committer_date, commit_id)

        self._detect_moved_and_copy_pasted(commit_id)
        return self
    
    def process_modified_file(self, filename, modified_file, author_name, commit_date, commit_id=None):
        if commit_id is None:
            commit_id = "unknown"
            
        filepath = self.renamed_files.get(modified_file.new_path, modified_file.new_path)
        
        if modified_file.change_type == ModificationType.RENAME:
            self.renamed_files[modified_file.old_path] = filepath
            return self

        try:
            _ = modified_file.source_code
        except ValueError as e:
            logger.warning(f"Could not retrieve source code for {filepath} in commit {commit_id}: {e}")

        if modified_file.diff_parsed:
            removed_lines = [line[1].strip() for line in modified_file.diff_parsed.get('deleted', [])]
            added_lines = [line[1].strip() for line in modified_file.diff_parsed.get('added', [])]
            removed_lines = [line for line in removed_lines if len(line) > 5]
            added_lines = [line for line in added_lines if len(line) > 5]
            
            if commit_id not in self.removed_lines_by_commit:
                self.removed_lines_by_commit[commit_id] = {}
            if commit_id not in self.added_lines_by_commit:
                self.added_lines_by_commit[commit_id] = {}
                
            self.removed_lines_by_commit[commit_id][filepath] = removed_lines
            self.added_lines_by_commit[commit_id][filepath] = added_lines

            self.total_changed_lines += len(removed_lines) + len(added_lines)
            
        return self
    
    def _detect_moved_and_copy_pasted(self, commit_id):
        all_removed = []
        all_added = []
        
        for file_path, lines in self.removed_lines_by_commit.get(commit_id, {}).items():
            all_removed.extend([(file_path, line) for line in lines])
        
        for file_path, lines in self.added_lines_by_commit.get(commit_id, {}).items():
            all_added.extend([(file_path, line) for line in lines])

        matched_removed_indices = set()
        matched_added_indices = set()
        
        for r_idx, (r_file, r_line) in enumerate(all_removed):
            if r_idx in matched_removed_indices:
                continue
                
            for a_idx, (a_file, a_line) in enumerate(all_added):
                if a_idx in matched_added_indices:
                    continue

                if r_line == a_line and r_file != a_file:
                    self.moved_lines += 1
                    matched_removed_indices.add(r_idx)
                    matched_added_indices.add(a_idx)
                    break

        added_line_counts = defaultdict(int)
        for _, line in all_added:
            added_line_counts[line] += 1
            
        for line, count in added_line_counts.items():
            if count > 1 and not any(line == r_line for _, r_line in all_removed):
                self.copy_pasted_lines += (count - 1)

        if commit_id in self.removed_lines_by_commit:
            del self.removed_lines_by_commit[commit_id]
        if commit_id in self.added_lines_by_commit:
            del self.added_lines_by_commit[commit_id]
    
    def get_metrics(self):
        moved_percent = 0
        copy_pasted_percent = 0
        
        if self.total_changed_lines > 0:
            moved_percent = (self.moved_lines / self.total_changed_lines) * 100
            copy_pasted_percent = (self.copy_pasted_lines / self.total_changed_lines) * 100
        
        return {
            "moved_lines_count": self.moved_lines,
            "copy_pasted_lines_count": self.copy_pasted_lines,
            "total_changed_lines": self.total_changed_lines,
            "moved_lines_percent": moved_percent,
            "copy_pasted_lines_percent": copy_pasted_percent
        }
    
    @staticmethod
    def merge_metrics(metrics_list):
        if not metrics_list:
            return {
                "moved_lines_count": 0,
                "copy_pasted_lines_count": 0,
                "total_changed_lines": 0,
                "moved_lines_percent": 0,
                "copy_pasted_lines_percent": 0
            }
        
        total_changed = 0
        total_moved = 0
        total_copy_pasted = 0
        
        for metrics in metrics_list:
            total_changed += metrics.get("total_changed_lines", 0)
            total_moved += metrics.get("moved_lines_count", 0)
            total_copy_pasted += metrics.get("copy_pasted_lines_count", 0)

        moved_percent = (total_moved / total_changed) * 100 if total_changed > 0 else 0
        copy_pasted_percent = (total_copy_pasted / total_changed) * 100 if total_changed > 0 else 0
        
        return {
            "moved_lines_count": total_moved,
            "copy_pasted_lines_count": total_copy_pasted,
            "total_changed_lines": total_changed,
            "moved_lines_percent": moved_percent,
            "copy_pasted_lines_percent": copy_pasted_percent
        }
