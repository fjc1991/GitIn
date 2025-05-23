from ...logger import get_logger
from ..base import BaseMetric
from pydriller import ModificationType

logger = get_logger(__name__)

class ContributorsMetric(BaseMetric):
    def __init__(self):
        super().__init__()
        self.contributors_by_file = {}
        self.lines_by_author = {}
        self.renamed_files = {}
    
    def process_commit(self, commit):
        author_email = commit.author.email.strip()
        
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file, author_email, commit.committer_date)
            
        return self
    
    def process_modified_file(self, modified_file, author_email, commit_date):
        """Process a single modified file and update metrics"""
        filepath = self.renamed_files.get(modified_file.new_path, modified_file.new_path)
        
        if modified_file.change_type == ModificationType.RENAME:
            self.renamed_files[modified_file.old_path] = filepath

        lines_authored = modified_file.added_lines + modified_file.deleted_lines

        if filepath not in self.contributors_by_file:
            self.contributors_by_file[filepath] = set()
        self.contributors_by_file[filepath].add(author_email)
        
        # Fix the KeyError by properly initializing the nested dictionary
        if filepath not in self.lines_by_author:
            self.lines_by_author[filepath] = {}
        if author_email not in self.lines_by_author[filepath]:
            self.lines_by_author[filepath][author_email] = 0
        self.lines_by_author[filepath][author_email] += lines_authored
        
        return self
    
    def get_metrics(self):
        contributors_count = {}
        minor_contributors = {}

        for filepath, contributions in list(self.lines_by_author.items()):
            total = sum(contributions.values())
            if total == 0:
                continue
            
            contributors_count[filepath] = len(contributions)
            minor_contributors[filepath] = sum(1 for v in contributions.values() if v/total < 0.05)
        
        return {
            "total": contributors_count,
            "minor": minor_contributors
        }
    
    def get_experience_metrics(self):
        return {filename: len(authors) for filename, authors in self.contributors_by_file.items()}
    
    @staticmethod
    def merge_metrics(metrics_list):
        if not metrics_list:
            return {
                "total": {},
                "minor": {}
            }
        
        merged_total = {}
        merged_minor = {}
        
        # Merge total contributors
        for metrics in metrics_list:
            total_metrics = metrics.get("total", {})
            for filename, count in total_metrics.items():
                if filename not in merged_total:
                    merged_total[filename] = 0
                merged_total[filename] = max(merged_total[filename], count)
        
        # Merge minor contributors
        for metrics in metrics_list:
            minor_metrics = metrics.get("minor", {})
            for filename, count in minor_metrics.items():
                if filename not in merged_minor:
                    merged_minor[filename] = 0
                merged_minor[filename] += count
        
        return {
            "total": merged_total,
            "minor": merged_minor
        }
    
    @staticmethod
    def merge_experience_metrics(metrics_list):
        if not metrics_list:
            return {}
        
        merged_metrics = {}
        
        # Take the maximum value for each file when merging
        for metrics in metrics_list:
            for filename, value in metrics.items():
                if filename not in merged_metrics:
                    merged_metrics[filename] = 0
                merged_metrics[filename] = max(merged_metrics[filename], value)
        
        return merged_metrics
