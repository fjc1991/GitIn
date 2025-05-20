from ...logger import get_logger
from .base import BaseMetric

logger = get_logger(__name__)

class ContributorsMetric(BaseMetric):
    def __init__(self):
        super().__init__()
        self.contributors_by_file = {}
        self.contributors_commit_count = {}
        self.commits_by_file = {}
    
    def process_commit(self, commit):
        """Process a single commit and update metrics"""
        author_name = commit.author.name
        
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file.filename, modified_file, author_name, commit.committer_date)
            
        return self
    
    def process_modified_file(self, filename, modified_file, author_name, commit_date):
        """Process a single modified file and update metrics"""
        # Update contributors by file
        if filename not in self.contributors_by_file:
            self.contributors_by_file[filename] = set()
        self.contributors_by_file[filename].add(author_name)
        
        # Update contributor commit counts
        if filename not in self.contributors_commit_count:
            self.contributors_commit_count[filename] = {}
        if author_name not in self.contributors_commit_count[filename]:
            self.contributors_commit_count[filename][author_name] = 0
        self.contributors_commit_count[filename][author_name] += 1
        
        # Update commits by file for threshold calculations
        if filename not in self.commits_by_file:
            self.commits_by_file[filename] = 0
        self.commits_by_file[filename] += 1
        
        return self
    
    def get_metrics(self):
        """Get the calculated metrics"""
        # Calculate total contributor count per file
        contributors_count = {filename: len(authors) for filename, authors in self.contributors_by_file.items()}
        
        # Calculate minor contributors (using 20% threshold)
        minor_contributors = {}
        for filename, author_commits in self.contributors_commit_count.items():
            if filename in self.commits_by_file and self.commits_by_file[filename] > 0:
                total_commits = self.commits_by_file[filename]
                threshold = max(1, total_commits * 0.2)
                minor_count = sum(1 for commits in author_commits.values() if commits < threshold)
                minor_contributors[filename] = minor_count
        
        return {
            "total": contributors_count,
            "minor": minor_contributors
        }
    
    def get_experience_metrics(self):
        """Get contributors experience metrics"""
        return {filename: len(authors) for filename, authors in self.contributors_by_file.items()}
    
    @staticmethod
    def merge_metrics(metrics_list):
        """Merge multiple metrics results into one"""
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
                # Take the maximum count as approximation when merging
                merged_total[filename] = max(merged_total[filename], count)
        
        # Merge minor contributors
        for metrics in metrics_list:
            minor_metrics = metrics.get("minor", {})
            for filename, count in minor_metrics.items():
                if filename not in merged_minor:
                    merged_minor[filename] = 0
                # Sum minor contributors to approximate when merging
                merged_minor[filename] += count
        
        return {
            "total": merged_total,
            "minor": merged_minor
        }
    
    @staticmethod
    def merge_experience_metrics(metrics_list):
        """Merge contributors experience metrics"""
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
