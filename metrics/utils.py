from datetime import datetime, timedelta
from logger import get_logger
logger = get_logger(__name__)

def generate_weekly_ranges(start_date, end_date):
    if not start_date or not end_date:
        return []
    
    # Convert timezone-aware dates to naive dates if needed
    if start_date.tzinfo:
        start_date = start_date.replace(tzinfo=None)
    if end_date.tzinfo:
        end_date = end_date.replace(tzinfo=None)
    
    weekly_ranges = []
    current_date = start_date
    week_number = 1
    
    while current_date < end_date:
        week_end = current_date + timedelta(days=6)
        if week_end > end_date:
            week_end = end_date
        
        week_label = f"Week_{week_number}_{current_date.strftime('%Y-%m-%d')}"
        weekly_ranges.append((current_date, week_end, week_label))
        
        current_date = week_end + timedelta(days=1)
        week_number += 1
        
    return weekly_ranges

class MetricsAccumulator:
    def __init__(self):
        """Initialize the accumulator with empty data structures."""
        self.file_changes = {}
        self.code_churn = {}
        self.commits_by_file = {}
        self.contributors_by_file = {}
        self.contributors_commit_count = {}
        self.hunks_by_file = {}
        self.lines_added_by_file = {}
        self.lines_removed_by_file = {}
    
    def update_from_modified_file(self, filename, modified_file, author_name):
        # Update file changes
        if filename not in self.file_changes:
            self.file_changes[filename] = 0
        self.file_changes[filename] += 1
        
        # Update code churn
        if filename not in self.code_churn:
            self.code_churn[filename] = 0
        file_churn = modified_file.added_lines + modified_file.deleted_lines
        self.code_churn[filename] += file_churn
        
        # Update commits count
        if filename not in self.commits_by_file:
            self.commits_by_file[filename] = 0
        self.commits_by_file[filename] += 1
        
        # Update contributors
        if filename not in self.contributors_by_file:
            self.contributors_by_file[filename] = set()
        self.contributors_by_file[filename].add(author_name)
        
        # Update contributor commit counts
        if filename not in self.contributors_commit_count:
            self.contributors_commit_count[filename] = {}
        if author_name not in self.contributors_commit_count[filename]:
            self.contributors_commit_count[filename][author_name] = 0
        self.contributors_commit_count[filename][author_name] += 1
        
        # Update hunks count
        if filename not in self.hunks_by_file:
            self.hunks_by_file[filename] = 0
        if hasattr(modified_file, 'diff_parsed') and modified_file.diff_parsed:
            self.hunks_by_file[filename] += len(modified_file.diff_parsed.get('added', [])) + \
                                           len(modified_file.diff_parsed.get('deleted', []))
        
        # Update lines added
        if filename not in self.lines_added_by_file:
            self.lines_added_by_file[filename] = 0
        self.lines_added_by_file[filename] += modified_file.added_lines

        # Update lines removed
        if filename not in self.lines_removed_by_file:
            self.lines_removed_by_file[filename] = 0
        self.lines_removed_by_file[filename] += modified_file.deleted_lines
    
    def get_metrics_data(self):
        return {
            'file_changes': self.file_changes,
            'code_churn': self.code_churn,
            'commits_by_file': self.commits_by_file,
            'contributors_by_file': self.contributors_by_file,
            'contributors_commit_count': self.contributors_commit_count,
            'hunks_by_file': self.hunks_by_file,
            'lines_added_by_file': self.lines_added_by_file,
            'lines_removed_by_file': self.lines_removed_by_file
        }