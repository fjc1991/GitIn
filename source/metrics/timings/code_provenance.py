# source/metrics/velocity/code_provenance.py
from ...logger import get_logger
from ..base import BaseMetric
from collections import defaultdict
from datetime import datetime, timedelta
import re

logger = get_logger(__name__)

class CodeProvenanceMetric(BaseMetric):
    """
    Tracks code provenance - whether developers are working on new code,
    recently modified code, or old/legacy code.
    
    Based on GitClear's approach:
    - New code: Never existed before
    - Recent code: Modified in the last 30 days
    - Old code: Modified 30-365 days ago
    - Legacy code: Not modified in over a year
    """
    
    def __init__(self):
        super().__init__()
        self.developer_stats = defaultdict(lambda: {
            'weekly_provenance': defaultdict(lambda: {
                'new_code_lines': 0,
                'recent_code_lines': 0,
                'old_code_lines': 0,
                'legacy_code_lines': 0,
                'total_lines': 0
            })
        })
        
        # Track line-level history for provenance
        self.line_history = defaultdict(lambda: {})  # file -> {line_num: {last_modified, author}}
        
        # Time thresholds for code age categories
        self.thresholds = {
            'recent': timedelta(days=30),
            'old': timedelta(days=365)
        }
    
    def process_commit(self, commit):
        """Process commit for code provenance tracking."""
        developer_email = commit.author.email.strip().lower()
        commit_date = commit.committer_date
        week_key = self._get_week_key(commit_date)
        
        for modified_file in commit.modified_files:
            self.process_modified_file(
                modified_file.filename,
                modified_file,
                developer_email,
                commit_date,
                commit.hash
            )
        
        return self
    
    def process_modified_file(self, filename, modified_file, developer_email, commit_date, commit_hash):
        """Analyze code provenance for modified file."""
        week_key = self._get_week_key(commit_date)
        
        # Skip non-code files
        if not self._is_code_file(filename):
            return self
        
        # Initialize file history if needed
        if filename not in self.line_history:
            self._initialize_file_history(filename, modified_file, commit_date)
        
        # Process diff to categorize line changes by age
        if hasattr(modified_file, 'diff_parsed') and modified_file.diff_parsed:
            added_lines = modified_file.diff_parsed.get('added', [])
            deleted_lines = modified_file.diff_parsed.get('deleted', [])
            
            # Process deletions first to update line history
            for line_num, line_content in deleted_lines:
                if line_num in self.line_history[filename]:
                    del self.line_history[filename][line_num]
            
            # Process additions and categorize by provenance
            for line_num, line_content in added_lines:
                if not self._is_meaningful_line(line_content):
                    continue
                
                # Determine code age category
                category = self._categorize_line_age(filename, line_num, commit_date)
                
                # Update stats
                self.developer_stats[developer_email]['weekly_provenance'][week_key][f'{category}_lines'] += 1
                self.developer_stats[developer_email]['weekly_provenance'][week_key]['total_lines'] += 1
                
                # Update line history
                self.line_history[filename][line_num] = {
                    'last_modified': commit_date,
                    'author': developer_email,
                    'commit': commit_hash
                }
        
        return self
    
    def _categorize_line_age(self, filename, line_num, current_date):
        """Categorize a line based on when it was last modified."""
        if line_num not in self.line_history[filename]:
            return 'new_code'
        
        line_info = self.line_history[filename][line_num]
        last_modified = line_info['last_modified']
        age = current_date - last_modified
        
        if age <= self.thresholds['recent']:
            return 'recent_code'
        elif age <= self.thresholds['old']:
            return 'old_code'
        else:
            return 'legacy_code'
    
    def _initialize_file_history(self, filename, modified_file, current_date):
        """Initialize line history for a file using git blame equivalent."""
        # For new files or files without history, all lines are "new"
        # In a real implementation, you might want to use git blame here
        # For now, we'll treat all existing lines as "old" code
        try:
            if hasattr(modified_file, 'source_code_before') and modified_file.source_code_before:
                lines = modified_file.source_code_before.split('\n')
                for i, line in enumerate(lines, 1):
                    if self._is_meaningful_line(line):
                        # Assume old code (>30 days) for existing lines
                        self.line_history[filename][i] = {
                            'last_modified': current_date - timedelta(days=60),
                            'author': 'unknown',
                            'commit': 'initial'
                        }
        except Exception as e:
            logger.debug(f"Could not initialize file history for {filename}: {str(e)}")
    
    def _is_code_file(self, filename):
        """Check if file is a code file that should be analyzed."""
        code_extensions = {
            '.py', '.js', '.java', '.cpp', '.c', '.cs', '.rb', '.go',
            '.rs', '.kt', '.swift', '.m', '.scala', '.php', '.ts',
            '.tsx', '.jsx', '.vue', '.dart', '.r', '.jl', '.ex', '.exs'
        }
        
        return any(filename.endswith(ext) for ext in code_extensions)
    
    def _is_meaningful_line(self, line):
        """Check if line is meaningful code."""
        stripped = line.strip()
        
        # Skip empty lines
        if not stripped:
            return False
        
        # Skip single character lines
        if len(stripped) <= 1:
            return False
        
        # Skip obvious comments (basic check)
        if stripped.startswith(('#', '//', '/*', '*', '<!--')):
            return False
        
        return True
    
    def _get_week_key(self, date):
        """Get week key for date."""
        start_of_week = date - timedelta(days=date.weekday())
        return start_of_week.strftime('%Y-%m-%d')
    
    def get_metrics(self):
        """Get code provenance metrics."""
        metrics = {}
        
        for developer, stats in self.developer_stats.items():
            metrics[developer] = {'weekly_provenance': {}}
            
            for week, week_stats in stats['weekly_provenance'].items():
                total = week_stats['total_lines']
                if total > 0:
                    metrics[developer]['weekly_provenance'][week] = {
                        'new_code_lines': week_stats['new_code_lines'],
                        'recent_code_lines': week_stats['recent_code_lines'],
                        'old_code_lines': week_stats['old_code_lines'],
                        'legacy_code_lines': week_stats['legacy_code_lines'],
                        'total_lines': total,
                        'new_code_percent': (week_stats['new_code_lines'] / total) * 100,
                        'recent_code_percent': (week_stats['recent_code_lines'] / total) * 100,
                        'old_code_percent': (week_stats['old_code_lines'] / total) * 100,
                        'legacy_code_percent': (week_stats['legacy_code_lines'] / total) * 100
                    }
        
        return metrics
    
    @staticmethod
    def merge_metrics(metrics_list):
        """Merge multiple code provenance metrics."""
        if not metrics_list:
            return {}
        
        merged = defaultdict(lambda: {'weekly_provenance': defaultdict(lambda: {
            'new_code_lines': 0,
            'recent_code_lines': 0,
            'old_code_lines': 0,
            'legacy_code_lines': 0,
            'total_lines': 0
        })})
        
        for metrics in metrics_list:
            for developer, dev_stats in metrics.items():
                for week, week_stats in dev_stats.get('weekly_provenance', {}).items():
                    for key in ['new_code_lines', 'recent_code_lines', 'old_code_lines', 
                               'legacy_code_lines', 'total_lines']:
                        merged[developer]['weekly_provenance'][week][key] += week_stats.get(key, 0)
        
        # Calculate percentages after merging
        result = {}
        for developer, dev_stats in merged.items():
            result[developer] = {'weekly_provenance': {}}
            for week, week_stats in dev_stats['weekly_provenance'].items():
                total = week_stats['total_lines']
                if total > 0:
                    result[developer]['weekly_provenance'][week] = {
                        **week_stats,
                        'new_code_percent': (week_stats['new_code_lines'] / total) * 100,
                        'recent_code_percent': (week_stats['recent_code_lines'] / total) * 100,
                        'old_code_percent': (week_stats['old_code_lines'] / total) * 100,
                        'legacy_code_percent': (week_stats['legacy_code_lines'] / total) * 100
                    }
        
        return result