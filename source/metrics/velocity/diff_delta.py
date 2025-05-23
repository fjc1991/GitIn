# source/metrics/velocity/diff_delta.py
from ...logger import get_logger
from ..productivity.base import BaseMetric
from collections import defaultdict
from datetime import datetime, timedelta
import re

logger = get_logger(__name__)

class DiffDeltaMetric(BaseMetric):
    """
    Implements GitClear's Diff Delta metric for measuring meaningful code contributions.
    
    Diff Delta attempts to quantify the cognitive load of code changes by:
    - Weighting different types of changes (adds > updates > deletes > moves)
    - Filtering out low-value changes (whitespace, generated code, etc.)
    - Accounting for code complexity and context
    """
    
    def __init__(self):
        super().__init__()
        self.developer_stats = defaultdict(lambda: {
            'weekly_velocity': defaultdict(lambda: {
                'diff_delta': 0,
                'lines_added': 0,
                'lines_updated': 0,
                'lines_deleted': 0,
                'lines_moved': 0,
                'commits': 0,
                'files_changed': set(),
                'active_days': set()
            }),
            'total_diff_delta': 0,
            'total_commits': 0
        })
        
        # Weights for different operations (inspired by GitClear)
        self.weights = {
            'add': 1.0,      # New code has highest cognitive load
            'update': 0.75,  # Modifying existing code
            'delete': 0.25,  # Removing code is easier
            'move': 0.1      # Moving/renaming has lowest load
        }
        
        # Track code changes for provenance analysis
        self.file_history = defaultdict(lambda: defaultdict(dict))
        
    def process_commit(self, commit):
        """Process a commit and calculate Diff Delta for the developer."""
        developer_email = commit.author.email.strip().lower()
        commit_date = commit.committer_date
        week_key = self._get_week_key(commit_date)
        
        # Track active days for velocity consistency
        day_key = commit_date.strftime('%Y-%m-%d')
        self.developer_stats[developer_email]['weekly_velocity'][week_key]['active_days'].add(day_key)
        self.developer_stats[developer_email]['weekly_velocity'][week_key]['commits'] += 1
        self.developer_stats[developer_email]['total_commits'] += 1
        
        commit_diff_delta = 0
        
        for modified_file in commit.modified_files:
            file_diff_delta = self.process_modified_file(
                modified_file.filename, 
                modified_file, 
                developer_email, 
                commit_date,
                commit.hash
            )
            commit_diff_delta += file_diff_delta
            
            # Track files changed
            self.developer_stats[developer_email]['weekly_velocity'][week_key]['files_changed'].add(
                modified_file.filename
            )
        
        # Add commit-level diff delta
        self.developer_stats[developer_email]['weekly_velocity'][week_key]['diff_delta'] += commit_diff_delta
        self.developer_stats[developer_email]['total_diff_delta'] += commit_diff_delta
        
        return self
    
    def process_modified_file(self, filename, modified_file, developer_email, commit_date, commit_hash):
        """Calculate Diff Delta for a single file modification."""
        week_key = self._get_week_key(commit_date)
        
        # Skip files that shouldn't contribute to velocity
        if self._should_skip_file(filename):
            return 0
        
        diff_delta = 0
        
        # Analyze the diff to categorize changes
        if hasattr(modified_file, 'diff_parsed') and modified_file.diff_parsed:
            added_lines = modified_file.diff_parsed.get('added', [])
            deleted_lines = modified_file.diff_parsed.get('deleted', [])
            
            # Detect moved lines (lines that appear in both added and deleted)
            moved_lines = self._detect_moved_lines(added_lines, deleted_lines)
            
            # Calculate meaningful additions
            meaningful_adds = 0
            for line_num, line_content in added_lines:
                if line_content not in moved_lines and self._is_meaningful_line(line_content):
                    meaningful_adds += 1
                    diff_delta += self.weights['add']
            
            # Calculate meaningful deletions
            meaningful_deletes = 0
            for line_num, line_content in deleted_lines:
                if line_content not in moved_lines and self._is_meaningful_line(line_content):
                    meaningful_deletes += 1
                    diff_delta += self.weights['delete']
            
            # Calculate updates (modified lines)
            updates = min(len(added_lines) - len(moved_lines), 
                         len(deleted_lines) - len(moved_lines))
            if updates > 0:
                meaningful_updates = int(updates * 0.8)  # Assume 80% are meaningful
                diff_delta += meaningful_updates * self.weights['update']
                self.developer_stats[developer_email]['weekly_velocity'][week_key]['lines_updated'] += meaningful_updates
            
            # Track moved lines
            moves = len(moved_lines)
            if moves > 0:
                diff_delta += moves * self.weights['move']
                self.developer_stats[developer_email]['weekly_velocity'][week_key]['lines_moved'] += moves
            
            # Update stats
            self.developer_stats[developer_email]['weekly_velocity'][week_key]['lines_added'] += meaningful_adds
            self.developer_stats[developer_email]['weekly_velocity'][week_key]['lines_deleted'] += meaningful_deletes
            
            # Store file history for code provenance
            self.file_history[filename][commit_hash] = {
                'developer': developer_email,
                'date': commit_date,
                'lines_added': meaningful_adds,
                'lines_deleted': meaningful_deletes,
                'lines_updated': updates
            }
        
        return diff_delta
    
    def _detect_moved_lines(self, added_lines, deleted_lines):
        """Detect lines that were moved rather than truly added/deleted."""
        moved = set()
        deleted_content = {line[1].strip() for line in deleted_lines if line[1].strip()}
        
        for line_num, line_content in added_lines:
            stripped = line_content.strip()
            if stripped and stripped in deleted_content:
                moved.add(line_content)
        
        return moved
    
    def _is_meaningful_line(self, line):
        """Determine if a line is meaningful (not whitespace, comments, etc.)."""
        stripped = line.strip()
        
        # Empty or whitespace-only
        if not stripped:
            return False
        
        # Single character lines (often just braces)
        if len(stripped) <= 1:
            return False
        
        # Common comment patterns
        comment_patterns = [
            r'^\s*#',        # Python, Ruby, Shell
            r'^\s*//',       # C++, Java, JavaScript
            r'^\s*/\*',      # C-style block comment start
            r'^\s*\*',       # C-style block comment continuation
            r'^\s*<!--',     # HTML/XML
            r'^\s*"""',      # Python docstring
            r"^\s*'''",      # Python docstring
        ]
        
        for pattern in comment_patterns:
            if re.match(pattern, line):
                return False
        
        # Import/include statements (lower value)
        import_patterns = [
            r'^\s*import\s+',
            r'^\s*from\s+.*\s+import',
            r'^\s*#include\s*[<"]',
            r'^\s*using\s+',
            r'^\s*require\s*\(',
        ]
        
        for pattern in import_patterns:
            if re.match(pattern, line):
                return False
        
        return True
    
    def _should_skip_file(self, filename):
        """Determine if a file should be skipped for velocity calculations."""
        skip_patterns = [
            r'\.min\.',           # Minified files
            r'\.map$',            # Source maps
            r'package-lock\.json', # Lock files
            r'yarn\.lock',
            r'\.generated\.',     # Generated files
            r'\.auto\.',
            r'vendor/',           # Vendor directories
            r'node_modules/',
            r'\.git/',
            r'\.svg$',            # Binary/data files
            r'\.png$',
            r'\.jpg$',
            r'\.jpeg$',
            r'\.gif$',
            r'\.ico$',
            r'\.woff2?$',
            r'\.ttf$',
            r'\.eot$',
        ]
        
        for pattern in skip_patterns:
            if re.search(pattern, filename):
                return True
        
        return False
    
    def _get_week_key(self, date):
        """Get the week key for a given date."""
        # Start week on Monday
        start_of_week = date - timedelta(days=date.weekday())
        return start_of_week.strftime('%Y-%m-%d')
    
    def get_metrics(self):
        """Get all calculated metrics."""
        metrics = {}
        
        for developer, stats in self.developer_stats.items():
            metrics[developer] = {
                'total_diff_delta': stats['total_diff_delta'],
                'total_commits': stats['total_commits'],
                'weekly_velocity': {}
            }
            
            for week, week_stats in stats['weekly_velocity'].items():
                metrics[developer]['weekly_velocity'][week] = {
                    'diff_delta': week_stats['diff_delta'],
                    'lines_added': week_stats['lines_added'],
                    'lines_updated': week_stats['lines_updated'],
                    'lines_deleted': week_stats['lines_deleted'],
                    'lines_moved': week_stats['lines_moved'],
                    'commits': week_stats['commits'],
                    'files_changed': len(week_stats['files_changed']),
                    'active_days': len(week_stats['active_days']),
                    'velocity_per_day': week_stats['diff_delta'] / max(1, len(week_stats['active_days']))
                }
        
        return metrics
    
    @staticmethod
    def merge_metrics(metrics_list):
        """Merge multiple DiffDelta metrics."""
        if not metrics_list:
            return {}
        
        merged = defaultdict(lambda: {
            'total_diff_delta': 0,
            'total_commits': 0,
            'weekly_velocity': defaultdict(lambda: {
                'diff_delta': 0,
                'lines_added': 0,
                'lines_updated': 0,
                'lines_deleted': 0,
                'lines_moved': 0,
                'commits': 0,
                'files_changed': 0,
                'active_days': 0,
                'velocity_per_day': 0
            })
        })
        
        for metrics in metrics_list:
            for developer, dev_stats in metrics.items():
                merged[developer]['total_diff_delta'] += dev_stats.get('total_diff_delta', 0)
                merged[developer]['total_commits'] += dev_stats.get('total_commits', 0)
                
                for week, week_stats in dev_stats.get('weekly_velocity', {}).items():
                    for key, value in week_stats.items():
                        if key != 'velocity_per_day':
                            merged[developer]['weekly_velocity'][week][key] += value
        
        # Recalculate velocity_per_day after merging
        for developer in merged:
            for week in merged[developer]['weekly_velocity']:
                week_stats = merged[developer]['weekly_velocity'][week]
                week_stats['velocity_per_day'] = (
                    week_stats['diff_delta'] / max(1, week_stats['active_days'])
                )
        
        return dict(merged)