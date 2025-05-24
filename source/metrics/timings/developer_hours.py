from ...logger import get_logger
from ..base import BaseMetric
from collections import defaultdict
from datetime import datetime, timedelta
import statistics

logger = get_logger(__name__)

class DeveloperHoursMetric(BaseMetric):
    """
    Estimates developer hours based on commit patterns.
    
    Based on GitClear's approach:
    - Uses commit timestamps to identify coding sessions
    - Estimates time between commits within a session
    - Accounts for breaks and context switching
    - Provides conservative estimates to avoid overestimation
    """
    
    def __init__(self):
        super().__init__()
        self.developer_sessions = defaultdict(list)  # developer -> list of sessions
        self.developer_stats = defaultdict(lambda: {
            'weekly_hours': defaultdict(lambda: {
                'estimated_hours': 0,
                'sessions': 0,
                'commits': 0,
                'avg_session_length': 0,
                'productive_days': set(),
                'hours_per_day': 0
            }),
            'total_hours': 0,
            'total_sessions': 0,
            'total_estimated_hours': 0
        })
        
        # Session parameters (based on GitClear research)
        # Defines the maximum time allowed between two commits for them to be considered part of the same coding session.
        self.max_commit_gap = timedelta(hours=2)
        # Defines the minimum total duration for a series of commits to be recognized as a valid coding session.
        self.min_session_length = timedelta(minutes=30)
        # Estimated time spent on work before the first commit of a session is made.
        self.default_first_commit_time = timedelta(minutes=30)
        # Estimated time spent on wrapping up work after the last commit of a session.
        self.default_last_commit_time = timedelta(minutes=15)
        
    def process_commit(self, commit):
        """Process commit for hour estimation."""
        developer_email = commit.author.email.strip().lower()
        commit_datetime = commit.committer_date
        
        # Calculate both raw and meaningful changes
        raw_changes = commit.insertions + commit.deletions
        meaningful_changes = self._calculate_meaningful_changes(commit)
        
        # Add commit to developer's timeline
        self.developer_sessions[developer_email].append({
            'timestamp': commit_datetime,
            'commit': commit.hash,
            'raw_changes': raw_changes,
            'meaningful_changes': meaningful_changes,
            'changes': meaningful_changes if meaningful_changes > 0 else raw_changes  # Use meaningful if available
        })
        
        return self
    
    def process_modified_file(self, filename, modified_file, author_name, commit_date, commit_hash=None):
        # This metric calculates developer hours based on commit-level data (timestamps and aggregate changes). Per-file analysis is not required for this estimation approach.
        return self
    
    def get_metrics(self):
        """Calculate and return developer hours metrics."""
        self.calculate_sessions()
        
        # Convert sets to counts for JSON serialization
        result = {}
        for developer, stats in self.developer_stats.items():
            dev_result = {
                'total_estimated_hours': stats['total_estimated_hours'],
                'total_sessions': stats['total_sessions'],
                'weekly_hours': {}
            }
            
            for week, week_stats in stats['weekly_hours'].items():
                dev_result['weekly_hours'][week] = {
                    'estimated_hours': round(week_stats['estimated_hours'], 2),
                    'sessions': week_stats['sessions'],
                    'commits': week_stats['commits'],
                    'hours_per_day': round(week_stats['hours_per_day'], 2)
                }
            
            result[developer] = dev_result
        
        return result
    
    def calculate_sessions(self):
        """Calculate coding sessions from commit patterns."""
        for developer, commits in self.developer_sessions.items():
            # Sort commits by timestamp
            sorted_commits = sorted(commits, key=lambda x: x['timestamp'])
            
            sessions = []
            current_session = None
            
            for commit_data in sorted_commits:
                timestamp = commit_data['timestamp']
                
                if current_session is None:
                    # Start new session
                    current_session = {
                        'start': timestamp - self.default_first_commit_time,
                        'end': timestamp,
                        'commits': [commit_data],
                        'total_changes': commit_data['changes']
                    }
                else:
                    # Check if this commit belongs to current session
                    time_since_last = timestamp - current_session['end']
                    
                    if time_since_last <= self.max_commit_gap:
                        # Add to current session
                        current_session['end'] = timestamp
                        current_session['commits'].append(commit_data)
                        current_session['total_changes'] += commit_data['changes']
                    else:
                        # Finalize current session and start new one
                        current_session['end'] += self.default_last_commit_time
                        current_session['duration'] = current_session['end'] - current_session['start']
                        sessions.append(current_session)
                        
                        # Start new session
                        current_session = {
                            'start': timestamp - self.default_first_commit_time,
                            'end': timestamp,
                            'commits': [commit_data],
                            'total_changes': commit_data['changes']
                        }
            
            # Finalize last session
            if current_session:
                current_session['end'] += self.default_last_commit_time
                current_session['duration'] = current_session['end'] - current_session['start']
                sessions.append(current_session)
            
            # Process sessions into weekly stats
            self._process_sessions_to_weekly_stats(developer, sessions)
    
    def _process_sessions_to_weekly_stats(self, developer, sessions):
        """Convert sessions into weekly statistics."""
        for session in sessions:
            # Skip sessions that are too short
            if session['duration'] < self.min_session_length:
                continue
            
            # Get week key for session start
            week_key = self._get_week_key(session['start'])
            
            # Calculate session hours (with some adjustments)
            session_hours = self._calculate_session_hours(session)
            
            # Update weekly stats
            week_stats = self.developer_stats[developer]['weekly_hours'][week_key]
            week_stats['estimated_hours'] += session_hours
            week_stats['sessions'] += 1
            week_stats['commits'] += len(session['commits'])
            week_stats['productive_days'].add(session['start'].strftime('%Y-%m-%d'))
            
            # Update totals
            self.developer_stats[developer]['total_hours'] += session_hours
            self.developer_stats[developer]['total_sessions'] += 1
            self.developer_stats[developer]['total_estimated_hours'] += session_hours
        
        # Calculate hours per day for each week
        for week_key, week_stats in self.developer_stats[developer]['weekly_hours'].items():
            if len(week_stats['productive_days']) > 0:
                week_stats['hours_per_day'] = week_stats['estimated_hours'] / len(week_stats['productive_days'])
    
    def _calculate_meaningful_changes(self, commit):
        """
        Calculate meaningful changes by analyzing diff deltas.
        Filters out trivial changes like whitespace, comments, and generated code.
        """
        meaningful_count = 0
        
        try:
            for modified_file in commit.modified_files:
                # Skip generated files, configs, and documentation
                if self._is_trivial_file(modified_file.filename):
                    continue
                
                # Analyze diff deltas for meaningful content
                if hasattr(modified_file, 'diff_parsed') and modified_file.diff_parsed:
                    for delta in modified_file.diff_parsed['added']:
                        if self._is_meaningful_line(delta):
                            meaningful_count += 1
                    
                    for delta in modified_file.diff_parsed['deleted']:
                        if self._is_meaningful_line(delta):
                            meaningful_count += 1
                else:
                    # Fallback: use a portion of raw changes as meaningful
                    file_changes = (modified_file.added_lines or 0) + (modified_file.deleted_lines or 0)
                    meaningful_count += int(file_changes * 0.7)  # Assume 70% are meaningful
                    
        except Exception as e:
            logger.warning(f"Error calculating meaningful changes for commit {commit.hash}: {e}")
            return 0
            
        return meaningful_count
    
    def _is_trivial_file(self, filename):
        """Check if file is likely to contain trivial changes."""
        trivial_extensions = {'.json', '.xml', '.yaml', '.yml', '.lock', '.md', '.txt'}
        trivial_patterns = {'package-lock.json', 'yarn.lock', 'composer.lock', 'Pipfile.lock'}
        
        return (any(filename.endswith(ext) for ext in trivial_extensions) or
                any(pattern in filename for pattern in trivial_patterns) or
                'generated' in filename.lower() or
                'dist/' in filename or
                'build/' in filename)
    
    def _is_meaningful_line(self, line_content):
        """Determine if a line represents meaningful code change."""
        line = line_content.strip()
        
        # Skip empty lines, comments, and trivial changes
        if (not line or 
            line.startswith('//') or 
            line.startswith('#') or 
            line.startswith('/*') or 
            line.startswith('*') or
            line in ['{', '}', '(', ')', '[', ']', ';'] or
            len(line) < 3):
            return False
            
        # Skip import/include statements (often auto-generated)
        if any(line.startswith(keyword) for keyword in ['import ', 'from ', '#include', 'using ', 'require']):
            return False
            
        return True

    def _calculate_session_hours(self, session):
        """Calculate estimated hours for a coding session."""
        duration = session['duration']
        hours = duration.total_seconds() / 3600
        
        # Apply adjustments based on session characteristics
        commit_count = len(session['commits'])
        # Use meaningful changes if available, fallback to raw changes
        changes = sum(commit.get('meaningful_changes', commit.get('changes', 0)) for commit in session['commits'])
        
        # Adjustment factors
        # Single commit sessions are often shorter or less representative of continuous work, hence reduced.
        if commit_count == 1:
            # Single commit sessions might be overestimated
            hours *= 0.75
        # Sessions with a very high number of commits might indicate automated processes (e.g., bulk refactoring, script-generated commits) rather than typical developer work.
        elif commit_count > 10:
            # Many commits might indicate automated activity
            hours *= 0.9
        
        # Adjust based on change volume
        # 'changes' currently refers to raw line counts. Adjustments based on this might be less accurate than if based on 'meaningful' changes.
        changes_per_hour = changes / max(0.1, hours) # max(0.1, hours) to avoid division by zero for very short sessions
        # Extremely high change rates can indicate bulk operations (e.g., adding large generated files, formatting changes) not reflective of typical coding effort per hour.
        if changes_per_hour > 1000:
            # Very high change rate might indicate generated code
            hours *= 0.8
        # Very low change rates might suggest activities like deep research, debugging, or problem-solving that involve less code modification but are still valuable time spent.
        elif changes_per_hour < 50:
            # Low change rate might indicate research/debugging
            hours *= 1.1
        
        # Cap session length at 8 hours
        return min(hours, 8.0)
    
    def _get_week_key(self, date):
        """Get week key for date."""
        year, week, _ = date.isocalendar()
        return f"{year}-W{week:02d}" # Example: 2023-W34
    
    @staticmethod
    def merge_metrics(metrics_list):
        """Merge multiple developer hours metrics."""
        if not metrics_list:
            return {}
        
        merged = defaultdict(lambda: {
            'total_estimated_hours': 0,
            'total_sessions': 0,
            'weekly_hours': defaultdict(lambda: {
                'estimated_hours': 0,
                'sessions': 0,
                'commits': 0,
                'productive_days': set(), # Add this
                'hours_per_day': 0
            })
        })
        
        # Merge all metrics
        for metrics in metrics_list:
            if not metrics:
                continue
                
            for developer, dev_stats in metrics.items():
                merged[developer]['total_estimated_hours'] += dev_stats.get('total_estimated_hours', 0)
                merged[developer]['total_sessions'] += dev_stats.get('total_sessions', 0)
                
                # Merge weekly hours
                for week, week_stats in dev_stats.get('weekly_hours', {}).items():
                    merged[developer]['weekly_hours'][week]['estimated_hours'] += week_stats.get('estimated_hours', 0)
                    merged[developer]['weekly_hours'][week]['sessions'] += week_stats.get('sessions', 0)
                    merged[developer]['weekly_hours'][week]['commits'] += week_stats.get('commits', 0)
                    # Ensure productive_days are merged as a set
                    if 'productive_days' in week_stats: # Check if productive_days exists
                         merged[developer]['weekly_hours'][week]['productive_days'].update(week_stats['productive_days'])
        
        # Recalculate hours per day for merged weekly data
        result = {}
        for developer, stats in merged.items():
            result[developer] = {
                'total_estimated_hours': stats['total_estimated_hours'],
                'total_sessions': stats['total_sessions'],
                'weekly_hours': {}
            }
            
            for week, week_stats in stats['weekly_hours'].items():
                num_productive_days = len(week_stats['productive_days']) # Get count from merged set
                hours_per_day = week_stats['estimated_hours'] / num_productive_days if num_productive_days > 0 else 0
                
                result[developer]['weekly_hours'][week] = {
                    'estimated_hours': round(week_stats['estimated_hours'], 2),
                    'sessions': week_stats['sessions'],
                    'commits': week_stats['commits'],
                    'hours_per_day': round(hours_per_day, 2)
                }
        
        return result