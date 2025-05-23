# source/metrics/velocity/developer_hours.py
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
        self.max_commit_gap = timedelta(hours=2)  # Max time between commits in same session
        self.min_session_length = timedelta(minutes=30)  # Minimum session length
        self.default_first_commit_time = timedelta(minutes=30)  # Time before first commit
        self.default_last_commit_time = timedelta(minutes=15)  # Time after last commit
        
    def process_commit(self, commit):
        """Process commit for hour estimation."""
        developer_email = commit.author.email.strip().lower()
        commit_datetime = commit.committer_date
        
        # Add commit to developer's timeline
        self.developer_sessions[developer_email].append({
            'timestamp': commit_datetime,
            'commit': commit.hash,
            'changes': commit.insertions + commit.deletions
        })
        
        return self
    
    def process_modified_file(self, filename, modified_file, author_name, commit_date, commit_hash=None):
        """Not used for this metric - all processing done at commit level."""
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
    
    def _calculate_session_hours(self, session):
        """Calculate estimated hours for a coding session."""
        duration = session['duration']
        hours = duration.total_seconds() / 3600
        
        # Apply adjustments based on session characteristics
        commit_count = len(session['commits'])
        changes = session['total_changes']
        
        # Adjustment factors
        if commit_count == 1:
            # Single commit sessions might be overestimated
            hours *= 0.75
        elif commit_count > 10:
            # Many commits might indicate automated activity
            hours *= 0.9
        
        # Adjust based on change volume
        changes_per_hour = changes / max(0.1, hours)
        if changes_per_hour > 1000:
            # Very high change rate might indicate generated code
            hours *= 0.8
        elif changes_per_hour < 50:
            # Low change rate might indicate research/debugging
            hours *= 1.1
        
        # Cap session length at 8 hours
        return min(hours, 8.0)
    
    def _get_week_key(self, date):
        """Get week key for date."""
        year, week, _ = date.isocalendar()
        return f"Week_{week}_{year}-{date.strftime('%m-%d')}"
    
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
        
        # Recalculate hours per day for merged weekly data
        result = {}
        for developer, stats in merged.items():
            result[developer] = {
                'total_estimated_hours': stats['total_estimated_hours'],
                'total_sessions': stats['total_sessions'],
                'weekly_hours': {}
            }
            
            for week, week_stats in stats['weekly_hours'].items():
                # Assume 7 days per week for simplicity in merged data
                hours_per_day = week_stats['estimated_hours'] / 7 if week_stats['estimated_hours'] > 0 else 0
                
                result[developer]['weekly_hours'][week] = {
                    'estimated_hours': round(week_stats['estimated_hours'], 2),
                    'sessions': week_stats['sessions'],
                    'commits': week_stats['commits'],
                    'hours_per_day': round(hours_per_day, 2)
                }
        
        return result