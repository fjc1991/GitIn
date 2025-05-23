from ...logger import get_logger
from ..base import BaseMetric
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import mean, median
import statistics

logger = get_logger(__name__)

class ComprehensiveTimeAnalysisMetric(BaseMetric):
    """
    Comprehensive time-based analysis for developers across all repositories.
    
    Measures:
    - Active/Rest periods and downtime analysis
    - Work session clustering and patterns
    - Coding rhythm and consistency
    - Inter-commit timing patterns
    - Peak productivity hours/days
    - Sustained activity periods
    """
    
    def __init__(self):
        super().__init__()
        self.author_activities = defaultdict(list)  # author -> [(timestamp, repo, commit_hash, lines_changed)]
        self.author_repos = defaultdict(set)  # author -> set of repos they've worked on
        self.repo_activities = defaultdict(list)  # repo -> [(timestamp, author, commit_hash, lines_changed)]
        
    def process_commit(self, commit):
        """Process a commit and extract timing information"""
        author_email = commit.author.email.strip().lower()
        author_name = commit.author.name.strip()
        commit_date = commit.committer_date
        repo_path = getattr(commit, 'project_path', 'unknown')
        
        # Calculate total lines changed in this commit
        total_lines = sum(mf.added_lines + mf.deleted_lines for mf in commit.modified_files)
        
        # Store activity data
        activity_record = {
            'timestamp': commit_date,
            'repo': repo_path,
            'commit_hash': commit.hash,
            'lines_changed': total_lines,
            'files_changed': len(commit.modified_files),
            'author_name': author_name
        }
        
        self.author_activities[author_email].append(activity_record)
        self.author_repos[author_email].add(repo_path)
        self.repo_activities[repo_path].append({
            **activity_record,
            'author_email': author_email
        })
        
        return self
    
    def process_modified_file(self, filename, modified_file, author_name, commit_date, commit_hash=None):
        """Process a modified file - required abstract method implementation"""
        # For comprehensive time analysis, we don't need per-file processing
        # as we handle everything at the commit level
        return self
    
    def get_metrics(self, commits=None, start_date=None, end_date=None):
        """Calculate comprehensive time-based metrics for all developers"""
        # If commits are passed directly, process them
        if commits:
            for commit in commits:
                # Handle both dict and object formats
                if isinstance(commit, dict):
                    # Convert dict format to activities
                    activity = {
                        'timestamp': commit.get('date'),
                        'author': commit.get('author', {}).get('email', ''),
                        'lines_changed': commit.get('stats', {}).get('total', 0),
                        'files_changed': len(commit.get('files', []))
                    }
                    author_email = activity['author'].strip().lower()
                    self.author_activities[author_email].append(activity)
                else:
                    # Process as commit object
                    self.process_commit(commit)
        
        metrics = {}
        
        for author_email, activities in self.author_activities.items():
            if len(activities) < 2:  # Need at least 2 commits for timing analysis
                continue
                
            # Sort activities by timestamp
            activities.sort(key=lambda x: x['timestamp'])
            
            author_metrics = self._analyze_author_time_patterns(author_email, activities)
            if author_metrics:
                metrics[author_email] = author_metrics
                
        return metrics
    
    def _analyze_author_time_patterns(self, author_email, activities):
        """Analyze time patterns for a single author"""
        if len(activities) < 2:
            return None
            
        # Basic time span analysis
        first_commit = activities[0]['timestamp']
        last_commit = activities[-1]['timestamp']
        total_span_days = (last_commit - first_commit).total_seconds() / 86400
        
        # Calculate inter-commit intervals
        intervals = []
        for i in range(1, len(activities)):
            interval = (activities[i]['timestamp'] - activities[i-1]['timestamp']).total_seconds()
            intervals.append(interval)
        
        # Work session analysis
        work_sessions = self._identify_work_sessions(activities)
        
        # Daily/weekly patterns
        daily_patterns = self._analyze_daily_patterns(activities)
        weekly_patterns = self._analyze_weekly_patterns(activities)
        
        # Downtime analysis
        downtime_analysis = self._analyze_downtime_patterns(intervals)
        
        # Productivity rhythm analysis
        rhythm_analysis = self._analyze_coding_rhythm(activities, work_sessions)
        
        # Sustained activity analysis
        sustained_activity = self._analyze_sustained_activity(activities)
        
        return {
            'basic_stats': {
                'total_commits': len(activities),
                'total_repos': len(self.author_repos.get(author_email, [])),
                'first_commit_date': first_commit.isoformat(),
                'last_commit_date': last_commit.isoformat(),
                'total_span_days': round(total_span_days, 2),
                'commits_per_day': round(len(activities) / max(total_span_days, 1), 3),
                'total_lines_changed': sum(a.get('lines_changed', 0) for a in activities),
                'total_files_changed': sum(a.get('files_changed', 0) for a in activities)
            },
            'timing_patterns': {
                'mean_interval_hours': round(mean(intervals) / 3600, 2) if intervals else 0,
                'median_interval_hours': round(median(intervals) / 3600, 2) if intervals else 0,
                'min_interval_minutes': round(min(intervals) / 60, 2) if intervals else 0,
                'max_interval_days': round(max(intervals) / 86400, 2) if intervals else 0
            },
            'work_sessions': work_sessions,
            'daily_patterns': daily_patterns,
            'weekly_patterns': weekly_patterns,
            'downtime_analysis': downtime_analysis,
            'rhythm_analysis': rhythm_analysis,
            'sustained_activity': sustained_activity
        }
    
    def _identify_work_sessions(self, activities, session_gap_hours=4):
        """Identify work sessions based on commit clustering"""
        if len(activities) < 2:
            return {'session_count': 0, 'avg_session_length_hours': 0, 'avg_commits_per_session': 0}
        
        sessions = []
        current_session_start = activities[0]['timestamp']
        current_session_commits = 1
        session_gap_seconds = session_gap_hours * 3600
        
        for i in range(1, len(activities)):
            time_since_last = (activities[i]['timestamp'] - activities[i-1]['timestamp']).total_seconds()
            
            if time_since_last <= session_gap_seconds:
                # Continue current session
                current_session_commits += 1
            else:
                # End current session, start new one
                session_length = (activities[i-1]['timestamp'] - current_session_start).total_seconds() / 3600
                sessions.append({
                    'start': current_session_start,
                    'end': activities[i-1]['timestamp'],
                    'length_hours': session_length,
                    'commits': current_session_commits
                })
                current_session_start = activities[i]['timestamp']
                current_session_commits = 1
        
        # Add final session
        if current_session_commits > 0:
            session_length = (activities[-1]['timestamp'] - current_session_start).total_seconds() / 3600
            sessions.append({
                'start': current_session_start,
                'end': activities[-1]['timestamp'],
                'length_hours': session_length,
                'commits': current_session_commits
            })
        
        if not sessions:
            return {'session_count': 0, 'avg_session_length_hours': 0, 'avg_commits_per_session': 0}
        
        return {
            'session_count': len(sessions),
            'avg_session_length_hours': round(mean(s['length_hours'] for s in sessions), 2),
            'max_session_length_hours': round(max(s['length_hours'] for s in sessions), 2),
            'avg_commits_per_session': round(mean(s['commits'] for s in sessions), 2),
            'max_commits_per_session': max(s['commits'] for s in sessions),
            'sessions': [
                {
                    'start': s['start'].isoformat(),
                    'end': s['end'].isoformat(),
                    'length_hours': round(s['length_hours'], 2),
                    'commits': s['commits']
                } for s in sessions[-5:]  # Only keep last 5 sessions for brevity
            ]
        }
    
    def _analyze_daily_patterns(self, activities):
        """Analyze daily patterns from activities."""
        if not activities:
            return {}
        
        from collections import defaultdict
        
        day_counts = defaultdict(int)
        
        for activity in activities:
            day_counts[activity['timestamp'].weekday()] += 1
        
        # Find peak days
        peak_day = max(day_counts.items(), key=lambda x: x[1]) if day_counts else (0, 0)
        
        return {
            'peak_day': peak_day[0],
            'peak_day_count': peak_day[1],
            'day_distribution': dict(day_counts)
        }
    
    def _analyze_weekly_patterns(self, activities):
        """Analyze weekly patterns from activities."""
        if not activities:
            return {}
        
        from collections import defaultdict
        
        week_counts = defaultdict(int)
        
        for activity in activities:
            # Get week number
            week_key = activity['timestamp'].strftime('%Y-W%U')
            week_counts[week_key] += 1
        
        if week_counts:
            import statistics
            weekly_activity_counts = list(week_counts.values())
            return {
                'total_weeks': len(week_counts),
                'avg_activities_per_week': statistics.mean(weekly_activity_counts),
                'max_activities_per_week': max(weekly_activity_counts),
                'min_activities_per_week': min(weekly_activity_counts)
            }
        
        return {'total_weeks': 0}
    
    def _analyze_downtime_patterns(self, intervals):
        """Analyze downtime patterns from intervals."""
        if not intervals:
            return {}
        
        # Convert to hours
        intervals_hours = [interval / 3600 for interval in intervals]
        
        # Categorize intervals
        short_breaks = [i for i in intervals_hours if i <= 4]  # <= 4 hours
        long_breaks = [i for i in intervals_hours if i > 24]   # > 24 hours
        
        return {
            'short_breaks_count': len(short_breaks),
            'long_breaks_count': len(long_breaks),
            'avg_break_hours': sum(intervals_hours) / len(intervals_hours) if intervals_hours else 0
        }
    
    def _analyze_coding_rhythm(self, activities, work_sessions):
        """Analyze coding rhythm."""
        if not activities:
            return {}
        
        # Simple rhythm analysis based on activity frequency
        total_days = (activities[-1]['timestamp'] - activities[0]['timestamp']).days + 1
        
        return {
            'activities_per_day': len(activities) / total_days if total_days > 0 else 0,
            'consistency_score': work_sessions.get('total_sessions', 0) / total_days if total_days > 0 else 0
        }
    
    def _analyze_sustained_activity(self, activities):
        """Analyze sustained activity patterns."""
        if not activities:
            return {}
        
        from collections import defaultdict
        
        # Group by date
        daily_activities = defaultdict(int)
        for activity in activities:
            date_key = activity['timestamp'].date()
            daily_activities[date_key] += 1
        
        # Find streaks of consecutive days with activity
        sorted_dates = sorted(daily_activities.keys())
        current_streak = 1
        max_streak = 1
        
        for i in range(1, len(sorted_dates)):
            if (sorted_dates[i] - sorted_dates[i-1]).days == 1:
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 1
        
        return {
            'total_active_days': len(daily_activities),
            'max_consecutive_days': max_streak,
            'avg_activities_per_active_day': sum(daily_activities.values()) / len(daily_activities) if daily_activities else 0
        }
    
    @staticmethod
    def merge_metrics(metrics_list):
        """Merge metrics from multiple instances"""
        if not metrics_list:
            return {}
        
        # For time analysis, we need to carefully merge author data
        merged = {}
        
        for metrics in metrics_list:
            if not metrics:
                continue
            for author, author_metrics in metrics.items():
                if author not in merged:
                    merged[author] = author_metrics
                else:
                    # Merge basic stats
                    merged[author]['basic_stats']['total_commits'] += author_metrics['basic_stats']['total_commits']
                    merged[author]['basic_stats']['total_lines_changed'] += author_metrics['basic_stats']['total_lines_changed']
                    merged[author]['basic_stats']['total_files_changed'] += author_metrics['basic_stats']['total_files_changed']
                    
                    # Update date ranges
                    if author_metrics['basic_stats']['first_commit_date'] < merged[author]['basic_stats']['first_commit_date']:
                        merged[author]['basic_stats']['first_commit_date'] = author_metrics['basic_stats']['first_commit_date']
                    if author_metrics['basic_stats']['last_commit_date'] > merged[author]['basic_stats']['last_commit_date']:
                        merged[author]['basic_stats']['last_commit_date'] = author_metrics['basic_stats']['last_commit_date']
                    
                    # For other metrics, take the average or most recent
                    for section in ['work_sessions', 'daily_patterns', 'weekly_patterns', 'downtime_analysis', 'rhythm_analysis', 'sustained_activity']:
                        if section in author_metrics:
                            merged[author][section] = author_metrics[section]
        
        return merged
