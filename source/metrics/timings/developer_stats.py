# source/metrics/velocity/developer_stats.py
from ...logger import get_logger
from collections import defaultdict
from datetime import timedelta
import json

logger = get_logger(__name__)

class DeveloperStatsAggregator:
    """
    Aggregates all metrics to provide comprehensive per-developer statistics.
    Combines velocity, quality, and productivity metrics into a unified view.
    """
    
    def __init__(self):
        self.developer_stats = defaultdict(lambda: {
            'summary': {},
            'weekly_stats': defaultdict(dict),
            'trends': {}
        })
    
    def aggregate_metrics(self, all_metrics):
        """
        Aggregate all metrics into per-developer statistics.
        
        Args:
            all_metrics: Dictionary containing all calculated metrics by type
        """
        # Process each metric type
        for metric_type, metrics_data in all_metrics.items():
            if metric_type == 'velocity':
                self._process_velocity_metrics(metrics_data)
            elif metric_type == 'productivity':
                self._process_productivity_metrics(metrics_data)
            elif metric_type == 'quality':
                self._process_quality_metrics(metrics_data)
        
        # Calculate trends and additional insights
        self._calculate_trends()
        
        return self.get_developer_stats()
    
    def _process_velocity_metrics(self, velocity_data):
        """Process velocity metrics with corrected time calculations."""
        # Process Diff Delta metrics
        if 'diff_delta' in velocity_data:
            for developer, dev_metrics in velocity_data['diff_delta'].items():
                self.developer_stats[developer]['summary']['total_diff_delta'] = dev_metrics.get('total_diff_delta', 0)
                self.developer_stats[developer]['summary']['total_commits'] = dev_metrics.get('total_commits', 0)
                
                for week, week_stats in dev_metrics.get('weekly_velocity', {}).items():
                    self.developer_stats[developer]['weekly_stats'][week].update({
                        'diff_delta': week_stats.get('diff_delta', 0),
                        'lines_added': week_stats.get('lines_added', 0),
                        'lines_updated': week_stats.get('lines_updated', 0),
                        'lines_deleted': week_stats.get('lines_deleted', 0),
                        'lines_moved': week_stats.get('lines_moved', 0),
                        'velocity_per_day': week_stats.get('velocity_per_day', 0),
                        'active_days': week_stats.get('active_days', 0)
                    })
        
        # Process Code Provenance metrics
        if 'code_provenance' in velocity_data:
            for developer, dev_metrics in velocity_data['code_provenance'].items():
                for week, week_stats in dev_metrics.get('weekly_provenance', {}).items():
                    self.developer_stats[developer]['weekly_stats'][week].update({
                        'new_code_percent': week_stats.get('new_code_percent', 0),
                        'recent_code_percent': week_stats.get('recent_code_percent', 0),
                        'old_code_percent': week_stats.get('old_code_percent', 0),
                        'legacy_code_percent': week_stats.get('legacy_code_percent', 0)
                    })
        
        # Process Developer Hours metrics
        if 'developer_hours' in velocity_data:
            for developer, dev_metrics in velocity_data['developer_hours'].items():
                self.developer_stats[developer]['summary']['total_estimated_hours'] = dev_metrics.get('total_estimated_hours', 0)
                
                for week, week_stats in dev_metrics.get('weekly_hours', {}).items():
                    self.developer_stats[developer]['weekly_stats'][week].update({
                        'estimated_hours': week_stats.get('estimated_hours', 0),
                        'sessions': week_stats.get('sessions', 0),
                        'hours_per_day': week_stats.get('hours_per_day', 0)
                    })
        
        # Process Code Domain metrics
        if 'code_domain' in velocity_data:
            for developer, dev_metrics in velocity_data['code_domain'].items():
                self.developer_stats[developer]['summary']['domain_distribution'] = dev_metrics.get('domain_percentages', {})
                
                for week, week_stats in dev_metrics.get('weekly_domains', {}).items():
                    self.developer_stats[developer]['weekly_stats'][week]['domain_focus'] = week_stats.get('percentages', {})
        
        # Process Comprehensive Time Analysis metrics
        if 'comprehensive_time_analysis' in velocity_data:
            for developer, dev_metrics in velocity_data['comprehensive_time_analysis'].items():
                # Add comprehensive time analysis summary data
                basic_stats = dev_metrics.get('basic_stats', {})
                self.developer_stats[developer]['summary'].update({
                    'total_repos_worked_on': basic_stats.get('total_repos', 0),
                    'total_span_days': basic_stats.get('total_span_days', 0),
                    'actual_active_days': basic_stats.get('actual_active_days', 0),
                    'activity_density': basic_stats.get('activity_density', 0),  # How much of span was active
                    'commits_per_day': basic_stats.get('commits_per_day', 0),
                    'total_lines_changed': basic_stats.get('total_lines_changed', 0),
                    'total_files_changed': basic_stats.get('total_files_changed', 0)
                })
                
                # Add work patterns and rhythm data with corrected units
                work_sessions = dev_metrics.get('work_sessions', {})
                rhythm_analysis = dev_metrics.get('rhythm_analysis', {})
                sustained_activity = dev_metrics.get('sustained_activity', {})
                downtime_analysis = dev_metrics.get('downtime_analysis', {})
                daily_patterns = dev_metrics.get('daily_patterns', {})
                weekly_patterns = dev_metrics.get('weekly_patterns', {})
                timing_patterns = dev_metrics.get('timing_patterns', {})
                
                self.developer_stats[developer]['summary'].update({
                    # Session metrics with proper units
                    'avg_session_length_hours': work_sessions.get('avg_session_length_hours', 0),
                    'avg_session_length_minutes': work_sessions.get('avg_session_length_minutes', 0),
                    'avg_commits_per_session': work_sessions.get('avg_commits_per_session', 0),
                    'session_count': work_sessions.get('session_count', 0),
                    
                    # Rhythm metrics with corrected calculations
                    'rhythm_score': rhythm_analysis.get('rhythm_score', 0),
                    'burst_coding_tendency': rhythm_analysis.get('burst_coding_tendency', 0),
                    'steady_pace_score': rhythm_analysis.get('steady_pace_score', 0),
                    'session_consistency_score': rhythm_analysis.get('session_consistency_score', 0),
                    
                    # CORRECTED: Inter-session downtime (realistic values)
                    'inter_session_downtime_avg_hours': rhythm_analysis.get('inter_session_downtime_avg_hours', 0),
                    'inter_session_downtime_avg_days': rhythm_analysis.get('inter_session_downtime_avg_days', 0),
                    'total_inter_session_breaks': rhythm_analysis.get('total_inter_session_breaks', 0),
                    
                    # CORRECTED: Intra-session timing
                    'intra_session_commit_intervals_avg_minutes': rhythm_analysis.get('intra_session_commit_intervals_avg_minutes', 0),
                    
                    # Activity and downtime with proper units
                    'longest_streak_days': sustained_activity.get('longest_streak_days', 0),
                    'avg_activity_days_per_week': sustained_activity.get('avg_activity_days_per_week', 0),
                    
                    # Downtime analysis with realistic breakdowns
                    'avg_downtime_minutes': downtime_analysis.get('avg_downtime_minutes', 0),
                    'rapid_coding_sessions': downtime_analysis.get('rapid_coding_sessions', 0),
                    'short_breaks_count': downtime_analysis.get('short_breaks_count', 0),
                    'medium_breaks_count': downtime_analysis.get('medium_breaks_count', 0),
                    'long_breaks_count': downtime_analysis.get('long_breaks_count', 0),
                    'very_long_breaks_count': downtime_analysis.get('very_long_breaks_count', 0),
                    
                    # Timing patterns with proper units
                    'avg_commit_interval_minutes': timing_patterns.get('mean_interval_minutes', 0),
                    'median_commit_interval_minutes': timing_patterns.get('median_interval_minutes', 0),
                    'max_gap_between_commits_days': timing_patterns.get('max_interval_days', 0),
                    
                    # Enhanced daily pattern metrics
                    'peak_hour': daily_patterns.get('peak_hour', 0),
                    'active_hours_span': daily_patterns.get('active_hours_span', 0),
                    'working_hours_consistency': daily_patterns.get('working_hours_consistency', 0),
                    'work_pattern_type': daily_patterns.get('work_pattern_type', 'unknown'),
                    'working_hours_percentage': daily_patterns.get('working_hours_percentage', 0),
                    'peak_activity_period': daily_patterns.get('peak_activity_period', 'unknown'),
                    
                    # Weekly patterns
                    'weeks_active': weekly_patterns.get('weeks_active', 0),
                    'weekly_consistency_score': weekly_patterns.get('consistency_score', 0)
                })
    
    def _process_productivity_metrics(self, productivity_data):
        pass
    
    def _process_quality_metrics(self, quality_data):
        if 'bugs' in quality_data:
            bug_metrics = quality_data['bugs']
        
        if 'test_doc_pct' in quality_data:
            quality_scores = quality_data['test_doc_pct']
        
        if 'meaningful_code' in quality_data:
            meaningful_metrics = quality_data['meaningful_code']
    
    def _calculate_trends(self):
        """Calculate trends and insights for each developer."""
        for developer, stats in self.developer_stats.items():
            weekly_stats = stats['weekly_stats']
            if not weekly_stats:
                continue
            
            # Sort weeks chronologically
            sorted_weeks = sorted(weekly_stats.keys())
            
            # Calculate velocity trend (is developer speeding up or slowing down?)
            if len(sorted_weeks) >= 2:
                recent_weeks = sorted_weeks[-4:]  # Last 4 weeks
                older_weeks = sorted_weeks[-8:-4] if len(sorted_weeks) >= 8 else sorted_weeks[:-4]
                
                recent_velocity = sum(weekly_stats[w].get('diff_delta', 0) for w in recent_weeks) / len(recent_weeks)
                older_velocity = sum(weekly_stats[w].get('diff_delta', 0) for w in older_weeks) / len(older_weeks) if older_weeks else recent_velocity
                
                velocity_trend = ((recent_velocity - older_velocity) / older_velocity * 100) if older_velocity > 0 else 0
                
                stats['trends']['velocity_trend'] = round(velocity_trend, 2)
                stats['trends']['velocity_consistency'] = self._calculate_consistency(
                    [weekly_stats[w].get('diff_delta', 0) for w in sorted_weeks[-8:]]
                )
            
            # Calculate work pattern insights
            total_weeks = len(weekly_stats)
            active_weeks = sum(1 for w in weekly_stats.values() if w.get('diff_delta', 0) > 0)
            stats['trends']['activity_rate'] = round((active_weeks / total_weeks * 100) if total_weeks > 0 else 0, 2)
            
            # Average statistics
            stats['summary']['avg_weekly_velocity'] = round(
                sum(w.get('diff_delta', 0) for w in weekly_stats.values()) / max(1, active_weeks), 2
            )
            stats['summary']['avg_weekly_hours'] = round(
                sum(w.get('estimated_hours', 0) for w in weekly_stats.values()) / max(1, active_weeks), 2
            )
    
    def _calculate_consistency(self, values):
        """Calculate consistency score (lower is more consistent)."""
        if len(values) < 2:
            return 100.0
        
        import statistics
        mean = statistics.mean(values) if values else 0
        if mean == 0:
            return 0.0
        
        stdev = statistics.stdev(values) if len(values) > 1 else 0
        coefficient_of_variation = (stdev / mean) * 100 if mean > 0 else 0
        return round(100 - coefficient_of_variation, 2)  # Higher score = more consistent
    
    def get_developer_stats(self):
        """Return the aggregated developer statistics."""
        return dict(self.developer_stats)
    
    def get_summary_report(self):
        """Generate a summary report of all developers."""
        report = {
            'total_developers': len(self.developer_stats),
            'top_contributors': [],
            'most_consistent': [],
            'most_active': []
        }
        
        # Sort developers by different criteria
        sorted_by_velocity = sorted(
            self.developer_stats.items(),
            key=lambda x: x[1]['summary'].get('total_diff_delta', 0),
            reverse=True
        )
        
        sorted_by_consistency = sorted(
            self.developer_stats.items(),
            key=lambda x: x[1]['trends'].get('velocity_consistency', 0),
            reverse=True
        )
        
        sorted_by_activity = sorted(
            self.developer_stats.items(),
            key=lambda x: x[1]['trends'].get('activity_rate', 0),
            reverse=True
        )
        
        # Top 5 in each category
        report['top_contributors'] = [(dev, stats['summary'].get('total_diff_delta', 0)) 
                                     for dev, stats in sorted_by_velocity[:5]]
        report['most_consistent'] = [(dev, stats['trends'].get('velocity_consistency', 0)) 
                                    for dev, stats in sorted_by_consistency[:5]]
        report['most_active'] = [(dev, stats['trends'].get('activity_rate', 0)) 
                                for dev, stats in sorted_by_activity[:5]]
        
        return report