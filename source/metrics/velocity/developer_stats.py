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
        """Process velocity metrics (diff delta, provenance, hours, domains)."""
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
    
    def _process_productivity_metrics(self, productivity_data):
        """Process existing productivity metrics."""
        # These would come from the existing metrics like commits, contributors, etc.
        # We'll extract per-developer data where available
        pass
    
    def _process_quality_metrics(self, quality_data):
        """Process quality metrics (bugs, test coverage, meaningful code)."""
        # Process bug-fixing percentage
        if 'bugs' in quality_data:
            bug_metrics = quality_data['bugs']
            # Extract per-developer bug-fixing work if available
            # This might require modification of the BugsMetric class to track by developer
        
        # Process quality cornerstones (test/doc coverage)
        if 'test_doc_pct' in quality_data:
            quality_scores = quality_data['test_doc_pct']
            # These are typically file-based, but we can attribute to developers
        
        # Process meaningful code metrics
        if 'meaningful_code' in quality_data:
            meaningful_metrics = quality_data['meaningful_code']
            # Extract meaningful code percentages
    
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
        
        # Convert to a 0-100 score where 100 is most consistent
        consistency_score = max(0, 100 - coefficient_of_variation)
        return round(consistency_score, 2)
    
    def get_developer_stats(self):
        """Get the aggregated developer statistics."""
        return dict(self.developer_stats)
    
    def export_for_regression(self, output_file):
        """Export developer stats in a format suitable for regression analysis."""
        regression_data = []
        
        for developer, stats in self.developer_stats.items():
            # Create a record for each week for each developer
            for week, week_stats in stats['weekly_stats'].items():
                record = {
                    'developer': developer,
                    'week': week,
                    'diff_delta': week_stats.get('diff_delta', 0),
                    'lines_added': week_stats.get('lines_added', 0),
                    'lines_updated': week_stats.get('lines_updated', 0),
                    'lines_deleted': week_stats.get('lines_deleted', 0),
                    'lines_moved': week_stats.get('lines_moved', 0),
                    'velocity_per_day': week_stats.get('velocity_per_day', 0),
                    'active_days': week_stats.get('active_days', 0),
                    'estimated_hours': week_stats.get('estimated_hours', 0),
                    'sessions': week_stats.get('sessions', 0),
                    'new_code_percent': week_stats.get('new_code_percent', 0),
                    'legacy_code_percent': week_stats.get('legacy_code_percent', 0)
                }
                
                # Add domain percentages
                domain_focus = week_stats.get('domain_focus', {})
                for domain in ['frontend', 'backend', 'test', 'docs', 'other']:
                    record[f'domain_{domain}_percent'] = domain_focus.get(domain, 0)
                
                regression_data.append(record)
        
        # Save to JSON for easy loading in analysis tools
        with open(output_file, 'w') as f:
            json.dump(regression_data, f, indent=2, default=str)
        
        logger.info(f"Exported {len(regression_data)} records for regression analysis to {output_file}")
        
        return regression_data