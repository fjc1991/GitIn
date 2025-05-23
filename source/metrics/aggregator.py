# source/metrics/aggregator.py
import traceback
import os
from tqdm import tqdm
from pydriller import Repository

# Fix import paths to use relative imports
from ..logger import get_logger
from ..memory_scheduler import check_memory_pressure, wait_for_memory_availability
from .utils import generate_weekly_ranges
# Updated imports with new folder structure
from .productivity import (
    ChangeSetMetric,
    CommitsMetric,
    ContributorsMetric,
    HunksMetric,
    LinesMetric
)
from .quality import (
    EnhancedCodeChurn,
    BugsMetric,
    CodeMovementMetric,
    QualityCornerstonesMetric,
    MeaningfulCodeMetric
)

# Import new timings metrics
from .timings import (
    DiffDeltaMetric, CodeProvenanceMetric, DeveloperHoursMetric, 
    CodeDomainMetric, DeveloperStatsAggregator, ComprehensiveTimeAnalysisMetric
)

from ..utils import get_repo_date_range

logger = get_logger(__name__)

def calculate_metrics(repo_url, repo_path, since=None, to=None, calculate_weekly=True, memory_limit=95):
    """
    Calculate process metrics using the class-based approach.
    Now includes timings metrics per developer.
    """
    if calculate_weekly and (since is None or to is None):
        logger.info("No date range provided. Determining repository date range for weekly metrics...")
        start_date, end_date = get_repo_date_range(repo_url, repo_path)
        
        if start_date and end_date:
            since = start_date
            to = end_date
            logger.debug(f"Using repository date range: {since.strftime('%Y-%m-%d')} to {to.strftime('%Y-%m-%d')}")
        else:
            logger.info("Could not determine repository date range. Falling back to overall metrics.")
            calculate_weekly = False
    
    if since and since.tzinfo:
        since = since.replace(tzinfo=None)
    if to and to.tzinfo:
        to = to.replace(tzinfo=None)

    repo_args = {'path_to_repo': repo_url}
    if since is not None:
        repo_args['since'] = since
    if to is not None:
        repo_args['to'] = to
    
    # Initialize all metric calculators, organized by category
    overall_metrics = {
        "productivity": {
            "change_set": ChangeSetMetric(),
            "commits_count": CommitsMetric(),
            "contributors": ContributorsMetric(),
            "hunks_count": HunksMetric(),
            "lines_count": LinesMetric()
        },
        "quality": {
            "code_churn": EnhancedCodeChurn(),
            "bugs": BugsMetric(),
            "code_movement": CodeMovementMetric(),
            "test_doc_pct": QualityCornerstonesMetric(),
            "meaningful_code": MeaningfulCodeMetric()
        },
        "timings": {
            "diff_delta": DiffDeltaMetric(),
            "code_provenance": CodeProvenanceMetric(),
            "developer_hours": DeveloperHoursMetric(),
            "code_domain": CodeDomainMetric(),
            "comprehensive_time_analysis": ComprehensiveTimeAnalysisMetric()
        }
    }
    
    weekly_metrics = {}
    
    if calculate_weekly:
        weekly_ranges = generate_weekly_ranges(since, to)
        for _, _, week_label in weekly_ranges:
            weekly_metrics[week_label] = {
                "productivity": {
                    "change_set": ChangeSetMetric(),
                    "commits_count": CommitsMetric(),
                    "contributors": ContributorsMetric(),
                    "hunks_count": HunksMetric(),
                    "lines_count": LinesMetric()
                },
                "quality": {
                    "code_churn": EnhancedCodeChurn(),
                    "bugs": BugsMetric(),
                    "code_movement": CodeMovementMetric(),
                    "test_doc_pct": QualityCornerstonesMetric(),
                    "meaningful_code": MeaningfulCodeMetric()
                },
                "timings": {
                    "diff_delta": DiffDeltaMetric(),
                    "code_provenance": CodeProvenanceMetric(),
                    "developer_hours": DeveloperHoursMetric(),
                    "code_domain": CodeDomainMetric(),
                    "comprehensive_time_analysis": ComprehensiveTimeAnalysisMetric()
                }
            }
    
    logger.info("Traversing repository to collect metrics...")
    
    try:
        total_commits = 0
        repository = Repository(**repo_args)
        for _ in repository.traverse_commits():
            total_commits += 1
        
        if total_commits == 0:
            logger.info("No commits found in the repository for the given time period.")
            return {}
            
        repository = Repository(**repo_args)
        
        processed_commits = 0
        with tqdm(total=total_commits, desc="Processing commits for metrics", unit="commit") as pbar:
            for commit in repository.traverse_commits():
                if processed_commits % 100 == 0 and check_memory_pressure(memory_limit):
                    logger.warning(f"Memory pressure during metrics calculation at {processed_commits}/{total_commits}, waiting...")
                    wait_for_memory_availability(memory_limit)
                
                week_label = None
                if calculate_weekly:
                    commit_date = commit.author_date
                    if commit_date.tzinfo:
                        commit_date_naive = commit_date.replace(tzinfo=None)
                    else:
                        commit_date_naive = commit_date
                    
                    for start_date, end_date, label in weekly_ranges:
                        if start_date <= commit_date_naive <= end_date:
                            week_label = label
                            break
                
                # Process this commit with all metric calculators (updated for nested structure)
                for category in overall_metrics:
                    for metric_calculator in overall_metrics[category].values():
                        metric_calculator.process_commit(commit)
                
                if calculate_weekly and week_label:
                    for category in weekly_metrics[week_label]:
                        for metric_calculator in weekly_metrics[week_label][category].values():
                            metric_calculator.process_commit(commit)
                
                processed_commits += 1
                if processed_commits % 100 == 0 or processed_commits == total_commits:
                    logger.debug(f"Processed {processed_commits}/{total_commits} commits")
                
                pbar.update(1)
                
                if processed_commits % 1000 == 0:
                    import gc
                    gc.collect()
    except Exception as e:
        logger.debug(f"Error traversing repository: {str(e)}")
        traceback.print_exc()
    
    # Collect metrics from calculators with new structure
    result = {}
    
    if not calculate_weekly:
        # Return format compatible with existing code
        for category, metrics in overall_metrics.items():
            if category == "timings":
                # Add timings metrics to a separate section
                if "developer_timings" not in result:
                    result["developer_timings"] = {}
                
                for metric_name, calculator in metrics.items():
                    result["developer_timings"][metric_name] = calculator.get_metrics()
            else:
                if category not in result:
                    result[category] = {}
                    
                for metric_name, calculator in metrics.items():
                    if metric_name == "contributors":
                        result[category]["contributors_count"] = calculator.get_metrics()
                        result[category]["contributors_experience"] = calculator.get_experience_metrics()
                    else:
                        result[category][metric_name] = calculator.get_metrics()
        return result
    else:
        weekly_results = {}
        for week_label, categories in weekly_metrics.items():
            weekly_results[week_label] = {}
            
            for category, metrics in categories.items():
                weekly_results[week_label][category] = {}
                
                for metric_name, calculator in metrics.items():
                    if metric_name == "contributors":
                        weekly_results[week_label][category]["contributors_count"] = calculator.get_metrics()
                        weekly_results[week_label][category]["contributors_experience"] = calculator.get_experience_metrics()
                    else:
                        # For timings metrics, get the metrics and integrate them properly
                        metric_data = calculator.get_metrics()
                        weekly_results[week_label][category][metric_name] = metric_data
        
        # Create aggregated developer stats from overall metrics (not weekly)
        if overall_metrics:
            # Collect all timings data for aggregation
            all_timings_data = {}
            
            for metric_name, calculator in overall_metrics["timings"].items():
                metric_data = calculator.get_metrics()
                all_timings_data[metric_name] = metric_data
            
            # Create aggregated developer stats
            if any(all_timings_data.values()):
                stats_aggregator = DeveloperStatsAggregator()
                developer_stats = stats_aggregator.aggregate_metrics({"timings": all_timings_data})
                weekly_results["developer_stats"] = developer_stats
        
        return weekly_results

def merge_metrics_results(all_chunk_results):
    merged_metrics = {}
    metrics_by_week = {}
    
    for chunk_result in all_chunk_results:
        if 'metrics' not in chunk_result or not chunk_result['metrics'] or 'error' in chunk_result.get('metrics', {}):
            continue
        
        # Process weekly metrics including timings
        if isinstance(chunk_result['metrics'], dict):
            for week, week_metrics in chunk_result['metrics'].items():
                if week == 'developer_stats':
                    continue  # Skip this special key for now
                    
                if week not in metrics_by_week:
                    metrics_by_week[week] = {}
                
                # Handle all categories including timings
                for category, metrics in week_metrics.items():
                    if category not in metrics_by_week[week]:
                        metrics_by_week[week][category] = {}
                    
                    # Collect metrics by type for each week and category
                    for metric_type, metric_value in metrics.items():
                        if metric_type not in metrics_by_week[week][category]:
                            metrics_by_week[week][category][metric_type] = []
                        
                        metrics_by_week[week][category][metric_type].append(metric_value)
    
    # Merge metrics for each week and category
    for week, categories in metrics_by_week.items():
        merged_metrics[week] = {}
        
        for category, metrics in categories.items():
            merged_metrics[week][category] = {}
            
            for metric_type, metrics_list in metrics.items():
                # Map metric types to their classes
                metric_class_map = {
                    "change_set": ChangeSetMetric,
                    "code_churn": EnhancedCodeChurn,
                    "commits_count": CommitsMetric,
                    "contributors_count": ContributorsMetric,
                    "contributors_experience": ContributorsMetric,
                    "hunks_count": HunksMetric,
                    "lines_count": LinesMetric,
                    "bugs": BugsMetric,
                    "code_movement": CodeMovementMetric,
                    "test_doc_pct": QualityCornerstonesMetric,
                    "meaningful_code": MeaningfulCodeMetric,
                    # Add timings metric mappings
                    "diff_delta": DiffDeltaMetric,
                    "code_provenance": CodeProvenanceMetric,
                    "developer_hours": DeveloperHoursMetric,
                    "code_domain": CodeDomainMetric,
                    "comprehensive_time_analysis": ComprehensiveTimeAnalysisMetric
                }
                
                if metric_type in metric_class_map:
                    try:
                        if metric_type == "contributors_experience":
                            # Special case for contributors experience
                            merged_metrics[week][category][metric_type] = ContributorsMetric.merge_experience_metrics(metrics_list)
                        else:
                            merged_metrics[week][category][metric_type] = metric_class_map[metric_type].merge_metrics(metrics_list)
                    except Exception as e:
                        logger.error(f"Error merging metric type {metric_type} for week {week}, category {category}: {str(e)}")
                        logger.debug(traceback.format_exc())
                        # Store empty/default for this metric to avoid crashing
                        merged_metrics[week][category][metric_type] = metric_class_map[metric_type].merge_metrics([])
    
    # Create aggregated developer stats if we have timings metrics
    all_timings_data = {
        "diff_delta": {},
        "code_provenance": {},
        "developer_hours": {},
        "code_domain": {}
    }
    
    for week, categories in merged_metrics.items():
        if "timings" in categories:
            for metric_name, metric_data in categories["timings"].items():
                if metric_name in all_timings_data:
                    # Merge this week's data into the overall timings data
                    for dev, dev_data in metric_data.items():
                        if dev not in all_timings_data[metric_name]:
                            all_timings_data[metric_name][dev] = {}
                        all_timings_data[metric_name][dev].update(dev_data)
    
    # Aggregate developer stats if timings metrics exist
    if any(all_timings_data.values()):
        stats_aggregator = DeveloperStatsAggregator()
        merged_metrics["developer_stats"] = stats_aggregator.aggregate_metrics({"timings": all_timings_data})
    
    return merged_metrics