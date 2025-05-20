import traceback
import os
from tqdm import tqdm
from pydriller import Repository

# Fix import paths to use relative imports
from ..logger import get_logger
from ..memory_scheduler import check_memory_pressure, wait_for_memory_availability
from .utils import generate_weekly_ranges
from .types import (
    ChangeSetMetric,
    EnhancedCodeChurn,
    CommitsMetric,
    ContributorsMetric,
    HunksMetric,
    LinesMetric
)

from ..utils import get_repo_date_range

logger = get_logger(__name__)

def calculate_metrics(repo_url, repo_path, since=None, to=None, calculate_weekly=True, memory_limit=85):
    """
    Calculate process metrics using the class-based approach.
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
    
    # Initialize all metric calculators
    overall_metrics = {
        "change_set": ChangeSetMetric(),
        "code_churn": EnhancedCodeChurn(),
        "commits_count": CommitsMetric(),
        "contributors": ContributorsMetric(),
        "hunks_count": HunksMetric(),
        "lines_count": LinesMetric()
    }
    
    weekly_metrics = {}
    if calculate_weekly:
        weekly_ranges = generate_weekly_ranges(since, to)
        for _, _, week_label in weekly_ranges:
            weekly_metrics[week_label] = {
                "change_set": ChangeSetMetric(),
                "code_churn": EnhancedCodeChurn(),
                "commits_count": CommitsMetric(),
                "contributors": ContributorsMetric(),
                "hunks_count": HunksMetric(),
                "lines_count": LinesMetric()
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
                
                # Process this commit with all metric calculators
                for metric_calculator in overall_metrics.values():
                    metric_calculator.process_commit(commit)
                
                if calculate_weekly and week_label:
                    for metric_calculator in weekly_metrics[week_label].values():
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
    
    # Collect metrics from calculators
    result = {}
    if not calculate_weekly:
        for metric_name, calculator in overall_metrics.items():
            if metric_name == "contributors":
                result["contributors_count"] = calculator.get_metrics()
                result["contributors_experience"] = calculator.get_experience_metrics()
            else:
                result[metric_name] = calculator.get_metrics()
        return result
    else:
        weekly_results = {}
        for week_label, calculators in weekly_metrics.items():
            weekly_results[week_label] = {}
            for metric_name, calculator in calculators.items():
                if metric_name == "contributors":
                    weekly_results[week_label]["contributors_count"] = calculator.get_metrics()
                    weekly_results[week_label]["contributors_experience"] = calculator.get_experience_metrics()
                else:
                    weekly_results[week_label][metric_name] = calculator.get_metrics()
        return weekly_results

def merge_metrics_results(all_chunk_results):
    merged_metrics = {}
    metrics_by_week = {}
    
    for chunk_result in all_chunk_results:
        if 'metrics' not in chunk_result or not chunk_result['metrics'] or 'error' in chunk_result.get('metrics', {}):
            continue
            
        # Process weekly metrics
        if isinstance(chunk_result['metrics'], dict):
            for week, week_metrics in chunk_result['metrics'].items():
                if week not in metrics_by_week:
                    metrics_by_week[week] = {}
                
                # Collect metrics by type for each week
                for metric_type, metric_value in week_metrics.items():
                    if metric_type not in metrics_by_week[week]:
                        metrics_by_week[week][metric_type] = []
                    
                    metrics_by_week[week][metric_type].append(metric_value)
    
    for week, week_data in metrics_by_week.items():
        merged_metrics[week] = {}
        
        for metric_type, metrics_list in week_data.items():
            metric_class_map = {
                "change_set": ChangeSetMetric,
                "code_churn": EnhancedCodeChurn,
                "commits_count": CommitsMetric,
                "contributors_count": ContributorsMetric,
                "contributors_experience": ContributorsMetric,
                "hunks_count": HunksMetric,
                "lines_count": LinesMetric
            }
            
            if metric_type in metric_class_map:
                if metric_type == "contributors_experience":
                    # Special case for contributors experience
                    merged_metrics[week][metric_type] = ContributorsMetric.merge_experience_metrics(metrics_list)
                else:
                    merged_metrics[week][metric_type] = metric_class_map[metric_type].merge_metrics(metrics_list)
    
    return merged_metrics