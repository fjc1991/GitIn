import traceback
import concurrent.futures
import os
from tqdm import tqdm
from pydriller import Repository

from logger import get_logger
from .memory import get_memory_usage, check_memory_pressure, wait_for_memory_availability
from .utils import generate_weekly_ranges, MetricsAccumulator
from .types import (
    calculate_change_set_metrics,
    calculate_code_churn_metrics,
    calculate_commits_metrics,
    calculate_contributors_metrics,
    calculate_contributors_experience,
    calculate_hunks_metrics,
    calculate_lines_metrics,
    
    merge_change_set_metrics,
    merge_code_churn_metrics,
    merge_commits_metrics,
    merge_contributors_metrics,
    merge_contributors_experience,
    merge_hunks_metrics,
    merge_lines_metrics
)

from utils import get_repo_date_range

logger = get_logger(__name__)

def calculate_process_metrics_optimized(repo_url, repo_path, since=None, to=None, calculate_weekly=True, use_parallel=True, max_workers=4):
    # Handle case when dates are not provided but weekly metrics are requested
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
    
    # Initialize repository
    repo_args = {'path_to_repo': repo_url}
    if since is not None:
        repo_args['since'] = since
    if to is not None:
        repo_args['to'] = to
    
    commit_count = 0
    logger.info("Counting commits...")
    try:
        for _ in Repository(**repo_args).traverse_commits():
            commit_count += 1
    except Exception as e:
        logger.debug(f"Error counting commits: {str(e)}")
        return {}
    
    if commit_count == 0:
        logger.info("No commits found in the repository for the given time period.")
        return {}
    
    logger.debug(f"Found {commit_count} commits")
    
    # Initialize data structures for metrics
    overall_accumulator = MetricsAccumulator()
    weekly_data = {}
    
    if calculate_weekly:
        weekly_ranges = generate_weekly_ranges(since, to)
        for _, _, week_label in weekly_ranges:
            weekly_data[week_label] = MetricsAccumulator()
    
    logger.info("Traversing repository to collect metrics...")
    
    processed_commits = 0
    try:
        repository = Repository(**repo_args)
        batch_size = 1000
        current_batch = 0
        
        with tqdm(total=commit_count, desc="Processing commits for metrics", unit="commit") as pbar:
            for commit in repository.traverse_commits():
                week_label = None
                if calculate_weekly:
                    commit_date = commit.author_date
                    for start_date, end_date, label in weekly_ranges:
                        if start_date <= commit_date <= end_date:
                            week_label = label
                            break
                
                author_name = commit.author.name
                
                # Process each modified file
                for modified_file in commit.modified_files:
                    filename = modified_file.filename
                    overall_accumulator.update_from_modified_file(filename, modified_file, author_name)
                    
                    if calculate_weekly and week_label:
                        weekly_data[week_label].update_from_modified_file(filename, modified_file, author_name)
                
                processed_commits += 1
                
                if processed_commits % 100 == 0:
                    if get_memory_usage() > 85:
                        logger.warning(f"High memory usage ({get_memory_usage()}%) during metrics calculation. Running garbage collection.")
                        import gc
                        gc.collect()
                        
                if processed_commits % 100 == 0 or processed_commits == commit_count:
                    logger.debug(f"Processed {processed_commits}/{commit_count} commits")

                pbar.update(1)
                
                current_batch += 1
                if current_batch >= batch_size:
                    import gc
                    gc.collect()
                    current_batch = 0
                
    except Exception as e:
        logger.debug(f"Error traversing repository: {str(e)}")
        traceback.print_exc()
        if not overall_accumulator.file_changes:
            return {}
    
    # Convert collected data to metrics format
    if not calculate_weekly:
        overall_data = overall_accumulator.get_metrics_data()
        
        return {
            "change_set": calculate_change_set_metrics(overall_data['file_changes']),
            "code_churn": calculate_code_churn_metrics(
                overall_data['code_churn'], 
                overall_data['lines_added_by_file'], 
                overall_data['lines_removed_by_file']
            ),
            "commits_count": calculate_commits_metrics(overall_data['commits_by_file']),
            "contributors_count": calculate_contributors_metrics(
                overall_data['contributors_by_file'],
                overall_data['contributors_commit_count'],
                overall_data['commits_by_file']
            ),
            "contributors_experience": calculate_contributors_experience(overall_data['contributors_by_file']),
            "hunks_count": calculate_hunks_metrics(overall_data['hunks_by_file']),
            "lines_count": calculate_lines_metrics(
                overall_data['lines_added_by_file'],
                overall_data['lines_removed_by_file']
            )
        }
    else:
        # Calculate weekly metrics
        weekly_metrics = {}
        week_count = len(weekly_data)
        processed_weeks = 0
        
        for week_label, accumulator in weekly_data.items():
            try:
                week_data = accumulator.get_metrics_data()
                
                weekly_metrics[week_label] = {
                    "change_set": calculate_change_set_metrics(week_data['file_changes']),
                    "code_churn": calculate_code_churn_metrics(
                        week_data['code_churn'], 
                        week_data['lines_added_by_file'], 
                        week_data['lines_removed_by_file']
                    ),
                    "commits_count": calculate_commits_metrics(week_data['commits_by_file']),
                    "contributors_count": calculate_contributors_metrics(
                        week_data['contributors_by_file'],
                        week_data['contributors_commit_count'],
                        week_data['commits_by_file']
                    ),
                    "contributors_experience": calculate_contributors_experience(week_data['contributors_by_file']),
                    "hunks_count": calculate_hunks_metrics(week_data['hunks_by_file']),
                    "lines_count": calculate_lines_metrics(
                        week_data['lines_added_by_file'],
                        week_data['lines_removed_by_file']
                    )
                }
                
                processed_weeks += 1
                
                if processed_weeks % 10 == 0 or processed_weeks == week_count:
                    logger.debug(f"Calculated metrics for {processed_weeks}/{week_count} weeks")
                    import gc
                    gc.collect()
                    
            except Exception as e:
                logger.debug(f"Error calculating metrics for week {week_label}: {str(e)}")
                traceback.print_exc()
        
        return weekly_metrics

def calculate_process_metrics_stream(repo_url, repo_path, since=None, to=None, calculate_weekly=True, use_parallel=True, max_workers=4, memory_limit=85):
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
    
    weekly_ranges = []
    if calculate_weekly:
        weekly_ranges = generate_weekly_ranges(since, to)
    
    overall_accumulator = MetricsAccumulator()
    weekly_accumulators = {week_label: MetricsAccumulator() for _, _, week_label in weekly_ranges}
    
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
                
                author_name = commit.author.name
                
                # Process each modified file
                for modified_file in commit.modified_files:
                    filename = modified_file.filename
                    overall_accumulator.update_from_modified_file(filename, modified_file, author_name)
                    if calculate_weekly and week_label:
                        weekly_accumulators[week_label].update_from_modified_file(filename, modified_file, author_name)
                
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
        if not overall_accumulator.file_changes:
            return {}
    
    # Convert collected data to metrics format
    if not calculate_weekly:
        return _convert_accumulator_to_metrics(overall_accumulator)
    else:
        weekly_metrics = {}
        logger.debug(f"Calculating metrics for {len(weekly_accumulators)} weeks...")

        for week_label, accumulator in weekly_accumulators.items():
            weekly_metrics[week_label] = _convert_accumulator_to_metrics(accumulator)
        
        return weekly_metrics

def _convert_accumulator_to_metrics(accumulator):
    data = accumulator.get_metrics_data()
    
    return {
        "change_set": calculate_change_set_metrics(data['file_changes']),
        "code_churn": calculate_code_churn_metrics(
            data['code_churn'], 
            data['lines_added_by_file'], 
            data['lines_removed_by_file']
        ),
        "commits_count": calculate_commits_metrics(data['commits_by_file']),
        "contributors_count": calculate_contributors_metrics(
            data['contributors_by_file'],
            data['contributors_commit_count'],
            data['commits_by_file']
        ),
        "contributors_experience": calculate_contributors_experience(data['contributors_by_file']),
        "hunks_count": calculate_hunks_metrics(data['hunks_by_file']),
        "lines_count": calculate_lines_metrics(
            data['lines_added_by_file'],
            data['lines_removed_by_file']
        )
    }

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
            if metric_type == "change_set":
                merged_metrics[week][metric_type] = merge_change_set_metrics(metrics_list)
            elif metric_type == "code_churn":
                merged_metrics[week][metric_type] = merge_code_churn_metrics(metrics_list)
            elif metric_type == "commits_count":
                merged_metrics[week][metric_type] = merge_commits_metrics(metrics_list)
            elif metric_type == "contributors_count":
                merged_metrics[week][metric_type] = merge_contributors_metrics(metrics_list)
            elif metric_type == "contributors_experience":
                merged_metrics[week][metric_type] = merge_contributors_experience(metrics_list)
            elif metric_type == "hunks_count":
                merged_metrics[week][metric_type] = merge_hunks_metrics(metrics_list)
            elif metric_type == "lines_count":
                merged_metrics[week][metric_type] = merge_lines_metrics(metrics_list)
    
    return merged_metrics