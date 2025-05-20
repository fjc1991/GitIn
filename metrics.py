import traceback
import concurrent.futures
import gc
import psutil
from tqdm import tqdm
from pydriller import Repository

from utils import get_repo_date_range, generate_weekly_ranges

# Set up logger for this module
from logger import get_logger
logger = get_logger(__name__)

def get_memory_usage():
    """Get current memory usage as percentage."""
    return psutil.virtual_memory().percent

def check_memory_pressure(threshold=85):
    """Check if memory usage is above threshold and perform garbage collection if needed."""
    memory_percent = get_memory_usage()
    if memory_percent > threshold:
        logger.warning(f"Memory pressure detected during metrics calculation: {memory_percent}% used")
        # Force garbage collection
        gc.collect()
        return True
    return False

def calculate_process_metrics_optimized(repo_url, repo_path, since=None, to=None, calculate_weekly=True, use_parallel=True, max_workers=4):
    """
    Optimized version of calculate_process_metrics that traverses the repository only once.
    Modified to have cleaner console output.
    
    Args:
        repo_url (str): URL of the repository
        repo_path (str): Local path to the repository
        since (datetime, optional): Start date for analysis. If None, analyze entire history.
        to (datetime, optional): End date for analysis. If None, analyze entire history.
        calculate_weekly (bool): Whether to calculate metrics on a weekly basis
        use_parallel (bool): Whether to use parallel processing for weekly metrics
        max_workers (int): Maximum number of parallel workers
    
    Returns:
        Dictionary containing metrics data. If calculate_weekly is True, returns a dictionary
        of weekly metrics. Otherwise, returns a dictionary of overall metrics.
    """
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
    
    # First, count commits for progress bar
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
    file_changes = {}  # For change set metrics
    code_churn = {}    # For code churn metrics
    commits_by_file = {}  # For commits count
    contributors_by_file = {}  # For contributors count
    contributors_commit_count = {}  # To track commit counts per author per file
    hunks_by_file = {}  # For hunks count
    lines_added_by_file = {}  # For lines count
    lines_removed_by_file = {}  # For lines count
    
    # For weekly metrics
    weekly_data = {}
    
    # Generate weekly ranges if needed
    if calculate_weekly:
        weekly_ranges = generate_weekly_ranges(since, to)
        for _, _, week_label in weekly_ranges:
            weekly_data[week_label] = {
                'file_changes': {},
                'code_churn': {},
                'commits_by_file': {},
                'contributors_by_file': {},
                'contributors_commit_count': {},
                'hunks_by_file': {},
                'lines_added_by_file': {},
                'lines_removed_by_file': {}
            }
    
    # Traverse repository and collect metrics data
    logger.info("Traversing repository to collect metrics...")
    
    processed_commits = 0
    try:
        repository = Repository(**repo_args)
        batch_size = 1000  # Process in batches to manage memory
        current_batch = 0
        
        with tqdm(total=commit_count, desc="Processing commits for metrics", unit="commit") as pbar:
            for commit in repository.traverse_commits():
                # Determine week if calculating weekly metrics
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
                    
                    # Update metrics for overall data
                    if filename not in file_changes:
                        file_changes[filename] = 0
                    file_changes[filename] += 1
                    
                    if filename not in code_churn:
                        code_churn[filename] = 0
                    file_churn = modified_file.added_lines + modified_file.deleted_lines
                    code_churn[filename] += file_churn
                    
                    if filename not in commits_by_file:
                        commits_by_file[filename] = 0
                    commits_by_file[filename] += 1
                    
                    if filename not in contributors_by_file:
                        contributors_by_file[filename] = set()
                    contributors_by_file[filename].add(author_name)
                    
                    # Track commit counts per author per file for minor contributors calculation
                    if filename not in contributors_commit_count:
                        contributors_commit_count[filename] = {}
                    if author_name not in contributors_commit_count[filename]:
                        contributors_commit_count[filename][author_name] = 0
                    contributors_commit_count[filename][author_name] += 1
                    
                    if filename not in hunks_by_file:
                        hunks_by_file[filename] = 0
                    # Count hunks (groups of changes) in the diff
                    if hasattr(modified_file, 'diff_parsed') and modified_file.diff_parsed:
                        hunks_by_file[filename] += len(modified_file.diff_parsed.get('added', [])) + len(modified_file.diff_parsed.get('deleted', []))
                    
                    if filename not in lines_added_by_file:
                        lines_added_by_file[filename] = 0
                    lines_added_by_file[filename] += modified_file.added_lines
                    
                    if filename not in lines_removed_by_file:
                        lines_removed_by_file[filename] = 0
                    lines_removed_by_file[filename] += modified_file.deleted_lines
                    
                    # If calculating weekly metrics, update weekly data too
                    if calculate_weekly and week_label:
                        week_data = weekly_data[week_label]
                        
                        if filename not in week_data['file_changes']:
                            week_data['file_changes'][filename] = 0
                        week_data['file_changes'][filename] += 1
                        
                        if filename not in week_data['code_churn']:
                            week_data['code_churn'][filename] = 0
                        week_data['code_churn'][filename] += file_churn
                        
                        if filename not in week_data['commits_by_file']:
                            week_data['commits_by_file'][filename] = 0
                        week_data['commits_by_file'][filename] += 1
                        
                        if filename not in week_data['contributors_by_file']:
                            week_data['contributors_by_file'][filename] = set()
                        week_data['contributors_by_file'][filename].add(author_name)
                        
                        # Track commit counts per author per file for weekly minor contributors
                        if filename not in week_data['contributors_commit_count']:
                            week_data['contributors_commit_count'][filename] = {}
                        if author_name not in week_data['contributors_commit_count'][filename]:
                            week_data['contributors_commit_count'][filename][author_name] = 0
                        week_data['contributors_commit_count'][filename][author_name] += 1
                        
                        if filename not in week_data['hunks_by_file']:
                            week_data['hunks_by_file'][filename] = 0
                        if hasattr(modified_file, 'diff_parsed') and modified_file.diff_parsed:
                            week_data['hunks_by_file'][filename] += len(modified_file.diff_parsed.get('added', [])) + len(modified_file.diff_parsed.get('deleted', []))
                        
                        if filename not in week_data['lines_added_by_file']:
                            week_data['lines_added_by_file'][filename] = 0
                        week_data['lines_added_by_file'][filename] += modified_file.added_lines
                        
                        if filename not in week_data['lines_removed_by_file']:
                            week_data['lines_removed_by_file'][filename] = 0
                        week_data['lines_removed_by_file'][filename] += modified_file.deleted_lines
                
                processed_commits += 1
                
                # Check memory every 100 commits
                if processed_commits % 100 == 0:
                    if get_memory_usage() > 85:
                        logger.warning(f"High memory usage ({get_memory_usage()}%) during metrics calculation. Running garbage collection.")
                        gc.collect()
                        
                # Report progress
                if processed_commits % 100 == 0 or processed_commits == commit_count:
                    logger.debug(f"Processed {processed_commits}/{commit_count} commits")
                
                pbar.update(1)
                
                # Process in batches to manage memory
                current_batch += 1
                if current_batch >= batch_size:
                    gc.collect()
                    current_batch = 0
                
    except Exception as e:
        logger.debug(f"Error traversing repository: {str(e)}")
        traceback.print_exc()
        # Return partial results if we have any
        if not file_changes:
            return {}
    
    # Convert collected data to metrics format
    if not calculate_weekly:
        # Calculate overall metrics
        metrics_result = {}
        
        # Change Set metrics
        change_set_values = list(file_changes.values())
        metrics_result["change_set"] = {
            "max": max(change_set_values) if change_set_values else 0,
            "avg": sum(change_set_values) / len(change_set_values) if change_set_values else 0
        }
        
        # Code Churn metrics
        churn_values = list(code_churn.values())
        metrics_result["code_churn"] = {
            "count": sum(churn_values),
            "max": max(churn_values) if churn_values else 0,
            "avg": sum(churn_values) / len(churn_values) if churn_values else 0,
            "added_removed": {
                "added": sum(lines_added_by_file.values()),
                "removed": sum(lines_removed_by_file.values())
            }
        }
        
        # Commits Count metrics
        metrics_result["commits_count"] = commits_by_file
        
        # Contributors Count metrics
        contributors_count = {filename: len(authors) for filename, authors in contributors_by_file.items()}
        
        # Calculate minor contributors (using 20% threshold as in original code)
        minor_contributors = {}
        for filename, author_commits in contributors_commit_count.items():
            if filename in commits_by_file and commits_by_file[filename] > 0:
                total_commits = commits_by_file[filename]
                threshold = max(1, total_commits * 0.2)  # 20% threshold
                minor_count = sum(1 for commits in author_commits.values() if commits < threshold)
                minor_contributors[filename] = minor_count
        
        metrics_result["contributors_count"] = {
            "total": contributors_count,
            "minor": minor_contributors
        }
        
        # Contributors Experience metrics
        # This is a simplified approximation that matches the original implementation
        metrics_result["contributors_experience"] = {filename: len(authors) for filename, authors in contributors_by_file.items()}
        
        # Hunks Count metrics
        metrics_result["hunks_count"] = hunks_by_file
        
        # Lines Count metrics
        metrics_result["lines_count"] = {
            "added": {
                "total": sum(lines_added_by_file.values()),
                "max": max(lines_added_by_file.values()) if lines_added_by_file else 0,
                "avg": sum(lines_added_by_file.values()) / len(lines_added_by_file) if lines_added_by_file else 0
            },
            "removed": {
                "total": sum(lines_removed_by_file.values()),
                "max": max(lines_removed_by_file.values()) if lines_removed_by_file else 0,
                "avg": sum(lines_removed_by_file.values()) / len(lines_removed_by_file) if lines_removed_by_file else 0
            }
        }
        
        return metrics_result
    else:
        # Calculate weekly metrics
        weekly_metrics = {}
        
        # Function to calculate metrics for a week
        def calculate_week_metrics(week_label, week_data):
            week_metrics = {}
            
            # Change Set metrics
            change_set_values = list(week_data['file_changes'].values())
            week_metrics["change_set"] = {
                "max": max(change_set_values) if change_set_values else 0,
                "avg": sum(change_set_values) / len(change_set_values) if change_set_values else 0
            }
            
            # Code Churn metrics
            churn_values = list(week_data['code_churn'].values())
            week_metrics["code_churn"] = {
                "count": sum(churn_values),
                "max": max(churn_values) if churn_values else 0,
                "avg": sum(churn_values) / len(churn_values) if churn_values else 0,
                "added_removed": {
                    "added": sum(week_data['lines_added_by_file'].values()),
                    "removed": sum(week_data['lines_removed_by_file'].values())
                }
            }
            
            # Commits Count metrics
            week_metrics["commits_count"] = week_data['commits_by_file']
            
            # Contributors Count metrics
            contributors_count = {filename: len(authors) for filename, authors in week_data['contributors_by_file'].items()}
            
            # Calculate minor contributors for this week
            minor_contributors = {}
            for filename, author_commits in week_data['contributors_commit_count'].items():
                if filename in week_data['commits_by_file'] and week_data['commits_by_file'][filename] > 0:
                    total_commits = week_data['commits_by_file'][filename]
                    threshold = max(1, total_commits * 0.2)  # 20% threshold
                    minor_count = sum(1 for commits in author_commits.values() if commits < threshold)
                    minor_contributors[filename] = minor_count
            
            week_metrics["contributors_count"] = {
                "total": contributors_count,
                "minor": minor_contributors
            }
            
            # Contributors Experience metrics (approximated)
            week_metrics["contributors_experience"] = {filename: len(authors) for filename, authors in week_data['contributors_by_file'].items()}
            
            # Hunks Count metrics
            week_metrics["hunks_count"] = week_data['hunks_by_file']
            
            # Lines Count metrics
            lines_added = week_data['lines_added_by_file']
            lines_removed = week_data['lines_removed_by_file']
            
            week_metrics["lines_count"] = {
                "added": {
                    "total": sum(lines_added.values()),
                    "max": max(lines_added.values()) if lines_added else 0,
                    "avg": sum(lines_added.values()) / len(lines_added) if lines_added else 0
                },
                "removed": {
                    "total": sum(lines_removed.values()),
                    "max": max(lines_removed.values()) if lines_removed else 0,
                    "avg": sum(lines_removed.values()) / len(lines_removed) if lines_removed else 0
                }
            }
            
            return week_metrics
        
        # Process each week with periodic garbage collection
        week_count = len(weekly_data)
        processed_weeks = 0
        
        for week_label, week_data in weekly_data.items():
            try:
                weekly_metrics[week_label] = calculate_week_metrics(week_label, week_data)
                processed_weeks += 1
                
                if processed_weeks % 10 == 0 or processed_weeks == week_count:
                    logger.debug(f"Calculated metrics for {processed_weeks}/{week_count} weeks")
                    # Free up memory
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
    
    class MetricsAccumulator:
        def __init__(self):
            self.file_changes = {}
            self.code_churn = {}
            self.commits_by_file = {}
            self.contributors_by_file = {}
            self.contributors_commit_count = {}
            self.hunks_by_file = {}
            self.lines_added_by_file = {}
            self.lines_removed_by_file = {}
            
        def update_from_modified_file(self, filename, modified_file, author_name):
            if filename not in self.file_changes:
                self.file_changes[filename] = 0
            self.file_changes[filename] += 1
            
            if filename not in self.code_churn:
                self.code_churn[filename] = 0
            file_churn = modified_file.added_lines + modified_file.deleted_lines
            self.code_churn[filename] += file_churn
            
            if filename not in self.commits_by_file:
                self.commits_by_file[filename] = 0
            self.commits_by_file[filename] += 1
            
            if filename not in self.contributors_by_file:
                self.contributors_by_file[filename] = set()
            self.contributors_by_file[filename].add(author_name)
            
            if filename not in self.contributors_commit_count:
                self.contributors_commit_count[filename] = {}
            if author_name not in self.contributors_commit_count[filename]:
                self.contributors_commit_count[filename][author_name] = 0
            self.contributors_commit_count[filename][author_name] += 1
            
            if filename not in self.hunks_by_file:
                self.hunks_by_file[filename] = 0
            if hasattr(modified_file, 'diff_parsed') and modified_file.diff_parsed:
                self.hunks_by_file[filename] += len(modified_file.diff_parsed.get('added', [])) + len(modified_file.diff_parsed.get('deleted', []))
            
            if filename not in self.lines_added_by_file:
                self.lines_added_by_file[filename] = 0
            self.lines_added_by_file[filename] += modified_file.added_lines

            if filename not in self.lines_removed_by_file:
                self.lines_removed_by_file[filename] = 0
            self.lines_removed_by_file[filename] += modified_file.deleted_lines
    
    # Create accumulators
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

                    while get_memory_usage() > memory_limit - 5:
                        import time
                        time.sleep(2)
                        gc.collect()
                
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
    metrics_result = {}
    
    # Change Set metrics
    change_set_values = list(accumulator.file_changes.values())
    metrics_result["change_set"] = {
        "max": max(change_set_values) if change_set_values else 0,
        "avg": sum(change_set_values) / len(change_set_values) if change_set_values else 0
    }
    
    # Code Churn metrics
    churn_values = list(accumulator.code_churn.values())
    metrics_result["code_churn"] = {
        "count": sum(churn_values),
        "max": max(churn_values) if churn_values else 0,
        "avg": sum(churn_values) / len(churn_values) if churn_values else 0,
        "added_removed": {
            "added": sum(accumulator.lines_added_by_file.values()),
            "removed": sum(accumulator.lines_removed_by_file.values())
        }
    }
    
    # Commits Count metrics
    metrics_result["commits_count"] = accumulator.commits_by_file
    
    # Contributors Count metrics
    contributors_count = {filename: len(authors) for filename, authors in accumulator.contributors_by_file.items()}
    
    # Calculate minor contributors (using 20% threshold as in original code)
    minor_contributors = {}
    for filename, author_commits in accumulator.contributors_commit_count.items():
        if filename in accumulator.commits_by_file and accumulator.commits_by_file[filename] > 0:
            total_commits = accumulator.commits_by_file[filename]
            threshold = max(1, total_commits * 0.2)  # 20% threshold
            minor_count = sum(1 for commits in author_commits.values() if commits < threshold)
            minor_contributors[filename] = minor_count
    
    metrics_result["contributors_count"] = {
        "total": contributors_count,
        "minor": minor_contributors
    }
    
    # Contributors Experience metrics (simplified approximation)
    metrics_result["contributors_experience"] = {filename: len(authors) for filename, authors in accumulator.contributors_by_file.items()}
    
    # Hunks Count metrics
    metrics_result["hunks_count"] = accumulator.hunks_by_file
    
    # Lines Count metrics
    metrics_result["lines_count"] = {
        "added": {
            "total": sum(accumulator.lines_added_by_file.values()),
            "max": max(accumulator.lines_added_by_file.values()) if accumulator.lines_added_by_file else 0,
            "avg": sum(accumulator.lines_added_by_file.values()) / len(accumulator.lines_added_by_file) if accumulator.lines_added_by_file else 0
        },
        "removed": {
            "total": sum(accumulator.lines_removed_by_file.values()),
            "max": max(accumulator.lines_removed_by_file.values()) if accumulator.lines_removed_by_file else 0,
            "avg": sum(accumulator.lines_removed_by_file.values()) / len(accumulator.lines_removed_by_file) if accumulator.lines_removed_by_file else 0
        }
    }
    
    return metrics_result

def merge_metrics_results(all_chunk_results):
    """
    Merge metrics data from multiple processing chunks.
    """
    # Initialize with first non-empty metrics
    merged_metrics = {}
    
    # First pass: find structure from a non-error result
    for chunk_result in all_chunk_results:
        if 'metrics' in chunk_result and chunk_result['metrics'] and 'error' not in chunk_result['metrics']:
            merged_metrics = chunk_result['metrics'].copy()
            break
    
    # If we have weekly metrics, merge them
    for chunk_result in all_chunk_results:
        if 'metrics' not in chunk_result or not chunk_result['metrics']:
            continue
            
        # Weekly metrics structure
        if isinstance(chunk_result['metrics'], dict) and list(chunk_result['metrics'].keys())[0].startswith('Week_'):
            for week, week_data in chunk_result['metrics'].items():
                # Add week if not exists
                if week not in merged_metrics:
                    merged_metrics[week] = week_data
                else:
                    # Merge metrics within week
                    for metric_name, metric_value in week_data.items():
                        if metric_name not in merged_metrics[week]:
                            merged_metrics[week][metric_name] = metric_value
                        else:
                            # Handle different metric structures
                            if isinstance(metric_value, dict) and 'count' in metric_value:
                                # Metric with count
                                if 'count' in merged_metrics[week][metric_name]:
                                    merged_metrics[week][metric_name]['count'] += metric_value['count']
                                    
                                # Handle max values
                                if 'max' in metric_value and 'max' in merged_metrics[week][metric_name]:
                                    merged_metrics[week][metric_name]['max'] = max(
                                        merged_metrics[week][metric_name]['max'], metric_value['max']
                                    )
                            elif isinstance(metric_value, dict) and 'added' in metric_value:
                                # Lines count with added/removed structure
                                for category in ['added', 'removed']:
                                    if category in metric_value:
                                        if 'total' in metric_value[category]:
                                            merged_metrics[week][metric_name][category]['total'] += metric_value[category]['total']
                                        if 'max' in metric_value[category]:
                                            merged_metrics[week][metric_name][category]['max'] = max(
                                                merged_metrics[week][metric_name][category]['max'],
                                                metric_value[category]['max']
                                            )
                            elif isinstance(metric_value, dict) and any(isinstance(v, int) for v in metric_value.values()):
                                # Dictionary of counts (like commits_count)
                                for key, val in metric_value.items():
                                    if key in merged_metrics[week][metric_name]:
                                        if isinstance(val, (int, float)):
                                            merged_metrics[week][metric_name][key] += val
    
    return merged_metrics