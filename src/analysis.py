import os
import traceback
import concurrent.futures
import multiprocessing
import signal
from datetime import datetime, timedelta
from pydriller import Repository
from tqdm import tqdm
import time
import threading
import gc
import psutil
import subprocess
import json

from .logger import get_logger
from .utils import ensure_dir, extract_commit_info, MASTER_OUTPUT_DIR, MASTER_TEMP_DIR, get_repo_date_range
from .metrics import calculate_metrics, merge_metrics_results

logger = get_logger(__name__)

def check_output_exists(output_dir, pattern):
    import glob
    import os
    
    if output_dir:
        if not pattern.startswith(output_dir):
            pattern = os.path.join(output_dir, pattern)
    
    base_pattern = pattern
    for ext in ['.json', '.7z']:
        if base_pattern.endswith(ext):
            base_pattern = base_pattern[:-len(ext)]
            break
    
    json_files = glob.glob(base_pattern + ".json")
    z7_files = glob.glob(base_pattern + ".7z")
    
    if json_files:
        logger.debug(f"Found JSON files: {json_files}")
    if z7_files:
        logger.debug(f"Found 7z files: {z7_files}")
    
    return json_files + z7_files

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException("Repository processing timed out")

def get_memory_usage():
    return psutil.virtual_memory().percent

def check_memory_pressure(threshold=85):
    memory_percent = get_memory_usage()
    if memory_percent > threshold:
        logger.warning(f"Memory pressure detected: {memory_percent}% used")
        gc.collect()
        return True
    return False

def analyze_repo_timeframe_enhanced(project_name, repo_url, start_year=None, start_month=None, end_year=None, end_month=None, 
                                  ecosystem=None, repo_category=None, calculate_weekly=True, split_large_repos=True, 
                                  max_workers=None, batch_size=1000, memory_limit=85):
    if max_workers is None:
        max_workers = max(1, multiprocessing.cpu_count() - 1)
    
    start_date = None
    end_date = None
    
    if start_year is not None and start_month is not None:
        start_date = datetime(start_year, start_month, 1)
        
    if end_year is not None and end_month is not None:
        if end_month == 12:
            end_date = datetime(end_year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(end_year, end_month + 1, 1) - timedelta(days=1)
    
    if start_date and end_date:
        timeframe = f"{start_date.year}_{start_date.month}_to_{end_date.year}_{end_date.month}"
    else:
        timeframe = "full_history"
    
    if ecosystem and repo_category:
        output_dir = os.path.join(MASTER_OUTPUT_DIR, ecosystem, repo_category)
    else:
        output_dir = MASTER_OUTPUT_DIR
    
    ensure_dir(output_dir)
    
    repo_name = repo_url.split('/')[-1] if '/' in repo_url else 'unnamed_repo'
    
    temp_dir_name = f"{project_name}_{repo_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    temp_dir = os.path.join(MASTER_TEMP_DIR, temp_dir_name)
    ensure_dir(temp_dir)
    
    output_filename = f"{project_name}_{repo_name}_{timeframe}_analysis.json"
    output_path = os.path.join(output_dir, output_filename)
    
    try:
        if start_date is None or end_date is None:
            logger.debug(f"Determining date range for full history analysis of {repo_name}")
            try:
                extracted_start_date, extracted_end_date = get_repo_date_range(repo_url, temp_dir)
                if start_date is None:
                    start_date = extracted_start_date
                if end_date is None:
                    end_date = extracted_end_date
                logger.debug(f"Extracted date range: {start_date} to {end_date}")
            except Exception as e:
                logger.error(f"Failed to extract date range for {repo_name}: {str(e)}")
        
        repo_args = {
            'path_to_repo': repo_url,
            'clone_repo_to': temp_dir
        }
        
        if start_date:
            repo_args['since'] = start_date
        if end_date:
            repo_args['to'] = end_date
            
        repository = Repository(**repo_args)
        
        commit_count = 0
        logger.debug(f"Counting commits for {repo_name}...")
        try:
            for _ in repository.traverse_commits():
                commit_count += 1
        except Exception as e:
            logger.error(f"Error counting commits: {str(e)}")
            logger.debug(traceback.format_exc())
        
        if commit_count == 0:
            logger.debug(f"No commits found for {project_name}")
            return
        
        logger.debug(f"Found {commit_count} commits in {repo_name}")
        
        repository = Repository(**repo_args)

        from .repo_processing import estimate_repo_size, split_date_range, process_repo_chunk, merge_commit_results
        estimated_commits, should_split = estimate_repo_size(repo_url, temp_dir, start_date, end_date)

        with open(output_path, 'w') as f:
            f.write('{\n')
            f.write(f'  "project_name": "{project_name}",\n')
            f.write(f'  "repository_url": "{repo_url}",\n')
            f.write(f'  "repository_name": "{repo_name}",\n')
            f.write(f'  "ecosystem": "{ecosystem or ""}",\n')
            f.write(f'  "repo_category": "{repo_category or ""}",\n')
            f.write('  "analysis_period": {\n')
            f.write(f'    "start_date": "{start_date.strftime("%Y-%m-%d") if start_date else None}",\n')
            f.write(f'    "end_date": "{end_date.strftime("%Y-%m-%d") if end_date else None}",\n')
            f.write(f'    "full_history": {str(start_date is None and end_date is None).lower()}\n')
            f.write('  },\n')
            f.write('  "commits": [\n')

        if should_split and estimated_commits > 500 and split_large_repos:
            chunk_count = min(4, estimated_commits // 200)
            chunk_count = max(2, chunk_count)
            date_chunks = split_date_range(start_date, end_date, chunk_count)

            all_chunk_results = []
            total_commits = 0
            
            for i, (chunk_start, chunk_end) in enumerate(date_chunks):
                if check_memory_pressure(memory_limit):
                    logger.warning(f"Memory pressure before chunk {i+1}/{len(date_chunks)}, waiting...")
                    while get_memory_usage() > memory_limit - 5:
                        time.sleep(2)
                        gc.collect()
                
                chunk_result = process_repo_chunk(
                    repo_url, chunk_start, chunk_end, temp_dir, output_dir, 
                    batch_size=batch_size
                )
                
                if 'commit_file_path' in chunk_result and os.path.exists(chunk_result['commit_file_path']):
                    with open(chunk_result['commit_file_path'], 'r') as chunk_file, open(output_path, 'a') as out_file:
                        if i > 0 or total_commits > 0:
                            out_file.write(',\n')
                        
                        first_line = True
                        for line in chunk_file:
                            if not first_line:
                                out_file.write(',\n')
                            out_file.write('    ' + line.strip())
                            first_line = False
                            total_commits += 1
                
                all_chunk_results.append(chunk_result)
                gc.collect()
                
            process_metrics = merge_metrics_results(all_chunk_results)
            
        else:
            process_metrics = {}
            commit_batch = []
            processed_commits = 0
            
            with tqdm(total=commit_count, desc=f"Processing {project_name} Commits", unit="commit") as pbar:
                first_commit = True
                
                for commit in repository.traverse_commits():
                    try:
                        if processed_commits % 100 == 0 and check_memory_pressure(memory_limit):
                            logger.warning(f"Memory pressure during commit processing at {processed_commits}/{commit_count}, waiting...")
                            while get_memory_usage() > memory_limit - 5:
                                time.sleep(2)
                                gc.collect()
                        
                        commit_info = extract_commit_info(commit)
                        commit_batch.append(commit_info)
                        processed_commits += 1
                        
                        if len(commit_batch) >= batch_size or processed_commits == commit_count:
                            with open(output_path, 'a') as f:
                                for i, c_info in enumerate(commit_batch):
                                    if not first_commit or i > 0:
                                        f.write(',\n')
                                    f.write('    ' + json.dumps(c_info, default=str))
                                    first_commit = False
                            
                            commit_batch = []
                            gc.collect()
                        
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Error extracting commit info: {str(e)}")
                        logger.debug(traceback.format_exc())
                        pbar.update(1)
            
            try:
                process_metrics = calculate_metrics(
                    repo_url, temp_dir, start_date, end_date, calculate_weekly=calculate_weekly,
                    memory_limit=memory_limit
                )
            except Exception as e:
                logger.error(f"Could not calculate process metrics for {project_name}: {str(e)}")
                process_metrics = {"error": str(e)}
        
        with open(output_path, 'a') as f:
            f.write('\n  ],\n')
            f.write('  "process_metrics": ' + json.dumps(process_metrics, default=str, indent=2) + ',\n')
            f.write('  "metrics_type": "' + ('weekly' if calculate_weekly else 'overall') + '"\n')
            f.write('}\n')
        
        logger.debug(f"Analysis complete for {project_name} ({repo_name})! Results saved to: {output_path}")
            
    except Exception as e:
        logger.error(f"Error analyzing {project_name} repository: {str(e)}")
        logger.debug(traceback.format_exc())
    finally:
        if os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

def analyze_organization_repos_enhanced(project_name, ecosystem, repos, start_year=None, start_month=None, 
                                       end_year=None, end_month=None, use_parallel=True, max_workers=None,
                                       split_large_repos=True, batch_size=1000, memory_limit=85, output_dir_override=None):

    if max_workers is None:
        max_workers = max(1, min(multiprocessing.cpu_count() - 1, 4))
    
    start_date = None
    end_date = None
    
    if start_year is not None and start_month is not None:
        start_date = datetime(start_year, start_month, 1)
        
    if end_year is not None and end_month is not None:
        if end_month == 12:
            end_date = datetime(end_year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(end_year, end_month + 1, 1) - timedelta(days=1)
    
    if start_date and end_date:
        timeframe = f"{start_date.year}_{start_date.month}_to_{end_date.year}_{end_date.month}"
    else:
        timeframe = "full_history"
    
    if output_dir_override:
        base_output_dir = output_dir_override
        ensure_dir(base_output_dir)
        
        logger.debug(f"Using override output directory: {base_output_dir}")
        
        temp_dir_name = f"{project_name}_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        temp_dir = os.path.join(MASTER_TEMP_DIR, temp_dir_name)
        ensure_dir(temp_dir)
        
        try:
            process_repo_group(
                project_name, 
                ecosystem, 
                repos,  
                "all",  
                start_date, 
                end_date, 
                temp_dir, 
                base_output_dir,  
                use_parallel, 
                max_workers, 
                False,  
                split_large_repos, 
                batch_size, 
                memory_limit,
                timeframe
            )
        except Exception as e:
            logger.error(f"Error analyzing {project_name} repositories: {str(e)}")
            logger.debug(traceback.format_exc())
        finally:
            if os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
    else:
        repos_by_category = {}
        for repo in repos:
            category = repo['repo_category']
            if category not in repos_by_category:
                repos_by_category[category] = []
            repos_by_category[category].append(repo)
        
        for category, category_repos in repos_by_category.items():
            if not category_repos:
                continue
            
            base_output_dir = os.path.join(MASTER_OUTPUT_DIR, ecosystem, category)
            ensure_dir(base_output_dir)
            
            logger.debug(f"Processing {len(category_repos)} {category} repositories for {project_name}")
            
            temp_dir_name = f"{project_name}_{category}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            temp_dir = os.path.join(MASTER_TEMP_DIR, temp_dir_name)
            ensure_dir(temp_dir)
            
            try:
                if category == 'organization':
                    org_repos = {}
                    for repo in category_repos:
                        repo_url = repo['repo_url']
                        org_name = repo.get('org_name')
                        if not org_name:
                            from .utils import extract_org_from_url
                            org_name = extract_org_from_url(repo_url) or "unknown_org"
                        
                        if org_name not in org_repos:
                            org_repos[org_name] = []
                        
                        org_repos[org_name].append(repo)
                    
                    for org_name, org_specific_repos in org_repos.items():
                        org_output_dir = os.path.join(base_output_dir, org_name)
                        ensure_dir(org_output_dir)
                        
                        process_repo_group(
                            project_name, 
                            ecosystem, 
                            org_specific_repos, 
                            org_name,
                            start_date, 
                            end_date, 
                            temp_dir, 
                            org_output_dir, 
                            use_parallel, 
                            max_workers, 
                            True,  
                            split_large_repos, 
                            batch_size, 
                            memory_limit,
                            timeframe
                        )
                
                elif category == 'other':
                    org_repos = {}
                    for repo in category_repos:
                        repo_url = repo['repo_url']
                        from .utils import extract_org_from_url
                        org_name = extract_org_from_url(repo_url) or "unknown_org"
                        
                        if org_name not in org_repos:
                            org_repos[org_name] = []
                        
                        org_repos[org_name].append(repo)
                    
                    for org_name, org_specific_repos in org_repos.items():
                        org_output_dir = os.path.join(base_output_dir, org_name)
                        ensure_dir(org_output_dir)
                        
                        process_repo_group(
                            project_name, 
                            ecosystem, 
                            org_specific_repos, 
                            org_name,
                            start_date, 
                            end_date, 
                            temp_dir, 
                            org_output_dir, 
                            use_parallel, 
                            max_workers, 
                            True,  
                            split_large_repos, 
                            batch_size, 
                            memory_limit,
                            timeframe
                        )
                
                else:
                    process_repo_group(
                        project_name, 
                        ecosystem, 
                        category_repos, 
                        category,
                        start_date, 
                        end_date, 
                        temp_dir, 
                        base_output_dir, 
                        use_parallel, 
                        max_workers, 
                        True,  
                        split_large_repos, 
                        batch_size, 
                        memory_limit,
                        timeframe
                    )
            
            except Exception as e:
                logger.error(f"Error analyzing {project_name} {category} repositories: {str(e)}")
                logger.debug(traceback.format_exc())
            finally:
                if os.path.exists(temp_dir):
                    import shutil
                    shutil.rmtree(temp_dir, ignore_errors=True)

def process_repo_group(project_name, ecosystem, repos, group_name, start_date, end_date, 
                   temp_dir, output_dir, use_parallel, max_workers, use_scheduler, 
                   split_large_repos, batch_size, memory_limit, timeframe):

    combined_metrics = {
        "combined_summary": {
            "repository_count": len(repos),
            "repositories": [repo['repo_url'] for repo in repos],
            "total_commits": 0,
            "total_lines_added": 0,
            "total_lines_removed": 0
        },
        "weekly_metrics": {}
    }
    
    all_repo_results = []
    failed_repos = []

    import subprocess
    import hashlib
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def check_access(repo):
        repo_url = repo['repo_url']
        try:
            subprocess.run(
                ["git", "ls-remote", repo_url],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=True
            )
            return (repo, None)
        except Exception:
            return (repo, repo_url)

    accessible_repos = []
    failed_repo_urls = []
    with ThreadPoolExecutor(max_workers=min(32, multiprocessing.cpu_count() * 2)) as executor:
        futures = [executor.submit(check_access, repo) for repo in repos]
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Checking {group_name} repo accessibility", unit="repo"):
            repo, failed_url = future.result()
            if failed_url is None:
                accessible_repos.append(repo)
            else:
                logger.warning(f"Skipping inaccessible repo: {failed_url}")
                failed_repo_urls.append(failed_url)

    from .repo_processing import process_single_repo
    import glob

    if use_scheduler:
        from .memory_scheduler import process_repos_with_scheduler
        all_repo_results = process_repos_with_scheduler(
            project_name=project_name,
            ecosystem=ecosystem,
            repos=accessible_repos,
            start_date=start_date,
            end_date=end_date,
            temp_dir=temp_dir,
            output_dir=output_dir,
            max_memory_percent=memory_limit,
            max_workers=max_workers
        )
    else:
        for i, repo in enumerate(accessible_repos):
            repo_url = repo['repo_url']
            repo_name = repo_url.split('/')[-1] if '/' in repo_url else f"repo_{i}"

            repo_hash = hashlib.md5(repo_name.encode()).hexdigest()[:8]
            repo_temp_dir = os.path.join(temp_dir, repo_hash)
            ensure_dir(repo_temp_dir)

            output_pattern = os.path.join(
                output_dir, f"{project_name}_{repo_name}_*_analysis"
            )
            existing_files = check_output_exists(output_dir, output_pattern)
            if existing_files:
                logger.info(f"Skipping already completed repo: {repo_name} (output exists: {os.path.basename(existing_files[0])})")
                continue

            if check_memory_pressure(memory_limit):
                logger.warning(f"Memory pressure before processing {repo_name}, waiting...")
                while get_memory_usage() > memory_limit - 5:
                    time.sleep(2)
                    gc.collect()

            logger.debug(f"Processing {repo_name}...")

            logger.debug(f"Starting process_single_repo for {repo_name}")
            try:
                repo_result = process_single_repo(
                    i,
                    repo,
                    project_name,
                    ecosystem,
                    group_name,
                    start_date,
                    end_date,
                    repo_temp_dir,
                    output_dir,
                    batch_size=batch_size,
                    memory_limit=memory_limit
                )
                logger.debug(f"Finished process_single_repo for {repo_name}")
                
                gc.collect()
            except Exception as e:
                logger.error(f"Exception in process_single_repo for {repo_name}: {str(e)}")
                logger.debug(traceback.format_exc())
                repo_result = {'error': str(e)}

            if 'error' in repo_result and repo_result['error']:
                logger.error(f"Repository {repo_name} failed: {repo_result['error']}")
                failed_repos.append(repo_name)
            else:
                all_repo_results.append(repo_result)

    combined_output_filename = f"{project_name}_{group_name}_combined_{timeframe}_analysis.json"
    combined_output_path = os.path.join(output_dir, combined_output_filename)
    
    
    with open(combined_output_path, 'w') as f:
        f.write('{\n')
        f.write(f'  "project_name": "{project_name}",\n')
        f.write(f'  "ecosystem": "{ecosystem}",\n')
        f.write(f'  "repo_category": "{group_name}",\n')
        f.write('  "analysis_period": {\n')
        f.write(f'    "start_date": "{start_date.strftime("%Y-%m-%d") if start_date else None}",\n')
        f.write(f'    "end_date": "{end_date.strftime("%Y-%m-%d") if end_date else None}",\n')
        f.write(f'    "full_history": {str(start_date is None and end_date is None).lower()}\n')
        f.write('  },\n')
        f.write(f'  "failed_repositories": {json.dumps(failed_repos + failed_repo_urls)},\n')
        
        for repo_result in all_repo_results:
            if 'summary' in repo_result:
                combined_metrics["combined_summary"]["total_commits"] += repo_result['summary'].get('total_commits', 0)
                combined_metrics["combined_summary"]["total_lines_added"] += repo_result['summary'].get('total_lines_added', 0)
                combined_metrics["combined_summary"]["total_lines_removed"] += repo_result['summary'].get('total_lines_removed', 0)
            
            if 'repo_name' in repo_result and 'metrics' in repo_result:
                repo_name = repo_result['repo_name']
                combined_metrics[f"repo_{repo_name}"] = repo_result['metrics']
                
                if isinstance(repo_result['metrics'], dict) and "warning" not in repo_result['metrics']:
                    for week, week_metrics in repo_result['metrics'].items():
                        if week not in combined_metrics["weekly_metrics"]:
                            combined_metrics["weekly_metrics"][week] = {}
                        
                        for metric_type, metric_data in week_metrics.items():
                            if metric_type not in combined_metrics["weekly_metrics"][week]:
                                combined_metrics["weekly_metrics"][week][metric_type] = metric_data
                            else:
                                if isinstance(metric_data, dict) and "count" in metric_data:
                                    if "count" in combined_metrics["weekly_metrics"][week][metric_type]:
                                        combined_metrics["weekly_metrics"][week][metric_type]["count"] += metric_data["count"]
                                
                                if isinstance(metric_data, dict) and "max" in metric_data:
                                    if "max" in combined_metrics["weekly_metrics"][week][metric_type]:
                                        combined_metrics["weekly_metrics"][week][metric_type]["max"] = max(
                                            combined_metrics["weekly_metrics"][week][metric_type]["max"],
                                            metric_data["max"]
                                        )
                                
                                if isinstance(metric_data, dict) and "added" in metric_data:
                                    for category in ["added", "removed"]:
                                        if category in metric_data and category in combined_metrics["weekly_metrics"][week][metric_type]:
                                            if "total" in metric_data[category]:
                                                combined_metrics["weekly_metrics"][week][metric_type][category]["total"] += metric_data[category]["total"]
                                            if "max" in metric_data[category]:
                                                combined_metrics["weekly_metrics"][week][metric_type][category]["max"] = max(
                                                    combined_metrics["weekly_metrics"][week][metric_type][category]["max"],
                                                    metric_data[category]["max"]
                                                )
        
        f.write(f'  "combined_metrics": {json.dumps(combined_metrics, default=str, indent=2)}\n')
        f.write('}\n')
    
    logger.debug(f"Combined analysis complete for {project_name} {group_name} repositories!")
    
    for i, repo in enumerate(repos):
        repo_url = repo['repo_url']
        repo_name = repo_url.split('/')[-1] if '/' in repo_url else f"repo_{i}"
        
        if repo_name in failed_repos:
            continue
        
        if f"repo_{repo_name}" not in combined_metrics:
            continue
        
        output_pattern = os.path.join(
            output_dir, f"{project_name}_{repo_name}_*_analysis"
        )
        existing_files = check_output_exists(output_dir, output_pattern)
        
        if existing_files:
            continue
        
        repo_metrics = combined_metrics[f"repo_{repo_name}"]
        
        output_filename = f"{project_name}_{repo_name}_{timeframe}_analysis.json"
        output_path = os.path.join(output_dir, output_filename)
        
        with open(output_path, 'w') as f:
            f.write('{\n')
            f.write(f'  "project_name": "{project_name}",\n')
            f.write(f'  "repository_url": "{repo_url}",\n')
            f.write(f'  "repository_name": "{repo_name}",\n')
            f.write(f'  "ecosystem": "{ecosystem}",\n')
            f.write(f'  "repo_category": "{group_name}",\n')
            f.write('  "analysis_period": {\n')
            f.write(f'    "start_date": "{start_date.strftime("%Y-%m-%d") if start_date else None}",\n')
            f.write(f'    "end_date": "{end_date.strftime("%Y-%m-%d") if end_date else None}",\n')
            f.write(f'    "full_history": {str(start_date is None and end_date is None).lower()}\n')
            f.write('  },\n')
            f.write(f'  "process_metrics": {json.dumps(repo_metrics, default=str, indent=2)}\n')
            f.write('}\n')