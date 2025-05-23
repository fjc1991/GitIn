import os
import shutil
import traceback
import gc
import psutil
from datetime import datetime, timedelta
from pydriller import Repository
import time
from tqdm import tqdm
import hashlib
import json

# Fix relative imports
from .logger import get_logger
from .utils import ensure_dir, extract_commit_info, get_repo_date_range

logger = get_logger(__name__)

def get_memory_usage():
    """Get current memory usage as percentage."""
    return psutil.virtual_memory().percent

def check_memory_pressure(threshold=85):
    """Check if memory usage is above threshold and perform garbage collection if needed."""
    memory_percent = get_memory_usage()
    if memory_percent > threshold:
        logger.warning(f"Memory pressure detected: {memory_percent}% used")
        # Force garbage collection
        gc.collect()
        return True
    return False

def estimate_repo_size(repo_url, temp_dir, since=None, to=None, sample_size=100):
    """
    Estimate repository size by sampling commits.
    Returns estimated number of commits and a flag if the repo should be split.
    """
    try:
        logger.debug(f"Estimating repository size for {repo_url}...")
        repo_args = {'path_to_repo': repo_url, 'clone_repo_to': temp_dir}
        
        if since is not None:
            repo_args['since'] = since
        if to is not None:
            repo_args['to'] = to
            
        repository = Repository(**repo_args)
        
        # Count commits up to sample_size to estimate
        commit_count = 0
        for _ in repository.traverse_commits():
            commit_count += 1
            if commit_count >= sample_size:
                break
                
        should_split = commit_count >= 500  # Threshold for splitting
        
        logger.debug(f"Repository size estimate: {commit_count}+ commits. Split recommendation: {should_split}")
        return commit_count, should_split
    except Exception as e:
        logger.error(f"Error estimating repository size: {str(e)}")
        return 0, False

def split_date_range(start_date, end_date, chunks):
    """
    Split a date range into roughly equal chunks.
    Returns a list of (chunk_start, chunk_end) tuples.
    """
    if start_date is None or end_date is None:
        return [(None, None)]
    
    total_days = (end_date - start_date).days + 1
    days_per_chunk = max(1, total_days // chunks)
    
    ranges = []
    chunk_start = start_date
    
    for i in range(chunks):
        if i == chunks - 1:
            chunk_end = end_date
        else:
            chunk_end = chunk_start + timedelta(days=days_per_chunk - 1)
            if chunk_end > end_date:
                chunk_end = end_date
        
        ranges.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
        
        if chunk_start > end_date:
            break
    
    return ranges

def merge_commit_results(all_chunk_results):
    """
    Merge commit data from multiple processing chunks.
    """
    merged_commits = []
    
    for chunk_result in all_chunk_results:
        if 'commits' in chunk_result:
            merged_commits.extend(chunk_result['commits'])
    
    return merged_commits

def process_repo_chunk(repo_url, chunk_start, chunk_end, temp_dir_prefix, output_dir=None, batch_size=1000, memory_limit=85):
    """
    Process a specific chunk of repository history.
    Modified for cleaner console output and memory efficiency.
    """
    repo_name = repo_url.split('/')[-1] if '/' in repo_url else 'unnamed_repo'
    chunk_id = f"{chunk_start.strftime('%Y%m%d') if chunk_start else 'start'}_to_{chunk_end.strftime('%Y%m%d') if chunk_end else 'end'}"
    
    repo_hash = hashlib.md5(repo_name.encode()).hexdigest()[:8]
    temp_dir = os.path.join(temp_dir_prefix, f"{repo_hash}_{chunk_id}")
    ensure_dir(temp_dir)
    
    try:
        chunk_result = {
            'chunk_id': chunk_id,
            'summary': {
                'start_date': chunk_start.strftime('%Y-%m-%d') if chunk_start else None,
                'end_date': chunk_end.strftime('%Y-%m-%d') if chunk_end else None,
                'commit_count': 0,
                'lines_added': 0,
                'lines_removed': 0
            }
        }
        
        output_commit_file = os.path.join(temp_dir, f"commits_{chunk_id}.jsonl")
        chunk_result['commit_file_path'] = output_commit_file
        
        repo_args = {
            'path_to_repo': repo_url,
            'clone_repo_to': temp_dir
        }
        
        if chunk_start:
            repo_args['since'] = chunk_start
        if chunk_end:
            repo_args['to'] = chunk_end
            
        repository = Repository(**repo_args)
        
        commit_count = 0
        for _ in repository.traverse_commits():
            commit_count += 1
        
        if commit_count == 0:
            logger.debug(f"No commits found in chunk {chunk_id}")
            return chunk_result
            
        repository = Repository(**repo_args)
        
        logger.debug(f"Processing {commit_count} commits in chunk {chunk_id}")
        
        commits_batch = []
        processed_commits = 0
        
        with tqdm(total=commit_count, desc=f"Chunk {chunk_id}", unit="commit", leave=False) as pbar:
            for commit in repository.traverse_commits():
                if processed_commits % 100 == 0 and check_memory_pressure(memory_limit):
                    logger.warning(f"Memory pressure during chunk processing at {processed_commits}/{commit_count}, waiting...")
                    while get_memory_usage() > memory_limit - 5:
                        time.sleep(2)
                        gc.collect()
                
                commit_info = extract_commit_info(commit)
                commits_batch.append(commit_info)
                
                chunk_result['summary']['commit_count'] += 1
                chunk_result['summary']['lines_added'] += commit.insertions
                chunk_result['summary']['lines_removed'] += commit.deletions
                
                processed_commits += 1
                
                if len(commits_batch) >= batch_size or processed_commits == commit_count:
                    with open(output_commit_file, 'a') as f:
                        for c_info in commits_batch:
                            f.write(json.dumps(c_info, default=str) + '\n')
                    
                    commits_batch = []
                    gc.collect()
                
                pbar.update(1)
        
        try:
            logger.debug(f"Calculating metrics for chunk {chunk_id}")
            # Fix import statement
            from .metrics import calculate_metrics
            chunk_result['metrics'] = calculate_metrics(
                repo_url, temp_dir, 
                chunk_start, chunk_end, 
                calculate_weekly=True,
                memory_limit=memory_limit
            )
        except Exception as e:
            logger.error(f"Could not calculate metrics for chunk {chunk_id}: {str(e)}")
            chunk_result['metrics'] = {"error": str(e)}
        
        logger.debug(f"Chunk {chunk_id} processing complete with {chunk_result['summary']['commit_count']} commits")
        return chunk_result
        
    except Exception as e:
        logger.error(f"Error processing chunk {chunk_id} for {repo_name}: {str(e)}")
        logger.debug(traceback.format_exc())
        return {'chunk_id': chunk_id, 'error': str(e)}
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

def process_chunk_wrapper(chunk_data, repo_url, temp_dir_prefix, output_dir):
    """Wrapper function to correctly pass parameters to process_repo_chunk."""
    chunk_start, chunk_end = chunk_data
    return process_repo_chunk(repo_url, chunk_start, chunk_end, temp_dir_prefix, output_dir)

def process_single_repo(repo_index, repo, project_name, ecosystem, category, start_date, end_date, temp_dir, output_dir, use_chronological=False, batch_size=1000, memory_limit=85):
    """Process a single repository. Module-level function for multiprocessing compatibility."""
    repo_url = repo['repo_url']
    repo_name = repo_url.split('/')[-1] if '/' in repo_url else f"repo_{repo_index}"
    
    logger.debug(f"Processing {repo_name} from {project_name}")
    
    repo_hash = hashlib.md5(repo_name.encode()).hexdigest()[:8]
    repo_temp_dir = os.path.join(temp_dir, repo_hash)
    ensure_dir(repo_temp_dir)
    
    timeframe = ""
    if start_date and end_date:
        timeframe = f"{start_date.year}_{start_date.month}_to_{end_date.year}_{end_date.month}"
    else:
        timeframe = "full_history"

    output_pattern = os.path.join(output_dir, f"{project_name}_{repo_name}_{timeframe}_analysis.json")
    output_pattern_7z = os.path.join(output_dir, f"{project_name}_{repo_name}_{timeframe}_analysis.7z")

    output_path = output_pattern

    if os.path.exists(output_pattern) or os.path.exists(output_pattern_7z):
        logger.info(f"Output file already exists for {repo_name}, skipping processing")
        try:
            with open(output_path, 'r') as f:
                data = json.load(f)
                return {
                    'commits': data.get('commits', []),
                    'metrics': data.get('process_metrics', {}),
                    'summary': {
                        'total_commits': data.get('processing', {}).get('total_commits', 0),
                        'total_lines_added': data.get('processing', {}).get('total_lines_added', 0),
                        'total_lines_removed': data.get('processing', {}).get('total_lines_removed', 0)
                    },
                    'repo_name': repo_name,
                    'repo_url': repo_url
                }
        except Exception as e:
            logger.error(f"Error reading existing output file for {repo_name}: {str(e)}")
    
    start_year = start_date.year if start_date else None
    start_month = start_date.month if start_date else None
    end_year = end_date.year if end_date else None
    end_month = end_date.month if end_date else None
    
    try:
        process_repo_directly(
            project_name, repo_url, start_date, end_date,
            ecosystem, category, repo_temp_dir, output_dir,
            use_chronological=use_chronological,
            batch_size=batch_size,
            memory_limit=memory_limit
        )
    except Exception as e:
        logger.error(f"Error in process_repo_directly: {str(e)}")
        logger.debug(traceback.format_exc())
    
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r') as f:
                data = json.load(f)
                return {
                    'commits': data.get('commits', []),
                    'metrics': data.get('process_metrics', {}),
                    'summary': {
                        'total_commits': data.get('processing', {}).get('total_commits', 0),
                        'total_lines_added': data.get('processing', {}).get('total_lines_added', 0),
                        'total_lines_removed': data.get('processing', {}).get('total_lines_removed', 0)
                    },
                    'repo_name': repo_name,
                    'repo_url': repo_url
                }
        except Exception as e:
            logger.error(f"Error reading analysis file for {repo_name}: {str(e)}")
    
    logger.warning(f"Could not find or read analysis file for {repo_name} at {output_path}")
    return {
        'commits': [],
        'metrics': {},
        'summary': {'total_commits': 0, 'total_lines_added': 0, 'total_lines_removed': 0},
        'repo_name': repo_name,
        'repo_url': repo_url
    }

def process_repo_directly(project_name, repo_url, start_date, end_date, ecosystem, category, temp_dir, output_dir, use_chronological=False, batch_size=1000, memory_limit=85):
    """
    Direct implementation of repo processing logic to avoid circular imports.
    This is a memory-efficient implementation that streams data to files.
    """
    repo_name = repo_url.split('/')[-1] if '/' in repo_url else 'unnamed_repo'
    
    logger.debug(f"Using direct implementation for {repo_name}")
    
    timeframe = ""
    if start_date and end_date:
        timeframe = f"{start_date.year}_{start_date.month}_to_{end_date.year}_{end_date.month}"
    else:
        timeframe = "full_history"
    
    repo_hash = hashlib.md5(repo_name.encode()).hexdigest()[:8]
    repo_temp_dir = os.path.join(temp_dir, f"{repo_hash}")
    ensure_dir(repo_temp_dir)
    
    output_filename = f"{project_name}_{repo_name}_{timeframe}_analysis.json"
    output_path = os.path.join(output_dir, output_filename)
    
    try:
        # Get repo date range if doing full history analysis
        if start_date is None or end_date is None:
            logger.debug(f"Determining date range for full history analysis of {repo_name}")
            try:
                # Fix import statement
                from .utils import get_repo_date_range
                extracted_start_date, extracted_end_date = get_repo_date_range(repo_url, repo_temp_dir)
                if start_date is None:
                    start_date = extracted_start_date
                if end_date is None:
                    end_date = extracted_end_date
                logger.debug(f"Extracted date range: {start_date} to {end_date}")
            except Exception as e:
                logger.error(f"Failed to extract date range for {repo_name}: {str(e)}")
        
        estimated_commits, should_split = estimate_repo_size(repo_url, repo_temp_dir, start_date, end_date)
        
        with open(output_path, 'w') as f:
            f.write('{\n')
            f.write(f'  "project_name": "{project_name}",\n')
            f.write(f'  "repository_url": "{repo_url}",\n')
            f.write(f'  "repository_name": "{repo_name}",\n')
            f.write(f'  "ecosystem": "{ecosystem}",\n')
            f.write(f'  "repo_category": "{category}",\n')
            f.write('  "analysis_period": {\n')
            f.write(f'    "start_date": "{start_date.strftime("%Y-%m-%d") if start_date else None}",\n')
            f.write(f'    "end_date": "{end_date.strftime("%Y-%m-%d") if end_date else None}",\n')
            f.write(f'    "full_history": {str(start_date is None and end_date is None).lower()}\n')
            f.write('  },\n')
            f.write('  "commits": [\n')
        
        if should_split and estimated_commits > 500:
            logger.debug(f"Repository {repo_name} is large. Processing in chunks...")
            
            chunk_count = min(4, max(2, estimated_commits // 200))
            
            date_chunks = split_date_range(start_date, end_date, chunk_count)
            logger.debug(f"Processing {repo_name} in {len(date_chunks)} chunks")
            
            all_chunk_results = []
            all_chunk_metrics = []
            total_commits = 0
            total_lines_added = 0
            total_lines_removed = 0
            
            for i, (chunk_start, chunk_end) in enumerate(date_chunks):
                if check_memory_pressure(memory_limit):
                    logger.warning(f"Memory pressure before chunk {i+1}/{len(date_chunks)}, waiting...")
                    while get_memory_usage() > memory_limit - 5:
                        time.sleep(2)
                        gc.collect()
                
                chunk_result = process_repo_chunk(
                    repo_url, chunk_start, chunk_end, repo_temp_dir, output_dir,
                    batch_size=batch_size,
                    memory_limit=memory_limit
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
                
                if 'metrics' in chunk_result and chunk_result['metrics']:
                    all_chunk_metrics.append(chunk_result['metrics'])
                
                if 'summary' in chunk_result:
                    total_commits = max(total_commits, chunk_result['summary'].get('commit_count', 0))
                    total_lines_added += chunk_result['summary'].get('lines_added', 0)
                    total_lines_removed += chunk_result['summary'].get('lines_removed', 0)
                
                all_chunk_results.append(chunk_result)
                
                gc.collect()
            
            # Fix import statement
            from .metrics import merge_metrics_results
            merged_metrics = merge_metrics_results(all_chunk_results)
        else:
            logger.debug(f"Processing {repo_name} as a single unit...")
            
            repo_args = {
                'path_to_repo': repo_url,
                'clone_repo_to': repo_temp_dir
            }
            
            if start_date:
                repo_args['since'] = start_date
            if end_date:
                repo_args['to'] = end_date
            
            repository = Repository(**repo_args)
            commit_count = 0
            for _ in repository.traverse_commits():
                commit_count += 1
            
            if commit_count == 0:
                logger.debug(f"No commits found for {repo_name}")
                with open(output_path, 'a') as f:
                    f.write('\n  ],\n')
                    f.write('  "process_metrics": {},\n')
                    f.write('  "metrics_type": "weekly",\n')
                    f.write('  "processing": {\n')
                    f.write('    "total_commits": 0,\n')
                    f.write('    "total_lines_added": 0,\n')
                    f.write('    "total_lines_removed": 0\n')
                    f.write('  }\n')
                    f.write('}\n')
                return
            
            repository = Repository(**repo_args)
            total_commits = 0
            total_lines_added = 0
            total_lines_removed = 0
            
            commits_batch = []
            processed_commits = 0
            
            with tqdm(total=commit_count, desc=f"Processing {repo_name} commits", unit="commit", leave=False) as pbar:
                first_commit = True
                
                for commit in repository.traverse_commits():
                    try:
                        if processed_commits % 100 == 0 and check_memory_pressure(memory_limit):
                            logger.warning(f"Memory pressure during commit processing at {processed_commits}/{commit_count}, waiting...")
                            while get_memory_usage() > memory_limit - 5:
                                time.sleep(2)
                                gc.collect()
                        
                        commit_info = extract_commit_info(commit)
                        commits_batch.append(commit_info)
                        
                        total_commits += 1
                        total_lines_added += commit.insertions
                        total_lines_removed += commit.deletions
                        
                        processed_commits += 1
                        
                        if len(commits_batch) >= batch_size or processed_commits == commit_count:
                            with open(output_path, 'a') as f:
                                for i, c_info in enumerate(commits_batch):
                                    if not first_commit or i > 0:
                                        f.write(',\n')
                                    f.write('    ' + json.dumps(c_info, default=str))
                                    first_commit = False
                            
                            commits_batch = []
                            gc.collect()
                        
                    except Exception as e:
                        logger.error(f"Error extracting commit info: {str(e)}")
                        logger.debug(traceback.format_exc())
                    
                    pbar.update(1)
            
            # Import and calculate metrics properly
            from .metrics.aggregator import calculate_metrics
            merged_metrics = calculate_metrics(
                repo_url, repo_temp_dir, 
                start_date, end_date, 
                calculate_weekly=True,
                memory_limit=memory_limit
            )
        
        with open(output_path, 'a') as f:
            f.write('\n  ],\n')
            f.write('  "process_metrics": ' + json.dumps(merged_metrics, default=str, indent=2) + ',\n')
            f.write('  "metrics_type": "weekly",\n')
            f.write('  "processing": {\n')
            f.write(f'    "total_commits": {total_commits},\n')
            f.write(f'    "total_lines_added": {total_lines_added},\n')
            f.write(f'    "total_lines_removed": {total_lines_removed}\n')
            f.write('  }\n')
            f.write('}\n')
        
        logger.debug(f"Direct processing complete for {repo_name}!")
    
    except Exception as e:
        logger.error(f"Error in direct processing for {repo_name}: {str(e)}")
        logger.debug(traceback.format_exc())
    finally:
        if os.path.exists(repo_temp_dir):
            shutil.rmtree(repo_temp_dir, ignore_errors=True)