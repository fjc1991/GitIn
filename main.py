import multiprocessing
import sys
import time
import json
import os
import traceback
import argparse

from source.logger import get_logger
from source.utils import ensure_dir, cleanup_temp_dirs, MASTER_OUTPUT_DIR, get_repo_path
from source.project_finder import find_all_projects
from source.analysis import analyze_organization_repos_enhanced
from source.file_filters import should_analyze_file
from tqdm import tqdm

logger = get_logger(__name__)
sys.setrecursionlimit(100000)
COMPLETED_USERS_FILE = os.path.join(MASTER_OUTPUT_DIR, 'completed_users.json')
USERS = 'github_repos_test.csv'

def load_completed_users():
    """Load the list of completed users."""
    if os.path.exists(COMPLETED_USERS_FILE):
        try:
            with open(COMPLETED_USERS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading completed users file: {str(e)}")
    return []

def save_completed_user(username):
    """Add a username to the completed users list."""
    completed = load_completed_users()
    if username not in completed:
        completed.append(username)
        try:
            with open(COMPLETED_USERS_FILE, 'w') as f:
                json.dump(completed, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving to completed users file: {str(e)}")

def analyze_all_projects(folder_filter=None, csv_path=USERS, start_year=None, 
                         start_month=None, end_year=None, end_month=None, limit=None, 
                         workers=4, use_parallel=True, split_large_repos=True, 
                         file_filter_fn=should_analyze_file, skip_completed=True):
    
    start_time = time.time()
    
    logger.info(f"Python recursion limit set to: {sys.getrecursionlimit()}")

    completed_users = [] if not skip_completed else load_completed_users()
    projects_by_username = find_all_projects(folder_filter, csv_path)
    
    if not projects_by_username:
        logger.warning("No projects found to analyze.")
        return
    
    usernames = list(projects_by_username.keys())
    if limit and limit > 0 and limit < len(usernames):
        usernames = usernames[:limit]
        logger.info(f"Limiting analysis to first {limit} users.")

    remaining_users = []
    for username in usernames:
        if username in completed_users and skip_completed:
            logger.info(f"Skipping already completed user: {username}")
        else:
            remaining_users.append(username)
    
    if not remaining_users:
        logger.info("All users already completed. Nothing to do.")
        return
        
    usernames = remaining_users
    total_users = len(usernames)
    logger.info(f"Found {total_users} users to analyze.")
    logger.info(f"Using {workers} worker processes with parallel={use_parallel}, split_large_repos={split_large_repos}")
    user_progress = tqdm(total=total_users, desc="Overall Progress", position=0, leave=True)
    
    try:
        # Analyze each user's repositories together
        for i, username in enumerate(usernames):
            iteration_start = time.time()
            
            user_info = projects_by_username[username]
            ecosystem = user_info['ecosystem']
            repositories = user_info['repositories']
            
            user_output_dir = os.path.join(MASTER_OUTPUT_DIR, username)
            ensure_dir(user_output_dir)

            user_progress.set_description(f"Processing {username} ({i+1}/{total_users})")
            logger.info(f"Starting analysis of user: {username} ({i+1}/{total_users})")
            filtered_repos = []
            
            for repo in repositories:
                repo_path = get_repo_path(repo)
                if not file_filter_fn(repo_path):
                    logger.info(f"Skipping filtered repository: {repo_path}")
                    continue
                    
                filtered_repos.append(repo)
            
            repositories = filtered_repos
            repo_count = len(repositories)
            
            logger.info(f"Analyzing {username} with {repo_count} repositories")
            
            # Each user gets its own try-except block to isolate failures
            try:
                with tqdm(total=1, desc=f"Analyzing {username}", leave=False) as user_pbar:
                    analyze_organization_repos_enhanced(
                        project_name=username,
                        ecosystem=ecosystem,
                        repos=repositories,
                        start_year=start_year,
                        start_month=start_month,
                        end_year=end_year,
                        end_month=end_month,
                        use_parallel=use_parallel,
                        max_workers=workers,
                        split_large_repos=split_large_repos,
                        output_dir_override=user_output_dir
                    )
                    user_pbar.update(1)
                
                save_completed_user(username)
                logger.info(f"User {username} successfully completed")
                
            except Exception as e:
                logger.error(f"Failed to analyze {username}: {str(e)}")
                logger.debug(traceback.format_exc())
            
            try:
                cleanup_temp_dirs()
            except Exception as e:
                logger.warning(f"Error during cleanup: {str(e)}")
            
            user_progress.update(1)
            iteration_time = time.time() - iteration_start
            logger.info(f"Processing time for {username}: {iteration_time:.2f}s")
        
    finally:
        pass

    user_progress.close()
    
    total_time = time.time() - start_time
    logger.info("\n" + "="*80)
    logger.info("ANALYSIS COMPLETE!")
    logger.info(f"Successfully processed {total_users} users")
    logger.info(f"Total processing time: {total_time:.2f}s")
    logger.info("="*80)

    try:
        logger.info("Performing final cleanup of temporary directories...")
        cleanup_temp_dirs()
        logger.info("Temporary directories cleanup completed.")
    except Exception as e:
        logger.warning(f"Error during final cleanup: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze GitHub user repositories')
    parser.add_argument('--folder', type=str, help='Only analyze usernames starting with this letter/character')
    parser.add_argument('--csv', type=str, default=USERS, help='Path to CSV file with repositories')
    parser.add_argument('--start-year', type=int, help='Start year for analysis')
    parser.add_argument('--start-month', type=int, help='Start month for analysis (1-12)')
    parser.add_argument('--end-year', type=int, help='End year for analysis')
    parser.add_argument('--end-month', type=int, help='End month for analysis (1-12)')
    parser.add_argument('--limit', type=int, help='Limit analysis to first N users')
    parser.add_argument('--cleanup-temp', action='store_true', help='Clean up all temporary directories before starting')
    parser.add_argument('--workers', type=int, default=multiprocessing.cpu_count()-1, help='Number of parallel workers')
    parser.add_argument('--disable-parallel', action='store_true', help='Disable parallel processing')
    parser.add_argument('--disable-repo-splitting', action='store_true', help='Disable splitting of large repositories')
    parser.add_argument('--recursion-limit', type=int, default=20000, help='Set Python recursion limit (default: 20000)')
    parser.add_argument('--force-reprocess', action='store_true', help='Process even already completed users')
    
    args = parser.parse_args()
    
    if args.recursion_limit:
        sys.setrecursionlimit(args.recursion_limit)
        logger.info(f"Setting Python recursion limit to: {args.recursion_limit}")
    
    if args.cleanup_temp:
        cleanup_temp_dirs()
    
    analyze_all_projects(
        folder_filter=args.folder,
        csv_path=args.csv,
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month,
        limit=args.limit,
        workers=args.workers,
        use_parallel=not args.disable_parallel,
        split_large_repos=not args.disable_repo_splitting,
        file_filter_fn=should_analyze_file,
        skip_completed=not args.force_reprocess
    )