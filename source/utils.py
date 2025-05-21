import os
import hashlib
import json
import shutil
import datetime
from datetime import timedelta
import logging
from functools import lru_cache
from tqdm import tqdm
from pydriller import Repository

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
MASTER_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "data")
LOGS_DIR = os.path.join(OUTPUT_DIR, "logs")
MASTER_TEMP_DIR = os.path.join(OUTPUT_DIR, "temp")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")

for directory in [OUTPUT_DIR, MASTER_OUTPUT_DIR, LOGS_DIR, MASTER_TEMP_DIR, CACHE_DIR]:
    os.makedirs(directory, exist_ok=True)

# Legacy path for backward compatibility
LEGACY_PROJECTS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "projects")

# Function to ensure directory exists
def ensure_dir(directory):
    """Ensure a directory exists, creating it if necessary."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.debug(f"Created directory: {directory}")
    return directory

# Ensure base directories exist
ensure_dir(MASTER_OUTPUT_DIR)
ensure_dir(MASTER_TEMP_DIR)

# Cache for processed files
CACHE_DIR = ensure_dir(os.path.join(MASTER_OUTPUT_DIR, 'cache'))

def get_repo_path(repo):
    """Extract repository path from repository object or string."""
    if isinstance(repo, dict):
        return repo.get('url', repo.get('repo_url', repo.get('path', str(repo))))
    return str(repo)

def get_file_hash(repo):
    """Hash calculation for repository or path."""
    repo_path = get_repo_path(repo)
    hash_value = hashlib.md5(repo_path.encode('utf-8')).hexdigest()
    logger.debug(f"Hashing repo path: {repo_path} -> {hash_value}")
    return hash_value

@lru_cache(maxsize=1000)
def get_path_hash(path_str):
    """Cache-enabled hash calculation for string paths only."""
    return hashlib.md5(path_str.encode('utf-8')).hexdigest()

def load_file_cache():
    """Load the cache of processed files."""
    cache_file = os.path.join(CACHE_DIR, 'processed_files.json')
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                cache = json.load(f)
                logger.debug(f"Loaded file cache with {len(cache)} entries")
                return cache
        except Exception as e:
            logger.error(f"Error loading file cache: {str(e)}")
    logger.debug("No existing file cache found or cache could not be loaded")
    return {}

def save_file_cache(cache):
    """Save the cache of processed files."""
    if not cache:
        logger.warning("Attempted to save empty cache - this may indicate an issue")
        
    cache_file = os.path.join(CACHE_DIR, 'processed_files.json')
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache, f)
        logger.debug(f"Saved file cache with {len(cache)} entries")
    except Exception as e:
        logger.error(f"Error saving file cache: {str(e)}")

# Global process pool for reuse
_process_pool = None

# Function to clean up all temporary directories
def cleanup_temp_dirs():
    """Clean up all temporary directories in the master temp folder."""
    if os.path.exists(MASTER_TEMP_DIR):
        try:
            # Try to remove the whole directory
            logger.debug(f"Cleaning up temporary directory: {MASTER_TEMP_DIR}")
            shutil.rmtree(MASTER_TEMP_DIR, ignore_errors=True)
        except Exception as e:
            logger.debug(f"Warning: Error during temp directory cleanup: {str(e)}")
            # If removal fails, try to remove files one by one
            try:
                logger.info("Attempting file-by-file cleanup...")
                for root, dirs, files in os.walk(MASTER_TEMP_DIR):
                    for f in files:
                        try:
                            os.unlink(os.path.join(root, f))
                        except:
                            pass
                    for d in dirs:
                        try:
                            shutil.rmtree(os.path.join(root, d))
                        except:
                            pass
            except:
                logger.info("Warning: Could not clean temp directory contents")
        
        # Recreate the directory
        os.makedirs(MASTER_TEMP_DIR, exist_ok=True)
        logger.debug(f"Recreated master temporary directory: {MASTER_TEMP_DIR}")
    
    # Also check for PyDriller's temp directories that might not be in MASTER_TEMP_DIR
    pydriller_temp = os.path.join(os.path.expanduser("~"), ".pydriller")
    if os.path.exists(pydriller_temp):
        try:
            logger.debug(f"Cleaning up PyDriller temp directory: {pydriller_temp}")
            shutil.rmtree(pydriller_temp, ignore_errors=True)
        except Exception as e:
            logger.debug(f"Warning: Error during PyDriller temp cleanup: {str(e)}")

# Define a function to extract modified file information
def extract_file_info(modified_file):
    file_info = {
        'old_path': modified_file.old_path,
        'new_path': modified_file.new_path,
        'filename': modified_file.filename,
        'change_type': str(modified_file.change_type),
        'change_type_name': modified_file.change_type.name if modified_file.change_type else None,
        'diff': modified_file.diff,
        'diff_parsed': {
            'added': [(line[0], line[1]) for line in modified_file.diff_parsed['added']],
            'deleted': [(line[0], line[1]) for line in modified_file.diff_parsed['deleted']]
        },
        'added_lines': modified_file.added_lines,
        'deleted_lines': modified_file.deleted_lines,
        'source_code': None,  # Will be set later if safe
        'source_code_before': None,  # Will be set later if safe
        'methods': [{'name': method.name, 'start_line': method.start_line, 'end_line': method.end_line} 
                   for method in modified_file.methods] if modified_file.methods else [],
        'methods_before': [{'name': method.name, 'start_line': method.start_line, 'end_line': method.end_line} 
                          for method in modified_file.methods_before] if modified_file.methods_before else [],
        'changed_methods': [{'name': method.name, 'start_line': method.start_line, 'end_line': method.end_line} 
                           for method in modified_file.changed_methods] if modified_file.changed_methods else [],
    }
    
    # Check source code size before setting
    try:
        if modified_file.source_code and len(modified_file.source_code) < 1024 * 1024:  # 1MB limit
            file_info['source_code'] = modified_file.source_code
        elif modified_file.source_code:
            logger.debug(f"Source code too large for {modified_file.filename}, skipping")
    except Exception as e:
        logger.debug(f"Error accessing source_code for {modified_file.filename}: {str(e)}")
    
    # Check source code before size
    try:
        if modified_file.source_code_before and len(modified_file.source_code_before) < 1024 * 1024:  # 1MB limit
            file_info['source_code_before'] = modified_file.source_code_before
        elif modified_file.source_code_before:
            logger.debug(f"Source code before too large for {modified_file.filename}, skipping")
    except Exception as e:
        logger.debug(f"Error accessing source_code_before for {modified_file.filename}: {str(e)}")
    
    # Add metrics that might not be available for all files
    try:
        file_info['nloc'] = modified_file.nloc
    except:
        file_info['nloc'] = None
        
    try:
        file_info['complexity'] = modified_file.complexity
    except:
        file_info['complexity'] = None
        
    try:
        file_info['token_count'] = modified_file.token_count
    except:
        file_info['token_count'] = None
        
    return file_info

# Generate weekly date ranges
def generate_weekly_ranges(start_date, end_date):
    """
    Generate a list of weekly date ranges from start_date to end_date.
    """
    weekly_ranges = []
    current_date = start_date
    week_number = 1
    
    while current_date < end_date:
        week_end = current_date + timedelta(days=6)
        if week_end > end_date:
            week_end = end_date
        
        week_label = f"Week_{week_number}_{current_date.strftime('%Y-%m-%d')}"
        weekly_ranges.append((current_date, week_end, week_label))
        
        current_date = week_end + timedelta(days=1)
        week_number += 1
        
    return weekly_ranges

# Define a function to extract all commit information
def extract_commit_info(commit):
    """
    Extract comprehensive information from a commit, with progress bar for large commits.
    Added safety checks for large files to prevent recursion errors.
    """
    try:
        # Basic commit info extraction
        commit_info = {
            'hash': commit.hash,
            'msg': commit.msg,
            'author': {
                'name': commit.author.name,
                'email': commit.author.email
            },
            'committer': {
                'name': commit.committer.name,
                'email': commit.committer.email
            },
            'author_date': commit.author_date,
            'author_timezone': commit.author_timezone,
            'committer_date': commit.committer_date,
            'committer_timezone': commit.committer_timezone,
            'branches': commit.branches,
            'in_main_branch': commit.in_main_branch,
            'merge': commit.merge,
            'parents': commit.parents,
            'project_name': commit.project_name,
            'project_path': commit.project_path,
            'deletions': commit.deletions,
            'insertions': commit.insertions,
            'lines': commit.lines,
            'files': commit.files,
        }
        
        # Add progress bar for modified files if there are many
        modified_files = commit.modified_files
        modified_files_count = len(modified_files)
        
        # Use progress bar for commits with many modified files
        if (modified_files_count > 10):
            modified_files_info = []
            with tqdm(total=modified_files_count, desc=f"Commit {commit.hash[:7]}", 
                    unit="file", leave=False) as file_pbar:
                for mod_file in modified_files:
                    try:
                        # Skip problematic files to prevent recursion errors
                        filename = mod_file.filename
                        
                        # Check for Solidity JS compiler files specifically
                        if filename.startswith('soljson-v') and filename.endsWith('.js'):
                            logger.debug(f"Skipping potentially problematic file: {filename}")
                            file_pbar.update(1)
                            continue
                            
                        # Check file size if source_code is available
                        if mod_file.source_code and len(mod_file.source_code) > 5 * 1024 * 1024:  # > 5MB
                            logger.debug(f"File too large (source_code > 5MB): {filename}")
                            file_pbar.update(1)
                            continue
                            
                        modified_files_info.append(extract_file_info(mod_file))
                    except RecursionError:
                        logger.debug(f"Failed to process '{mod_file.filename}' with RecursionError")
                    except Exception as e:
                        logger.debug(f"Error processing file '{mod_file.filename}': {str(e)}")
                    
                    file_pbar.update(1)
            commit_info['modified_files'] = modified_files_info
        else:
            # For small number of files, process without progress bar
            modified_files_info = []
            for mod_file in modified_files:
                try:
                    # Skip problematic files to prevent recursion errors
                    filename = mod_file.filename
                    # Check for Solidity JS compiler files specifically
                    if filename.startswith('soljson-v') and filename.endsWith('.js'):
                        logger.debug(f"Skipping potentially problematic file: {filename}")
                        continue
                        
                    # Check file size if source_code is available
                    if mod_file.source_code and len(mod_file.source_code) > 5 * 1024 * 1024:  # > 5MB
                        logger.debug(f"File too large (source_code > 5MB): {filename}")
                        continue
                        
                    modified_files_info.append(extract_file_info(mod_file))
                except RecursionError:
                    logger.debug(f"Failed to process '{mod_file.filename}' with RecursionError")
                except Exception as e:
                    logger.debug(f"Error processing file '{mod_file.filename}': {str(e)}")
            
            commit_info['modified_files'] = modified_files_info
        
        # Add DMM metrics if available
        try:
            commit_info['dmm_unit_size'] = commit.dmm_unit_size
            commit_info['dmm_unit_complexity'] = commit.dmm_unit_complexity 
            commit_info['dmm_unit_interfacing'] = commit.dmm_unit_interfacing
        except:
            commit_info['dmm_unit_size'] = None
            commit_info['dmm_unit_complexity'] = None
            commit_info['dmm_unit_interfacing'] = None
        
        return commit_info
    except RecursionError:
        logger.error(f"Failed to process commit {commit.hash} with RecursionError")
        return {
            'hash': commit.hash,
            'error': 'RecursionError',
            'author_date': commit.author_date,
            'msg': commit.msg[:100] + '...' if len(commit.msg) > 100 else commit.msg
        }
    except Exception as e:
        error_msg = str(e)
        # Special handling for Git submodule issues
        if "No option 'path' in section: 'submodule" in error_msg:
            logger.warning(f"Git submodule configuration issue in commit {commit.hash}, skipping detailed extraction")
            return {
                'hash': commit.hash,
                'author_date': commit.author_date,
                'msg': commit.msg[:100] + '...' if len(commit.msg) > 100 else commit.msg,
                'error': 'Git submodule configuration issue'
            }
        # Original error handling
        logger.error(f"Error extracting commit info for {commit.hash}: {error_msg}")
        return {
            'hash': commit.hash,
            'error': error_msg,
            'author_date': commit.author_date
        }

# Helper function to safely get filename from path
def safe_basename(file_path):
    if file_path is None:
        return "Unknown file"
    try:
        return os.path.basename(file_path)
    except (TypeError, AttributeError):
        return str(file_path)

# Helper function to get repository date range
def get_repo_date_range(repo_url, temp_dir):
    """Get the min and max dates from a repository's commits."""
    try:
        repository = Repository(path_to_repo=repo_url, clone_repo_to=temp_dir)
        min_date = None
        max_date = None
        
        for commit in repository.traverse_commits():
            if min_date is None or commit.author_date < min_date:
                min_date = commit.author_date
            if max_date is None or commit.author_date > max_date:
                max_date = commit.author_date
        
        return min_date, max_date
    except Exception as e:
        logger.debug(f"Error getting repository date range: {str(e)}")
        # Default to 1 year ago if we can't determine the range
        now = datetime.now()
        return now - timedelta(days=365), now

# Helper function to extract organization name from the repository URL
def extract_org_from_url(repo_url):
    """
    Extract the organization/user name from a GitHub repository URL.
    
    Args:
        repo_url (str): GitHub repository URL
        
    Returns:
        str: Organization/user name or None if not found
    """
    try:
        # Handle URLs in the format https://github.com/organization/repo
        if "github.com" in repo_url:
            parts = repo_url.split('/')
            if len(parts) >= 4:  # At minimum: https://github.com/org/repo
                return parts[3]  # Return the organization name
    except Exception as e:
        logger.debug(f"Error extracting organization from {repo_url}: {str(e)}")
    
    return None

def clean_csv_bom(csv_path):
    import csv
    
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        # Clean field names if BOM is present
        fieldnames = [name.replace('\ufeff', '') for name in reader.fieldnames]
        
        # Read all rows with corrected field names
        cleaned_rows = []
        for row in reader:
            cleaned_row = {}
            for i, old_key in enumerate(reader.fieldnames):
                # Map the old key to the cleaned field name
                cleaned_key = fieldnames[i]
                cleaned_row[cleaned_key] = row[old_key]
            cleaned_rows.append(cleaned_row)
            
    return cleaned_rows, fieldnames