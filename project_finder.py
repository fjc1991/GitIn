import os
import csv
import json
import glob

from utils import LEGACY_PROJECTS_PATH
from logger import get_logger
logger = get_logger(__name__)


def find_all_projects(folder_filter=None, csv_path='github_repos.csv'):

    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found at {csv_path}")
        logger.warning(f"Falling back to legacy directory-based project finding")
        return find_projects_from_directory(folder_filter)
    
    projects_by_username = {}
    
    try:
        # Read the CSV file
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                username = row.get('username')
                repo_name = row.get('repo_name')
                url = row.get('url')
                
                if not username or not repo_name or not url:
                    logger.warning(f"Skipping incomplete row: {row}")
                    continue
                
                if folder_filter and not username.lower().startswith(folder_filter.lower()):
                    continue
                
                if username not in projects_by_username:
                    projects_by_username[username] = {
                        'ecosystem': 'github',
                        'repositories': []
                    }
            
                projects_by_username[username]['repositories'].append({
                    'repo_url': url,
                    'ecosystem': 'github',
                    'repo_category': 'user',
                    'repo_name': repo_name,
                    'username': username
                })
        
        total_repos = sum(len(project['repositories']) for project in projects_by_username.values())
        logger.info(f"Found {len(projects_by_username)} usernames with a total of {total_repos} repositories")
        
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_path}: {str(e)}")
        logger.debug(f"Falling back to directory-based project finding")
        
        # Fallback to the original directory-based method
        return find_projects_from_directory(folder_filter)
    
    return projects_by_username

def find_projects_from_directory(folder_filter=None):
    """
    Legacy method: Find all projects by parsing the directory structure.
    Only used as fallback if CSV processing fails.
    """
    projects_by_org = {}
    
    # Apply folder filter if specified
    if folder_filter:
        folder_pattern = os.path.join(LEGACY_PROJECTS_PATH, folder_filter)
    else:
        folder_pattern = os.path.join(LEGACY_PROJECTS_PATH, "*")
    
    logger.warning(f"Using legacy project path: {LEGACY_PROJECTS_PATH}")
    
    # Walk through all folders (with optional filter)
    for letter_folder in glob.glob(folder_pattern):
        if not os.path.isdir(letter_folder):
            continue
            
        # Look for project folders within each letter folder
        for project_folder in glob.glob(os.path.join(letter_folder, "*")):
            if not os.path.isdir(project_folder):
                continue
                
            project_name = os.path.basename(project_folder)
            
            # Look for categorized json file to determine category
            category_files = glob.glob(os.path.join(project_folder, "*_categorized.json"))
            
            if category_files:
                try:
                    with open(category_files[0], 'r') as f:
                        category_data = json.load(f)
                        
                    # Get the ecosystem name
                    ecosystem = category_data.get('ecosystem', 'uncategorized')
                    
                    # Initialize project in our structure if not exists
                    if project_name not in projects_by_org:
                        projects_by_org[project_name] = {
                            'ecosystem': ecosystem,
                            'repositories': []
                        }
                    
                    # Process core repos
                    if 'core_repos' in category_data and category_data['core_repos']:
                        for repo_url in category_data['core_repos']:
                            projects_by_org[project_name]['repositories'].append({
                                'repo_url': repo_url,
                                'ecosystem': ecosystem,
                                'repo_category': 'core',
                                'local_path': project_folder
                            })
                    
                    # Process organization repos
                    if 'organization_repos' in category_data and category_data['organization_repos']:
                        for org, repos in category_data['organization_repos'].items():
                            for repo_url in repos:
                                projects_by_org[project_name]['repositories'].append({
                                    'repo_url': repo_url,
                                    'ecosystem': ecosystem,
                                    'repo_category': 'organization',
                                    'local_path': project_folder
                                })
                    
                    # Process other repos
                    if 'other_repos' in category_data and category_data['other_repos']:
                        for repo_url in category_data['other_repos']:
                            projects_by_org[project_name]['repositories'].append({
                                'repo_url': repo_url,
                                'ecosystem': ecosystem,
                                'repo_category': 'other',
                                'local_path': project_folder
                            })
                    
                    # Report if no repos found
                    if not projects_by_org[project_name]['repositories']:
                        logger.debug(f"Warning: No repository URLs found for {project_name}")
                        del projects_by_org[project_name]  # Remove empty projects
                        
                except Exception as e:
                    logger.debug(f"Error processing category file for {project_name}: {str(e)}")
            else:
                logger.debug(f"No category file found for {project_name}")
    
    return projects_by_org