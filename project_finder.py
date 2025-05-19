import os
import json
import glob

from utils import BASE_PROJECTS_PATH

# Set up logger for this module
from logger import get_logger
logger = get_logger(__name__)


def find_all_projects(folder_filter=None):
    """Find all projects and group repositories by organization."""
    # Same code as before, but we'll return a different structure
    projects_by_org = {}
    
    # Apply folder filter if specified
    if folder_filter:
        folder_pattern = os.path.join(BASE_PROJECTS_PATH, folder_filter)
    else:
        folder_pattern = os.path.join(BASE_PROJECTS_PATH, "*")
    
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