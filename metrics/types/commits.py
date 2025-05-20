from logger import get_logger
logger = get_logger(__name__)

def calculate_commits_metrics(commits_by_file):
    return commits_by_file

def extract_commits_count(commits):
    commits_by_file = {}
    
    for commit in commits:
        for modified_file in commit.modified_files:
            filename = modified_file.filename
            
            if filename not in commits_by_file:
                commits_by_file[filename] = 0
            
            commits_by_file[filename] += 1
    
    return commits_by_file

def merge_commits_metrics(metrics_list):
    if not metrics_list:
        return {}
    
    merged_metrics = {}
    
    # Combine file commit counts from all metrics
    for metrics in metrics_list:
        for filename, count in metrics.items():
            if filename not in merged_metrics:
                merged_metrics[filename] = 0
            merged_metrics[filename] += count
    
    return merged_metrics