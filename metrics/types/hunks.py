from logger import get_logger
logger = get_logger(__name__)

def calculate_hunks_metrics(hunks_by_file):
    return hunks_by_file

def extract_hunks_data(commits):
    hunks_by_file = {}
    
    for commit in commits:
        for modified_file in commit.modified_files:
            filename = modified_file.filename
            
            if filename not in hunks_by_file:
                hunks_by_file[filename] = 0
            
            if hasattr(modified_file, 'diff_parsed') and modified_file.diff_parsed:
                hunks_by_file[filename] += len(modified_file.diff_parsed.get('added', [])) + \
                                        len(modified_file.diff_parsed.get('deleted', []))
    
    return hunks_by_file

def merge_hunks_metrics(metrics_list):
    if not metrics_list:
        return {}
    
    merged_metrics = {}
    
    # Combine file hunks counts from all metrics
    for metrics in metrics_list:
        for filename, count in metrics.items():
            if filename not in merged_metrics:
                merged_metrics[filename] = 0
            merged_metrics[filename] += count
    
    return merged_metrics