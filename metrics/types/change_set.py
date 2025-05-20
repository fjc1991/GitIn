from logger import get_logger
logger = get_logger(__name__)

def calculate_change_set_metrics(file_changes):
    if not file_changes:
        return {"max": 0, "avg": 0}
    
    change_set_values = list(file_changes.values())
    
    return {
        "max": max(change_set_values) if change_set_values else 0,
        "avg": sum(change_set_values) / len(change_set_values) if change_set_values else 0
    }

def extract_file_changes(commits):
    file_changes = {}
    
    for commit in commits:
        for modified_file in commit.modified_files:
            filename = modified_file.filename
            
            if filename not in file_changes:
                file_changes[filename] = 0
            
            file_changes[filename] += 1
    
    return file_changes

def merge_change_set_metrics(metrics_list):
    if not metrics_list:
        return {"max": 0, "avg": 0}
    
    max_val = max(m.get("max", 0) for m in metrics_list)
    # Calculating weighted average based on original data size
    total_sum = sum(m.get("avg", 0) * m.get("_count", 1) for m in metrics_list)
    total_count = sum(m.get("_count", 1) for m in metrics_list)
    
    return {
        "max": max_val,
        "avg": total_sum / total_count if total_count > 0 else 0
    }