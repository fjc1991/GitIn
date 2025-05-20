from logger import get_logger
logger = get_logger(__name__)

def calculate_code_churn_metrics(code_churn, lines_added, lines_removed):
    if not code_churn:
        return {
            "count": 0,
            "max": 0,
            "avg": 0,
            "added_removed": {
                "added": 0,
                "removed": 0
            }
        }
    
    churn_values = list(code_churn.values())
    
    return {
        "count": sum(churn_values),
        "max": max(churn_values) if churn_values else 0,
        "avg": sum(churn_values) / len(churn_values) if churn_values else 0,
        "added_removed": {
            "added": sum(lines_added.values()),
            "removed": sum(lines_removed.values())
        }
    }

def extract_code_churn(commits):
    code_churn = {}
    lines_added_by_file = {}
    lines_removed_by_file = {}
    
    for commit in commits:
        for modified_file in commit.modified_files:
            filename = modified_file.filename
            
            # Calculate file churn (added + deleted lines)
            file_churn = modified_file.added_lines + modified_file.deleted_lines
            
            # Update code churn
            if filename not in code_churn:
                code_churn[filename] = 0
            code_churn[filename] += file_churn
            
            # Update lines added
            if filename not in lines_added_by_file:
                lines_added_by_file[filename] = 0
            lines_added_by_file[filename] += modified_file.added_lines
            
            # Update lines removed
            if filename not in lines_removed_by_file:
                lines_removed_by_file[filename] = 0
            lines_removed_by_file[filename] += modified_file.deleted_lines
    
    return code_churn, lines_added_by_file, lines_removed_by_file

def merge_code_churn_metrics(metrics_list):
    if not metrics_list:
        return {
            "count": 0,
            "max": 0,
            "avg": 0,
            "added_removed": {
                "added": 0,
                "removed": 0
            }
        }
    
    # Calculate merged values
    total_count = sum(m.get("count", 0) for m in metrics_list)
    max_val = max(m.get("max", 0) for m in metrics_list)
    
    # Calculate weighted average
    total_sum = sum(m.get("avg", 0) * m.get("_count", 1) for m in metrics_list)
    total_items = sum(m.get("_count", 1) for m in metrics_list)
    
    # Calculate total added and removed
    total_added = sum(m.get("added_removed", {}).get("added", 0) for m in metrics_list)
    total_removed = sum(m.get("added_removed", {}).get("removed", 0) for m in metrics_list)
    
    return {
        "count": total_count,
        "max": max_val,
        "avg": total_sum / total_items if total_items > 0 else 0,
        "added_removed": {
            "added": total_added,
            "removed": total_removed
        }
    }