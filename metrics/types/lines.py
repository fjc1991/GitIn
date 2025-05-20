from logger import get_logger
logger = get_logger(__name__)

def calculate_lines_metrics(lines_added_by_file, lines_removed_by_file):
    return {
        "added": {
            "total": sum(lines_added_by_file.values()),
            "max": max(lines_added_by_file.values()) if lines_added_by_file else 0,
            "avg": sum(lines_added_by_file.values()) / len(lines_added_by_file) if lines_added_by_file else 0
        },
        "removed": {
            "total": sum(lines_removed_by_file.values()),
            "max": max(lines_removed_by_file.values()) if lines_removed_by_file else 0,
            "avg": sum(lines_removed_by_file.values()) / len(lines_removed_by_file) if lines_removed_by_file else 0
        }
    }

def extract_lines_data(commits):
    lines_added_by_file = {}
    lines_removed_by_file = {}
    
    for commit in commits:
        for modified_file in commit.modified_files:
            filename = modified_file.filename
            
            # Update lines added
            if filename not in lines_added_by_file:
                lines_added_by_file[filename] = 0
            lines_added_by_file[filename] += modified_file.added_lines
            
            # Update lines removed
            if filename not in lines_removed_by_file:
                lines_removed_by_file[filename] = 0
            lines_removed_by_file[filename] += modified_file.deleted_lines
    
    return lines_added_by_file, lines_removed_by_file

def merge_lines_metrics(metrics_list):
    if not metrics_list:
        return {
            "added": {
                "total": 0,
                "max": 0,
                "avg": 0
            },
            "removed": {
                "total": 0,
                "max": 0,
                "avg": 0
            }
        }
    
    # Initialize counters
    total_added = 0
    max_added = 0
    avg_added_sum = 0
    avg_added_count = 0
    
    total_removed = 0
    max_removed = 0
    avg_removed_sum = 0
    avg_removed_count = 0
    
    # Calculate merged values
    for metrics in metrics_list:
        added_metrics = metrics.get("added", {})
        removed_metrics = metrics.get("removed", {})
        
        # Added lines
        total_added += added_metrics.get("total", 0)
        max_added = max(max_added, added_metrics.get("max", 0))
        
        # For averages, use weighted approach
        count = added_metrics.get("_count", 1)
        avg_added_sum += added_metrics.get("avg", 0) * count
        avg_added_count += count
        
        # Removed lines
        total_removed += removed_metrics.get("total", 0)
        max_removed = max(max_removed, removed_metrics.get("max", 0))
        
        # For averages, use weighted approach
        count = removed_metrics.get("_count", 1)
        avg_removed_sum += removed_metrics.get("avg", 0) * count
        avg_removed_count += count
    
    return {
        "added": {
            "total": total_added,
            "max": max_added,
            "avg": avg_added_sum / avg_added_count if avg_added_count > 0 else 0
        },
        "removed": {
            "total": total_removed,
            "max": max_removed,
            "avg": avg_removed_sum / avg_removed_count if avg_removed_count > 0 else 0
        }
    }