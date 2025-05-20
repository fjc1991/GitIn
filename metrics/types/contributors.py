from logger import get_logger
logger = get_logger(__name__)

def calculate_contributors_metrics(contributors_by_file, contributors_commit_count, commits_by_file):
    # Calculate total contributor count per file
    contributors_count = {filename: len(authors) for filename, authors in contributors_by_file.items()}
    
    # Calculate minor contributors (using 20% threshold)
    minor_contributors = {}
    for filename, author_commits in contributors_commit_count.items():
        if filename in commits_by_file and commits_by_file[filename] > 0:
            total_commits = commits_by_file[filename]
            threshold = max(1, total_commits * 0.2)
            minor_count = sum(1 for commits in author_commits.values() if commits < threshold)
            minor_contributors[filename] = minor_count
    
    return {
        "total": contributors_count,
        "minor": minor_contributors
    }

def extract_contributors_data(commits):
    contributors_by_file = {}
    contributors_commit_count = {}
    
    for commit in commits:
        author_name = commit.author.name
        
        for modified_file in commit.modified_files:
            filename = modified_file.filename
            
            # Update contributors by file
            if filename not in contributors_by_file:
                contributors_by_file[filename] = set()
            contributors_by_file[filename].add(author_name)
            
            # Update contributor commit counts
            if filename not in contributors_commit_count:
                contributors_commit_count[filename] = {}
            if author_name not in contributors_commit_count[filename]:
                contributors_commit_count[filename][author_name] = 0
            contributors_commit_count[filename][author_name] += 1
    
    return contributors_by_file, contributors_commit_count

def calculate_contributors_experience(contributors_by_file):
    return {filename: len(authors) for filename, authors in contributors_by_file.items()}

def merge_contributors_metrics(metrics_list):
    if not metrics_list:
        return {
            "total": {},
            "minor": {}
        }
    
    merged_total = {}
    merged_minor = {}
    
    # Merge total contributors
    for metrics in metrics_list:
        total_metrics = metrics.get("total", {})
        for filename, count in total_metrics.items():
            if filename not in merged_total:
                merged_total[filename] = 0
            # Take the maximum count as approximation when merging
            merged_total[filename] = max(merged_total[filename], count)
    
    # Merge minor contributors
    for metrics in metrics_list:
        minor_metrics = metrics.get("minor", {})
        for filename, count in minor_metrics.items():
            if filename not in merged_minor:
                merged_minor[filename] = 0
            # Sum minor contributors to approximate when merging
            merged_minor[filename] += count
    
    return {
        "total": merged_total,
        "minor": merged_minor
    }

def merge_contributors_experience(metrics_list):
    if not metrics_list:
        return {}
    
    merged_metrics = {}
    
    # Take the maximum value for each file when merging
    for metrics in metrics_list:
        for filename, value in metrics.items():
            if filename not in merged_metrics:
                merged_metrics[filename] = 0
            merged_metrics[filename] = max(merged_metrics[filename], value)
    
    return merged_metrics