from ...logger import get_logger
from datetime import datetime, timedelta
from collections import defaultdict
import statistics
from pydriller import ModificationType
import re
from ..base import BaseMetric

logger = get_logger(__name__)

class EnhancedCodeChurn(BaseMetric):
    """
    Code churn calculator that tracks both types of churn metrics as shown in PyDriller:
    - Total churn: added_lines + deleted_lines
    - Net churn: added_lines - deleted_lines
    
    Provides per-file, per-commit, and weekly aggregation capabilities.
    
    Also includes a "true code churn" metric based on the approach by
    Francis Laclé and Jonathan Guerne (https://github.com/flacle/truegitcodechurn)
    which defines true churn as "when an engineer rewrites their own code in a short time period"
    """
    
    def __init__(self):
        super().__init__()
        self.commit_data = {}
        self.file_data = defaultdict(list)
        self.weekly_data = defaultdict(lambda: {"files": {}, "commits": []})
        self.line_history = {}
        self.true_churn_metrics = {
            "total_contribution": 0,
            "total_churn": 0,
            "per_author": defaultdict(lambda: {"contribution": 0, "churn": 0}),
            "per_file": defaultdict(lambda: {"contribution": 0, "churn": 0})
        }
        self.code_churn_by_file = {}
        self.lines_added_by_file = {}
        self.lines_removed_by_file = {}

    def process_commit(self, commit):
        """Process a single commit and update metrics"""
        commit_hash = commit.hash
        commit_date = commit.committer_date
        author = commit.author.name
        
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file.new_path or modified_file.filename, 
                                       modified_file, author, commit_date, commit_hash)
        return self
            
    def process_modified_file(self, filename, modified_file, author_name, commit_date, commit_hash=None):
        """Process a single modified file and update metrics"""
        week_key = self._get_week_key(commit_date)
        # Don't try to access commit attribute directly from modified_file
        # Use the commit_hash parameter instead
        added_lines = modified_file.added_lines
        removed_lines = modified_file.deleted_lines
        
        # Update simple code churn metrics
        if filename not in self.code_churn_by_file:
            self.code_churn_by_file[filename] = 0
            self.lines_added_by_file[filename] = 0
            self.lines_removed_by_file[filename] = 0
            
        self.code_churn_by_file[filename] += added_lines + removed_lines
        self.lines_added_by_file[filename] += added_lines
        self.lines_removed_by_file[filename] += removed_lines
            
        # Update detailed tracking data
        if commit_hash not in self.commit_data:
            self.commit_data[commit_hash] = {}
            
        self.commit_data[commit_hash][filename] = {
            "timestamp": commit_date,
            "author": author_name,
            "added": added_lines,
            "removed": removed_lines
        }
            
        self.file_data[filename].append({
            "commit": commit_hash,
            "timestamp": commit_date,
            "author": author_name,
            "added": added_lines,
            "removed": removed_lines
        })
            
        if filename not in self.weekly_data[week_key]["files"]:
            self.weekly_data[week_key]["files"][filename] = {
                "added": 0,
                "removed": 0,
                "changes": []
            }
            
        self.weekly_data[week_key]["files"][filename]["added"] += added_lines
        self.weekly_data[week_key]["files"][filename]["removed"] += removed_lines
        self.weekly_data[week_key]["files"][filename]["changes"].append({
            "total_churn": added_lines + removed_lines,
            "net_churn": added_lines - removed_lines
        })
            
        if hasattr(modified_file, 'diff') and modified_file.diff:
            self._process_true_churn(filename, author_name, commit_date, modified_file)
            
        return self
    
    def get_metrics(self):
        """Get the calculated metrics"""
        metrics = calculate_code_churn_metrics(
            self.code_churn_by_file,
            self.lines_added_by_file,
            self.lines_removed_by_file
        )
        
        # Include true churn metrics in the output
        metrics["true_churn"] = self.get_true_churn_metrics()
        
        return metrics
    
    @staticmethod
    def merge_metrics(metrics_list):
        """Merge multiple metrics results into one"""
        merged = merge_code_churn_metrics(metrics_list)
        
        # Merge true churn metrics if present
        true_churn_metrics = [m.get("true_churn", {}) for m in metrics_list if "true_churn" in m]
        
        if true_churn_metrics:
            # Start with empty structure
            merged_true_churn = {
                "overall": {
                    "contribution": 0,
                    "churn": 0
                },
                "per_author": {},
                "per_file": {}
            }
            
            # Merge overall values
            for m in true_churn_metrics:
                if "overall" in m:
                    merged_true_churn["overall"]["contribution"] += m["overall"].get("contribution", 0)
                    merged_true_churn["overall"]["churn"] += m["overall"].get("churn", 0)
                    
                # Merge per-author values
                if "per_author" in m:
                    for author, values in m["per_author"].items():
                        if author not in merged_true_churn["per_author"]:
                            merged_true_churn["per_author"][author] = {"contribution": 0, "churn": 0}
                        merged_true_churn["per_author"][author]["contribution"] += values.get("contribution", 0)
                        merged_true_churn["per_author"][author]["churn"] += values.get("churn", 0)
                        
                # Merge per-file values
                if "per_file" in m:
                    for file, values in m["per_file"].items():
                        if file not in merged_true_churn["per_file"]:
                            merged_true_churn["per_file"][file] = {"contribution": 0, "churn": 0}
                        merged_true_churn["per_file"][file]["contribution"] += values.get("contribution", 0)
                        merged_true_churn["per_file"][file]["churn"] += values.get("churn", 0)
            
            merged["true_churn"] = merged_true_churn
            
        return merged
        
    def process_commits(self, commits):
        for commit in commits:
            self._process_commit(commit)
        return self

    def _process_commit(self, commit):
        commit_hash = commit.hash
        commit_date = commit.committer_date
        author = commit.author.name
        week_key = self._get_week_key(commit_date)
        
        commit_info = {
            "hash": commit_hash,
            "timestamp": commit_date,
            "author": author,
            "files": {}
        }
        
        self.commit_data[commit_hash] = {}
        
        for modified_file in commit.modified_files:
            filename = modified_file.new_path
            added_lines = modified_file.added_lines
            removed_lines = modified_file.deleted_lines
            
            self.commit_data[commit_hash][filename] = {
                "timestamp": commit_date,
                "author": author,
                "added": added_lines,
                "removed": removed_lines
            }
            
            self.file_data[filename].append({
                "commit": commit_hash,
                "timestamp": commit_date,
                "author": author,
                "added": added_lines,
                "removed": removed_lines
            })

            commit_info["files"][filename] = {
                "added": added_lines,
                "removed": removed_lines
            }
            
            if filename not in self.weekly_data[week_key]["files"]:
                self.weekly_data[week_key]["files"][filename] = {
                    "added": 0,
                    "removed": 0,
                    "changes": []
                }
            
            self.weekly_data[week_key]["files"][filename]["added"] += added_lines
            self.weekly_data[week_key]["files"][filename]["removed"] += removed_lines
            self.weekly_data[week_key]["files"][filename]["changes"].append({
                "total_churn": added_lines + removed_lines,
                "net_churn": added_lines - removed_lines
            })
            
            if modified_file.diff:
                self._process_true_churn(filename, author, commit_date, modified_file)
        
        self.weekly_data[week_key]["commits"].append(commit_info)
        
        return self
    
    def _process_true_churn(self, filename, author, timestamp, modified_file):
        if filename not in self.line_history:
            self.line_history[filename] = {}
        
        # Parse the diff to extract line-level changes
        diff_sections = re.finditer(r'@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@', modified_file.diff)
        
        contribution = 0
        churn = 0
        
        for section in diff_sections:
            old_start = int(section.group(1))
            old_count = int(section.group(2)) if section.group(2) else 1
            new_start = int(section.group(3))
            new_count = int(section.group(4)) if section.group(4) else 1
            
            section_start = section.end()
            next_section = modified_file.diff.find("@@", section_start)
            if next_section == -1:
                section_content = modified_file.diff[section_start:]
            else:
                section_content = modified_file.diff[section_start:next_section]
            
            lines = section_content.split('\n')
            old_line = old_start
            new_line = new_start
            
            for line in lines:
                if not line:
                    continue
                
                if line.startswith('-'):
                    line_content = line[1:]
                    line_key = old_line
                    
                    if line_key in self.line_history[filename]:
                        if self.line_history[filename][line_key]["author"] == author:
                            churn += 1
                        del self.line_history[filename][line_key]
                    
                    old_line += 1
                
                elif line.startswith('+'):
                    line_content = line[1:]
                    line_key = new_line
                    
                    self.line_history[filename][line_key] = {
                        "author": author,
                        "timestamp": timestamp,
                        "content": line_content
                    }
                    contribution += 1
                    new_line += 1
                
                else:
                    old_line += 1
                    new_line += 1
        
        # Update the true churn metrics
        self.true_churn_metrics["total_contribution"] += contribution
        self.true_churn_metrics["total_churn"] += churn
        self.true_churn_metrics["per_author"][author]["contribution"] += contribution
        self.true_churn_metrics["per_author"][author]["churn"] += churn
        self.true_churn_metrics["per_file"][filename]["contribution"] += contribution
        self.true_churn_metrics["per_file"][filename]["churn"] += churn

    def _get_week_key(self, timestamp):
        start_of_week = timestamp - timedelta(days=timestamp.weekday())
        start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_of_week.strftime("%Y-%m-%d")

    def get_metrics_per_file(self):
        metrics = {}
        
        for filename, changes in self.file_data.items():
            total_churns = [change["added"] + change["removed"] for change in changes]
            net_churns = [change["added"] - change["removed"] for change in changes]
            added = sum(change["added"] for change in changes)
            removed = sum(change["removed"] for change in changes)
            
            metrics[filename] = {
                "total_churn": {
                    "count": sum(total_churns),
                    "max": max(total_churns) if total_churns else 0,
                    "avg": round(statistics.mean(total_churns)) if total_churns else 0
                },
                "net_churn": {
                    "count": sum(net_churns),
                    "max": max(net_churns) if net_churns else 0,
                    "avg": round(statistics.mean(net_churns)) if net_churns else 0
                },
                "added_removed": {
                    "added": added,
                    "removed": removed
                }
            }
            
            if filename in self.true_churn_metrics["per_file"]:
                metrics[filename]["true_churn"] = {
                    "contribution": self.true_churn_metrics["per_file"][filename]["contribution"],
                    "churn": self.true_churn_metrics["per_file"][filename]["churn"]
                }
        
        return metrics

    def get_metrics_per_commit(self):
        """Get code churn metrics for each commit across all files"""
        metrics = {}
        
        for commit_hash, files in self.commit_data.items():
            total_churn = 0
            net_churn = 0
            total_added = 0
            total_removed = 0
            file_metrics = {}
            
            for filename, data in files.items():
                added = data["added"]
                removed = data["removed"]
                file_total_churn = added + removed
                file_net_churn = added - removed
                
                total_churn += file_total_churn
                net_churn += file_net_churn
                total_added += added
                total_removed += removed
                
                file_metrics[filename] = {
                    "total_churn": file_total_churn,
                    "net_churn": file_net_churn,
                    "added": added,
                    "removed": removed
                }
            
            metrics[commit_hash] = {
                "total_churn": total_churn,
                "net_churn": net_churn,
                "added_removed": {
                    "added": total_added,
                    "removed": total_removed
                },
                "files": file_metrics,
                "timestamp": next(iter(files.values()))["timestamp"] if files else None,
                "author": next(iter(files.values()))["author"] if files else None
            }
        
        return metrics

    def get_weekly_metrics(self):
        """Get aggregated code churn metrics by week"""
        weekly_metrics = {}
        
        for week_key, data in self.weekly_data.items():
            file_metrics = {}
            
            for filename, file_data in data["files"].items():
                total_churns = [change["total_churn"] for change in file_data["changes"]]
                net_churns = [change["net_churn"] for change in file_data["changes"]]
                
                file_metrics[filename] = {
                    "total_churn": {
                        "count": sum(total_churns),
                        "max": max(total_churns) if total_churns else 0,
                        "avg": round(statistics.mean(total_churns)) if total_churns else 0
                    },
                    "net_churn": {
                        "count": sum(net_churns),
                        "max": max(net_churns) if net_churns else 0,
                        "avg": round(statistics.mean(net_churns)) if net_churns else 0
                    },
                    "added_removed": {
                        "added": file_data["added"],
                        "removed": file_data["removed"]
                    }
                }
            
            # Overall metrics for this week
            all_total_churns = []
            all_net_churns = []
            total_added = 0
            total_removed = 0
            
            for file_data in data["files"].values():
                total_added += file_data["added"]
                total_removed += file_data["removed"]
                all_total_churns.extend([c["total_churn"] for c in file_data["changes"]])
                all_net_churns.extend([c["net_churn"] for c in file_data["changes"]])
            
            weekly_metrics[week_key] = {
                "total_churn": {
                    "count": sum(all_total_churns),
                    "max": max(all_total_churns) if all_total_churns else 0,
                    "avg": round(statistics.mean(all_total_churns)) if all_total_churns else 0
                },
                "net_churn": {
                    "count": sum(all_net_churns),
                    "max": max(all_net_churns) if all_net_churns else 0,
                    "avg": round(statistics.mean(all_net_churns)) if all_net_churns else 0
                },
                "added_removed": {
                    "added": total_added,
                    "removed": total_removed
                },
                "files": file_metrics,
                "commit_count": len(data["commits"])
            }
        
        return weekly_metrics
    
    def get_true_churn_metrics(self):
        """
        Get true code churn metrics based on line-level tracking.
        
        This approach is based on Francis Laclé and Jonathan Guerne's implementation
        (https://github.com/flacle/truegitcodechurn) which defines true churn as
        "when an engineer rewrites their own code in a short time period"
        """
        return {
            "overall": {
                "contribution": self.true_churn_metrics["total_contribution"],
                "churn": self.true_churn_metrics["total_churn"]
            },
            "per_author": dict(self.true_churn_metrics["per_author"]),
            "per_file": dict(self.true_churn_metrics["per_file"])
        }

    def get_metrics_per_author(self):
        """Get code churn metrics aggregated by author"""
        author_metrics = defaultdict(lambda: {
            "total_churn": 0,
            "net_churn": 0,
            "added": 0,
            "removed": 0,
            "file_count": 0,
            "commit_count": 0,
            "true_churn": {
                "contribution": 0,
                "churn": 0
            }
        })
        
        for commit_hash, files in self.commit_data.items():
            for filename, data in files.items():
                author = data["author"]
                added = data["added"]
                removed = data["removed"]
                
                author_metrics[author]["total_churn"] += added + removed
                author_metrics[author]["net_churn"] += added - removed
                author_metrics[author]["added"] += added
                author_metrics[author]["removed"] += removed
                author_metrics[author]["file_count"] += 1
                author_metrics[author]["commit_count"] += 1
        
        for author, metrics in self.true_churn_metrics["per_author"].items():
            if author in author_metrics:
                author_metrics[author]["true_churn"]["contribution"] = metrics["contribution"]
                author_metrics[author]["true_churn"]["churn"] = metrics["churn"]
        
        return dict(author_metrics)

def extract_code_churn(commits):
    code_churn = {}
    lines_added_by_file = {}
    lines_removed_by_file = {}
    
    for commit in commits:
        for modified_file in commit.modified_files:
            filename = modified_file.filename
            added = modified_file.added_lines
            removed = modified_file.deleted_lines
            
            # Initialize if needed
            if filename not in code_churn:
                code_churn[filename] = 0
                lines_added_by_file[filename] = 0
                lines_removed_by_file[filename] = 0
            
            # Update metrics
            code_churn[filename] += added + removed  # Total churn
            lines_added_by_file[filename] += added
            lines_removed_by_file[filename] += removed
    
    return code_churn, lines_added_by_file, lines_removed_by_file

def calculate_code_churn_metrics(code_churn, lines_added, lines_removed):
    if not code_churn:
        return {
            "total_churn": {
                "count": 0,
                "max": 0,
                "avg": 0
            },
            "net_churn": {
                "count": 0,
                "max": 0,
                "avg": 0
            },
            "added_removed": {
                "added": 0,
                "removed": 0
            }
        }
    
    code_churn_total = code_churn

    code_churn_net = {}
    for filename in set(lines_added.keys()) | set(lines_removed.keys()):
        added = lines_added.get(filename, 0)
        removed = lines_removed.get(filename, 0)
        code_churn_net[filename] = added - removed
    
    total_churn_values = list(code_churn_total.values())
    net_churn_values = list(code_churn_net.values())
    
    return {
        "total_churn": {
            "count": sum(total_churn_values),
            "max": max(total_churn_values) if total_churn_values else 0,
            "avg": round(sum(total_churn_values) / len(total_churn_values)) if total_churn_values else 0
        },
        "net_churn": {
            "count": sum(net_churn_values),
            "max": max(net_churn_values) if net_churn_values else 0,
            "avg": round(sum(net_churn_values) / len(net_churn_values)) if net_churn_values else 0
        },
        "added_removed": {
            "added": sum(lines_added.values()),
            "removed": sum(lines_removed.values())
        }
    }

def merge_code_churn_metrics(metrics_list):
    if not metrics_list:
        return {
            "total_churn": {
                "count": 0,
                "max": 0,
                "avg": 0
            },
            "net_churn": {
                "count": 0,
                "max": 0,
                "avg": 0
            },
            "added_removed": {
                "added": 0,
                "removed": 0
            }
        }

    # Calculate all metrics
    total_count = sum(m.get("total_churn", {}).get("count", 0) for m in metrics_list)
    max_total = max(m.get("total_churn", {}).get("max", 0) for m in metrics_list)
    net_count = sum(m.get("net_churn", {}).get("count", 0) for m in metrics_list)
    max_net = max(m.get("net_churn", {}).get("max", 0) for m in metrics_list)
    
    # Calculate weighted average
    total_sum = sum((m.get("total_churn", {}).get("avg", 0) * m.get("_count", 1)) for m in metrics_list)
    net_sum = sum((m.get("net_churn", {}).get("avg", 0) * m.get("_count", 1)) for m in metrics_list)
    total_items = sum(m.get("_count", 1) for m in metrics_list)
    
    # Sum added and removed lines
    total_added = sum(m.get("added_removed", {}).get("added", 0) for m in metrics_list)
    total_removed = sum(m.get("added_removed", {}).get("removed", 0) for m in metrics_list)
    
    return {
        "total_churn": {
            "count": total_count,
            "max": max_total,
            "avg": round(total_sum / total_items) if total_items > 0 else 0
        },
        "net_churn": {
            "count": net_count,
            "max": max_net,
            "avg": round(net_sum / total_items) if total_items > 0 else 0
        },
        "added_removed": {
            "added": total_added,
            "removed": total_removed
        }
    }