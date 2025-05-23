# source/metrics/velocity/code_domain.py
from ...logger import get_logger
from ..productivity.base import BaseMetric
from collections import defaultdict
from datetime import timedelta
import re
import os

logger = get_logger(__name__)

class CodeDomainMetric(BaseMetric):
    """
    Classifies code changes by domain (frontend, backend, test, docs, etc.)
    to understand where developers spend their time.
    """
    
    def __init__(self):
        super().__init__()
        self.developer_stats = defaultdict(lambda: {
            'weekly_domains': defaultdict(lambda: defaultdict(int)),
            'total_by_domain': defaultdict(int)
        })
        
        # Domain classification rules
        self.domain_rules = {
            'frontend': {
                'extensions': {'.js', '.jsx', '.ts', '.tsx', '.vue', '.html', '.css', '.scss', '.sass', '.less'},
                'paths': {'frontend/', 'client/', 'src/components/', 'src/views/', 'public/', 'static/'},
                'files': {'webpack.config.js', 'vite.config.js', 'rollup.config.js'}
            },
            'backend': {
                'extensions': {'.py', '.java', '.go', '.rb', '.php', '.cs', '.rs'},
                'paths': {'backend/', 'server/', 'api/', 'src/controllers/', 'src/models/', 'src/services/'},
                'files': {'app.py', 'main.py', 'server.py', 'api.py'}
            },
            'database': {
                'extensions': {'.sql', '.prisma'},
                'paths': {'migrations/', 'db/', 'database/', 'schema/'},
                'files': {'schema.sql', 'migration.sql'}
            },
            'test': {
                'extensions': {'.test.js', '.spec.js', '.test.ts', '.spec.ts', '_test.py', '_test.go'},
                'paths': {'test/', 'tests/', '__tests__/', 'spec/'},
                'patterns': [r'test_.*\.py$', r'.*_test\.py$', r'.*\.test\.[jt]sx?$', r'.*\.spec\.[jt]sx?$']
            },
            'docs': {
                'extensions': {'.md', '.rst', '.txt', '.adoc'},
                'paths': {'docs/', 'documentation/'},
                'files': {'README.md', 'CHANGELOG.md', 'CONTRIBUTING.md', 'LICENSE'}
            },
            'config': {
                'extensions': {'.json', '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf'},
                'paths': {'config/', '.github/', '.circleci/'},
                'files': {'package.json', 'tsconfig.json', 'setup.cfg', 'pyproject.toml', 
                         'docker-compose.yml', 'Dockerfile', '.gitignore', '.env'}
            },
            'mobile': {
                'extensions': {'.swift', '.kt', '.dart', '.m', '.mm'},
                'paths': {'ios/', 'android/', 'mobile/'},
                'files': {'Info.plist', 'AndroidManifest.xml'}
            },
            'devops': {
                'extensions': {'.sh', '.bash', '.ps1'},
                'paths': {'.github/workflows/', 'scripts/', 'bin/', 'deploy/'},
                'files': {'Dockerfile', 'docker-compose.yml', 'Jenkinsfile', '.gitlab-ci.yml',
                         'azure-pipelines.yml', 'buildspec.yml'}
            }
        }
    
    def process_commit(self, commit):
        """Process commit for code domain classification."""
        developer_email = commit.author.email.strip().lower()
        commit_date = commit.committer_date
        week_key = self._get_week_key(commit_date)
        
        for modified_file in commit.modified_files:
            domain = self._classify_file_domain(modified_file.filename)
            changes = modified_file.added_lines + modified_file.deleted_lines
            
            if changes > 0:
                # Update weekly stats
                self.developer_stats[developer_email]['weekly_domains'][week_key][domain] += changes
                
                # Update total stats
                self.developer_stats[developer_email]['total_by_domain'][domain] += changes
        
        return self
    
    def process_modified_file(self, filename, modified_file, author_name, commit_date, commit_hash=None):
        """Not used - processing done at commit level."""
        return self
    
    def _classify_file_domain(self, filepath):
        """Classify a file into a domain based on its path and extension."""
        # Normalize path separators
        filepath = filepath.replace('\\', '/')
        filename = os.path.basename(filepath)
        _, ext = os.path.splitext(filename)
        
        # Check each domain's rules
        for domain, rules in self.domain_rules.items():
            # Check file extension
            if ext in rules.get('extensions', set()):
                return domain
            
            # Check specific files
            if filename in rules.get('files', set()):
                return domain
            
            # Check path patterns
            for path_pattern in rules.get('paths', set()):
                if path_pattern in filepath:
                    return domain
            
            # Check regex patterns
            for pattern in rules.get('patterns', []):
                if re.search(pattern, filepath):
                    return domain
        
        # Default to 'other' if no domain matches
        return 'other'
    
    def _get_week_key(self, date):
        """Get week key for date."""
        start_of_week = date - timedelta(days=date.weekday())
        return start_of_week.strftime('%Y-%m-%d')
    
    def get_metrics(self):
        """Get code domain metrics."""
        metrics = {}
        
        for developer, stats in self.developer_stats.items():
            metrics[developer] = {
                'total_by_domain': dict(stats['total_by_domain']),
                'weekly_domains': {}
            }
            
            # Calculate percentages for totals
            total_changes = sum(stats['total_by_domain'].values())
            if total_changes > 0:
                metrics[developer]['domain_percentages'] = {
                    domain: (count / total_changes) * 100
                    for domain, count in stats['total_by_domain'].items()
                }
            
            # Process weekly stats
            for week, domains in stats['weekly_domains'].items():
                week_total = sum(domains.values())
                if week_total > 0:
                    metrics[developer]['weekly_domains'][week] = {
                        'domains': dict(domains),
                        'total_changes': week_total,
                        'percentages': {
                            domain: (count / week_total) * 100
                            for domain, count in domains.items()
                        }
                    }
        
        return metrics
    
    @staticmethod
    def merge_metrics(metrics_list):
        """Merge multiple code domain metrics."""
        if not metrics_list:
            return {}
        
        merged = defaultdict(lambda: {
            'total_by_domain': defaultdict(int),
            'weekly_domains': defaultdict(lambda: defaultdict(int))
        })
        
        # Merge raw counts
        for metrics in metrics_list:
            for developer, dev_stats in metrics.items():
                # Merge total domain counts
                for domain, count in dev_stats.get('total_by_domain', {}).items():
                    merged[developer]['total_by_domain'][domain] += count
                
                # Merge weekly domain counts
                for week, week_data in dev_stats.get('weekly_domains', {}).items():
                    for domain, count in week_data.get('domains', {}).items():
                        merged[developer]['weekly_domains'][week][domain] += count
        
        # Calculate percentages after merging
        result = {}
        for developer, dev_stats in merged.items():
            result[developer] = {
                'total_by_domain': dict(dev_stats['total_by_domain']),
                'weekly_domains': {}
            }
            
            # Calculate total percentages
            total_changes = sum(dev_stats['total_by_domain'].values())
            if total_changes > 0:
                result[developer]['domain_percentages'] = {
                    domain: (count / total_changes) * 100
                    for domain, count in dev_stats['total_by_domain'].items()
                }
            
            # Calculate weekly percentages
            for week, domains in dev_stats['weekly_domains'].items():
                week_total = sum(domains.values())
                if week_total > 0:
                    result[developer]['weekly_domains'][week] = {
                        'domains': dict(domains),
                        'total_changes': week_total,
                        'percentages': {
                            domain: (count / week_total) * 100
                            for domain, count in domains.items()
                        }
                    }
        
        return result