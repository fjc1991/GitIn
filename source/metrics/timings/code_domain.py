# source/metrics/velocity/code_domain.py
from ...logger import get_logger
from ..base import BaseMetric
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
        
        # Comprehensive domain classification rules based on GitClear's supported languages
        self.domain_rules = {
            'frontend': {
                'extensions': {
                    # JavaScript/TypeScript ecosystem
                    '.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs',
                    # Web markup and styling
                    '.html', '.htm', '.xhtml', '.css', '.scss', '.sass', '.less', '.styl',
                    # Frontend frameworks
                    '.vue', '.svelte', '.astro',
                    # Template engines
                    '.hbs', '.handlebars', '.mustache', '.ejs', '.pug', '.jade',
                    # Frontend build artifacts
                    '.map'
                },
                'paths': {
                    'frontend/', 'client/', 'web/', 'www/', 'public/', 'static/', 'assets/',
                    'src/components/', 'src/views/', 'src/pages/', 'src/layouts/',
                    'components/', 'views/', 'pages/', 'layouts/',
                    'styles/', 'css/', 'scss/', 'sass/'
                },
                'files': {
                    'webpack.config.js', 'vite.config.js', 'rollup.config.js', 'parcel.config.js',
                    'nuxt.config.js', 'next.config.js', 'gatsby.config.js', 'svelte.config.js',
                    '.babelrc', 'babel.config.js', 'postcss.config.js', 'tailwind.config.js'
                }
            },
            'backend': {
                'extensions': {
                    # Python
                    '.py', '.pyw', '.pyx', '.pxd', '.pxi',
                    # Java ecosystem
                    '.java', '.scala', '.kt', '.kts', '.groovy', '.gradle',
                    # .NET ecosystem
                    '.cs', '.vb', '.fs', '.fsx', '.fsi',
                    # Go
                    '.go',
                    # Ruby
                    '.rb', '.rake', '.gemspec',
                    # PHP
                    '.php', '.php3', '.php4', '.php5', '.phtml',
                    # Rust
                    '.rs',
                    # C/C++
                    '.c', '.cpp', '.cxx', '.cc', '.c++', '.h', '.hpp', '.hxx', '.hh',
                    # Objective-C
                    '.m', '.mm',
                    # Perl
                    '.pl', '.pm', '.perl',
                    # Lua
                    '.lua',
                    # Elixir
                    '.ex', '.exs',
                    # Erlang
                    '.erl', '.hrl',
                    # Haskell
                    '.hs', '.lhs',
                    # Clojure
                    '.clj', '.cljs', '.cljc', '.edn',
                    # F#
                    '.fs', '.fsi', '.fsx',
                    # OCaml
                    '.ml', '.mli',
                    # Crystal
                    '.cr',
                    # Nim
                    '.nim', '.nims',
                    # Zig
                    '.zig'
                },
                'paths': {
                    'backend/', 'server/', 'api/', 'service/', 'services/',
                    'src/controllers/', 'src/models/', 'src/services/', 'src/handlers/',
                    'controllers/', 'models/', 'handlers/', 'middleware/',
                    'lib/', 'libs/', 'pkg/', 'packages/'
                },
                'files': {
                    'app.py', 'main.py', 'server.py', 'api.py', 'wsgi.py', 'asgi.py',
                    'main.go', 'server.go', 'main.java', 'Application.java',
                    'Program.cs', 'Startup.cs', 'app.rb', 'server.rb',
                    'main.rs', 'lib.rs', 'mod.rs', 'index.php'
                }
            },
            'database': {
                'extensions': {
                    '.sql', '.mysql', '.psql', '.sqlite', '.db',
                    # NoSQL and other databases
                    '.cql', '.cypher', '.sparql',
                    # Schema and migration files
                    '.prisma', '.dbml'
                },
                'paths': {
                    'migrations/', 'db/', 'database/', 'schema/', 'sql/',
                    'src/migrations/', 'src/db/', 'data/', 'fixtures/'
                },
                'files': {
                    'schema.sql', 'migration.sql', 'seed.sql', 'init.sql',
                    'schema.prisma', 'database.sqlite', 'db.sqlite3'
                }
            },
            'test': {
                'extensions': {
                    # JavaScript/TypeScript test files
                    '.test.js', '.spec.js', '.test.ts', '.spec.ts',
                    '.test.jsx', '.spec.jsx', '.test.tsx', '.spec.tsx',
                    # Python test files
                    '.test.py', '_test.py',
                    # Java test files
                    '.test.java', 'Test.java', 'Tests.java',
                    # Go test files
                    '_test.go',
                    # Ruby test files
                    '.test.rb', '_test.rb', '_spec.rb',
                    # C# test files
                    '.test.cs', 'Test.cs', 'Tests.cs',
                    # Other test extensions
                    '.feature', '.story'
                },
                'paths': {
                    'test/', 'tests/', '__tests__/', 'spec/', 'specs/',
                    'src/test/', 'src/tests/', 'src/__tests__/', 'src/spec/',
                    'testing/', 'e2e/', 'integration/', 'unit/',
                    'cypress/', 'playwright/', '__mocks__/', 'fixtures/'
                },
                'patterns': [
                    r'test_.*\.py$', r'.*_test\.py$', r'.*_test\.go$',
                    r'.*\.test\.[jt]sx?$', r'.*\.spec\.[jt]sx?$',
                    r'.*Test\.java$', r'.*Tests\.java$',
                    r'.*Test\.cs$', r'.*Tests\.cs$',
                    r'.*_spec\.rb$', r'.*_test\.rb$'
                ],
                'files': {
                    'jest.config.js', 'vitest.config.js', 'karma.conf.js',
                    'cypress.config.js', 'playwright.config.js',
                    'pytest.ini', 'tox.ini', 'conftest.py'
                }
            },
            'docs': {
                'extensions': {
                    '.md', '.markdown', '.rst', '.txt', '.adoc', '.asciidoc',
                    '.org', '.tex', '.wiki', '.mediawiki'
                },
                'paths': {
                    'docs/', 'doc/', 'documentation/', 'wiki/',
                    'guides/', 'manual/', 'help/'
                },
                'files': {
                    'README.md', 'CHANGELOG.md', 'CONTRIBUTING.md', 'LICENSE',
                    'CODE_OF_CONDUCT.md', 'SECURITY.md', 'AUTHORS', 'COPYING',
                    'INSTALL.md', 'USAGE.md', 'API.md'
                }
            },
            'config': {
                'extensions': {
                    '.json', '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf',
                    '.xml', '.plist', '.properties', '.env', '.envrc',
                    '.lock', '.sum', '.mod'
                },
                'paths': {
                    'config/', 'configs/', 'configuration/', '.github/', '.circleci/',
                    '.gitlab/', 'ci/', 'deploy/', 'deployment/'
                },
                'files': {
                    'package.json', 'package-lock.json', 'yarn.lock', 'pnpm-lock.yaml',
                    'tsconfig.json', 'jsconfig.json', 'setup.cfg', 'setup.py',
                    'pyproject.toml', 'requirements.txt', 'Pipfile', 'Pipfile.lock',
                    'pom.xml', 'build.gradle', 'build.gradle.kts', 'settings.gradle',
                    'Cargo.toml', 'Cargo.lock', 'go.mod', 'go.sum',
                    'composer.json', 'composer.lock', 'Gemfile', 'Gemfile.lock',
                    '.gitignore', '.gitattributes', '.editorconfig', '.prettierrc',
                    '.eslintrc', '.stylelintrc', 'docker-compose.yml', 'Dockerfile',
                    'Makefile', 'CMakeLists.txt', '.env', '.env.example'
                }
            },
            'mobile': {
                'extensions': {
                    # iOS
                    '.swift', '.m', '.mm', '.h',
                    # Android
                    '.kt', '.kts', '.java',
                    # Cross-platform
                    '.dart', '.xaml',
                    # React Native
                    '.tsx', '.jsx', '.js', '.ts'
                },
                'paths': {
                    'ios/', 'android/', 'mobile/', 'app/',
                    'src/ios/', 'src/android/', 'platforms/',
                    'lib/ios/', 'lib/android/'
                },
                'files': {
                    'Info.plist', 'AndroidManifest.xml', 'build.gradle',
                    'pubspec.yaml', 'Package.swift', 'project.pbxproj'
                }
            },
            'devops': {
                'extensions': {
                    '.sh', '.bash', '.zsh', '.fish', '.ps1', '.bat', '.cmd',
                    '.tf', '.tfvars', '.hcl',  # Terraform
                    '.jenkinsfile'
                },
                'paths': {
                    '.github/workflows/', 'scripts/', 'bin/', 'deploy/', 'deployment/',
                    'infrastructure/', 'terraform/', 'ansible/', 'k8s/', 'kubernetes/',
                    'helm/', 'charts/', 'docker/', '.circleci/', '.gitlab-ci/'
                },
                'files': {
                    'Dockerfile', 'docker-compose.yml', 'docker-compose.yaml',
                    'Jenkinsfile', '.gitlab-ci.yml', '.travis.yml',
                    'azure-pipelines.yml', 'buildspec.yml', 'appveyor.yml',
                    'Vagrantfile', 'ansible.cfg', 'inventory',
                    'main.tf', 'variables.tf', 'outputs.tf'
                }
            },
            'data_science': {
                'extensions': {
                    '.ipynb', '.py', '.r', '.R', '.rmd', '.Rmd',
                    '.jl', '.scala', '.m'  # Julia, Scala, MATLAB
                },
                'paths': {
                    'notebooks/', 'analysis/', 'data/', 'models/',
                    'experiments/', 'research/', 'analytics/'
                },
                'files': {
                    'requirements.txt', 'environment.yml', 'conda.yml'
                }
            },
            'machine_learning': {
                'extensions': {
                    '.py', '.ipynb', '.yaml', '.yml', '.json',
                    '.pkl', '.pickle', '.h5', '.pb', '.onnx'
                },
                'paths': {
                    'models/', 'ml/', 'ai/', 'training/', 'inference/',
                    'experiments/', 'pipelines/', 'features/'
                },
                'files': {
                    'model.py', 'train.py', 'inference.py', 'pipeline.py'
                }
            },
            'game_dev': {
                'extensions': {
                    '.cs', '.cpp', '.c', '.h', '.hpp',  # Unity, Unreal
                    '.gd', '.tres', '.tscn',  # Godot
                    '.lua', '.py', '.js'  # Scripting
                },
                'paths': {
                    'Assets/', 'Scripts/', 'Scenes/', 'Prefabs/',
                    'Content/', 'Source/', 'game/', 'engine/'
                },
                'files': {
                    'project.godot', 'Assembly-CSharp.csproj'
                }
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