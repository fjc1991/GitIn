from ...logger import get_logger
from ..productivity.base import BaseMetric
from .test_doc_pct import QualityCornerstonesMetric as BaseQualityMetric
import re
from collections import defaultdict
from datetime import datetime, timedelta
logger = get_logger(__name__)

class MeaningfulCodeMetric(BaseMetric):
    """
    This metric focuses on tracking meaningful lines of code for programming languages.
    It attempts to follow the rough guidelines found at the following GitClear URL: 
    https://www.gitclear.com/help/meaningful_code_line_change_definition
    """
    
    def __init__(self):
        super().__init__()
        self.base_metric = BaseQualityMetric()
        
        # Only track meaningful lines for programming language files (not tests/docs)
        self.meaningful_total_lines = 0
        self.file_stats = {}
        self.commit_times = {}
        
        self.unrealistic_commits = {
            'large_commits': 0,
            'rapid_large_commits': 0,
            'total': 0,
            'skipped_lines': 0
        }

        self.auto_generated = {
            'long_sequences': 0,
            'repeated_chars': 0,
            'repetitive_patterns': 0,
            'total': 0
        }
        
    def process_commit(self, commit):
        commit_hash = commit.hash
        unrealistic_type = self._is_unrealistic_commit(commit)

        if unrealistic_type:
            logger.debug(f"Marking unrealistic commit {commit_hash} of type: {unrealistic_type}")
            return self
        
        self.base_metric.process_commit(commit)
        
        for modified_file in commit.modified_files:
            self.process_meaningful_metrics(modified_file.filename, modified_file, 
                                    commit.author.name, commit.committer_date, 
                                    commit_hash)
        return self
    
    def _is_unrealistic_commit(self, commit):
        """Check if a commit is unrealistically large or fast, and categorize it"""
        total_changes = sum(
            (mf.added_lines or 0) + (mf.deleted_lines or 0) 
            for mf in commit.modified_files
        )
        
        self.unrealistic_commits['skipped_lines'] += total_changes
        
        if total_changes > 5000:
            self.unrealistic_commits['large_commits'] += 1
            self.unrealistic_commits['total'] += 1
            return "large_commit"

        author = commit.author.name
        commit_time = commit.committer_date
        
        if author in self.commit_times:
            last_commit_time = self.commit_times[author]
            time_diff = commit_time - last_commit_time
            
            if total_changes > 1000 and time_diff < timedelta(minutes=10):
                self.unrealistic_commits['rapid_large_commits'] += 1
                self.unrealistic_commits['total'] += 1
                return "rapid_large_commit"
        
        self.commit_times[author] = commit_time
        return None
    
    def _get_language_from_filename(self, filename):
        """Determine language from filename and extension"""
        ext_to_lang = {
            # Apex
            'apex': 'apex', 'cls': 'apex', 'trigger': 'apex',
            # Astro
            'astro': 'astro',
            # C/C++
            'c': 'c', 'h': 'c',
            'cpp': 'cpp', 'cxx': 'cpp', 'cc': 'cpp', 'c++': 'cpp', 
            'hpp': 'cpp', 'hxx': 'cpp', 'hh': 'cpp', 'h++': 'cpp',
            # C#
            'cs': 'csharp',
            'csx': 'csx',  # C# script
            'cshtml': 'cshtml',  # Razor
            # CSS/SCSS
            'css': 'css',
            'scss': 'scss', 'sass': 'scss',
            # Clojure
            'clj': 'clojure', 'cljs': 'clojurescript', 'cljc': 'clojure',
            'edn': 'clojure',
            # Dart
            'dart': 'dart',
            # Elm
            'elm': 'elm',
            # Erb (Embedded Ruby)
            'erb': 'erb', 'html.erb': 'erb',
            # Erlang
            'erl': 'erlang', 'hrl': 'erlang',
            # HAML
            'haml': 'haml',
            # Haskell
            'hs': 'haskell', 'lhs': 'haskell',
            # Go
            'go': 'go',
            # IDL
            'idl': 'idl', 'pro': 'idl',
            # Java
            'java': 'java',
            # JavaScript
            'js': 'javascript', 'mjs': 'javascript', 'cjs': 'javascript',
            'jsx': 'jsx',
            # Jelly (Jenkins)
            'jelly': 'jelly',
            # JSON
            'json': 'json', 'jsonc': 'json',
            # Markdown
            'md': 'markdown', 'markdown': 'markdown', 'mdown': 'markdown',
            'mkd': 'markdown', 'mkdown': 'markdown',
            # OCaml
            'ml': 'ocaml', 'mli': 'ocaml',
            # Objective-C
            'm': 'objc', 'mm': 'objc',
            # Pascal
            'pas': 'pascal', 'pp': 'pascal', 'inc': 'pascal',
            # PHP
            'php': 'php', 'phtml': 'php', 'php3': 'php', 'php4': 'php', 'php5': 'php',
            # Perl
            'pl': 'perl', 'pm': 'perl', 'perl': 'perl',
            # Python
            'py': 'python', 'pyw': 'python', 'pyi': 'python',
            # R
            'r': 'r', 'R': 'r', 'rmd': 'r', 'Rmd': 'r',
            # Ruby
            'rb': 'ruby', 'rbw': 'ruby',
            # Rust
            'rs': 'rust',
            # Scala
            'scala': 'scala', 'sc': 'scala',
            # Shell
            'sh': 'shell', 'bash': 'shell', 'zsh': 'shell', 'fish': 'shell',
            'ksh': 'shell', 'csh': 'shell', 'tcsh': 'shell',
            # SQL
            'sql': 'sql', 'psql': 'sql', 'mysql': 'sql',
            # Swift
            'swift': 'swift',
            # TypeScript
            'ts': 'typescript', 'tsx': 'tsx',
            # Visual Basic
            'vb': 'vb', 'vbs': 'vb', 'vba': 'vb',
            'vbhtml': 'vbhtml',
            # XML
            'xml': 'xml', 'xsl': 'xml', 'xslt': 'xml', 'xsd': 'xml',
            # YAML
            'yaml': 'yaml', 'yml': 'yaml',
        }
        
        if '.' not in filename:
            return 'unknown'
        
        ext = filename.split('.')[-1].lower()
        
        # Handle special cases like .html.erb
        if filename.endswith('.html.erb'):
            return 'erb'
        elif filename.endswith('.cshtml'):
            return 'cshtml'
        elif filename.endswith('.vbhtml'):
            return 'vbhtml'
        
        return ext_to_lang.get(ext, 'unknown')
    
    def process_meaningful_metrics(self, filename, modified_file, author_name, commit_date, commit_hash):
        """Process meaningful metrics only for programming language files (skip test/doc files)"""
        if filename not in self.file_stats:
            is_test = self.base_metric._is_test_file(filename)
            is_doc = self.base_metric._is_doc_file(filename)
            
            self.file_stats[filename] = {
                'is_test': is_test,
                'is_doc': is_doc,
                'language': self._get_language_from_filename(filename),
                'meaningful_lines': 0
            }
            
            # Skip processing for test or doc files - we only want programming language files
            if is_test or is_doc:
                return self
        else:
            # Skip if already identified as test or doc file
            if self.file_stats[filename]['is_test'] or self.file_stats[filename]['is_doc']:
                return self
        
        source_code = None
        meaningful_lines = 0

        try:
            source_code_content = modified_file.source_code
            if modified_file.source_code is None:
                logger.debug(f"Source code is None for {filename} in commit {commit_hash}. Treating as empty.")
                source_code = ""
            else:
                if isinstance(source_code_content, bytes):
                    try:
                        source_code = source_code_content.decode('utf-8', errors='replace')
                    except Exception as decode_err:
                        logger.error(f"Error decoding source code for {filename}: {str(decode_err)}")
                        source_code = ""
                else:
                    source_code = str(source_code_content)
        except Exception as e:
            logger.error(f"Error retrieving source code for {filename}: {str(e)}")
            source_code = ""
            
        if source_code:
            meaningful_lines = self._count_meaningful_lines(filename, source_code)
            self.meaningful_total_lines += meaningful_lines
            self.file_stats[filename]['meaningful_lines'] = meaningful_lines
            
        return self
    
    def process_modified_file(self, filename, modified_file, author_name, commit_date, commit_hash=None):
        return self.process_meaningful_metrics(filename, modified_file, author_name, commit_date, commit_hash)
    
    def _count_meaningful_lines(self, filename, source_code):
        if not source_code:
            return 0
            
        language = self._get_language_from_filename(filename)
        lines = source_code.split('\n')
        meaningful_count = 0
        
        for line in lines:
            if self._is_meaningful_line(line, language):
                meaningful_count += 1
                
        return meaningful_count
    
    def _is_meaningful_line(self, line, language):
        stripped = line.strip()
        
        if not stripped:
            return False
        
        if self._looks_auto_generated(stripped):
            return False
        
        if self._is_only_keywords_or_braces(stripped, language):
            return False
            
        return True
    
    def _looks_auto_generated(self, line):
        is_auto_generated = False
        
        long_alphanum_pattern = r'[a-zA-Z0-9]{20,}'
        if len(re.findall(long_alphanum_pattern, line)) > 0:
            cleaned = re.sub(long_alphanum_pattern, '', line)
            if len(cleaned.strip()) < len(line.strip()) * 0.3:
                self.auto_generated['long_sequences'] += 1
                self.auto_generated['total'] += 1
                is_auto_generated = True
        
        if not is_auto_generated and re.search(r'_{5,}|={5,}|-{5,}', line):
            self.auto_generated['repeated_chars'] += 1
            self.auto_generated['total'] += 1
            is_auto_generated = True

        if not is_auto_generated and len(line) > 200 and len(set(line.replace(' ', ''))) < 10:
            self.auto_generated['repetitive_patterns'] += 1
            self.auto_generated['total'] += 1
            is_auto_generated = True
            
        return is_auto_generated
    
    def _is_only_keywords_or_braces(self, line, language):
         # Language-specific keywords that when alone don't add meaning
        language_keywords = {
            'apex': {'else', 'catch', 'finally', 'try', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'astro': {'else', 'catch', 'finally', 'try', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue'},
            'c': {'else', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'cpp': {'else', 'catch', 'try', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'csharp': {'else', 'catch', 'finally', 'try', 'if', 'for', 'foreach', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'csx': {'else', 'catch', 'finally', 'try', 'if', 'for', 'foreach', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'css': {},
            'scss': {},
            'clojure': {'if', 'when', 'when-not', 'cond', 'case', 'try', 'catch', 'finally', 'do', 'loop', 'recur'},
            'clojurescript': {'if', 'when', 'when-not', 'cond', 'case', 'try', 'catch', 'finally', 'do', 'loop', 'recur'},
            'dart': {'else', 'catch', 'finally', 'try', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'elm': {'if', 'then', 'else', 'case', 'of'},
            'erlang': {'if', 'case', 'of', 'try', 'catch', 'after', 'end', 'receive'},
            'haskell': {'if', 'then', 'else', 'case', 'of', 'where', 'let', 'in', 'do'},
            'go': {'else', 'if', 'for', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'select', 'go', 'defer'},
            'idl': {'if', 'then', 'else', 'endif', 'for', 'endfor', 'while', 'endwhile', 'case', 'endcase', 'switch', 'endswitch'},
            'java': {'else', 'catch', 'finally', 'try', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'javascript': {'else', 'catch', 'finally', 'try', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'jsx': {'else', 'catch', 'finally', 'try', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'ocaml': {'if', 'then', 'else', 'match', 'with', 'try', 'begin', 'end', 'for', 'while', 'do', 'done'},
            'objc': {'else', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'pascal': {'if', 'then', 'else', 'case', 'of', 'for', 'to', 'do', 'while', 'repeat', 'until', 'try', 'except', 'finally', 'begin', 'end'},
            'php': {'else', 'elseif', 'catch', 'finally', 'try', 'if', 'for', 'foreach', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'perl': {'else', 'elsif', 'if', 'unless', 'for', 'foreach', 'while', 'until', 'return', 'last', 'next', 'do'},
            'python': {'pass', 'else:', 'elif:', 'except:', 'finally:', 'try:', 'if:', 'for:', 'while:', 'with:', 'def:', 'class:', 'return', 'break', 'continue'},
            'r': {'if', 'else', 'for', 'in', 'while', 'repeat', 'next', 'break', 'return'},
            'ruby': {'else', 'elsif', 'rescue', 'ensure', 'begin', 'end', 'if', 'unless', 'for', 'while', 'until', 'case', 'when', 'return', 'break', 'next'},
            'rust': {'else', 'if', 'for', 'while', 'loop', 'match', 'return', 'break', 'continue'},
            'scala': {'else', 'catch', 'finally', 'try', 'if', 'for', 'while', 'match', 'case', 'return', 'do'},
            'shell': {'if', 'then', 'else', 'elif', 'fi', 'for', 'do', 'done', 'while', 'until', 'case', 'esac', 'return', 'break', 'continue'},
            'sql': {'if', 'else', 'end', 'case', 'when', 'then', 'begin', 'return'},
            'swift': {'else', 'catch', 'try', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'typescript': {'else', 'catch', 'finally', 'try', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'tsx': {'else', 'catch', 'finally', 'try', 'if', 'for', 'while', 'switch', 'case:', 'default:', 'return', 'break', 'continue', 'do'},
            'vb': {'If', 'Then', 'Else', 'ElseIf', 'End If', 'Select', 'Case', 'End Select', 'For', 'Next', 'Do', 'Loop', 'While', 'Try', 'Catch', 'Finally', 'Return'},
        }

        brace_only_patterns = [
            r'^\s*[\{\}\(\)\[\]]\s*$',  # Single braces/parentheses
            r'^\s*[\{\}\(\)\[\]]\s*[;,]?\s*$',  # Braces with semicolon/comma
            r'^\s*\}\s*else\s*\{\s*$',  # } else {
            r'^\s*\}\s*catch\s*\{\s*$', # } catch {
            r'^\s*\}\s*finally\s*\{\s*$', # } finally {
            r'^\s*<\s*/?\s*[a-zA-Z][a-zA-Z0-9]*\s*>\s*$',  # Simple XML/HTML tags
        ]
        
        for pattern in brace_only_patterns:
            if re.match(pattern, line, re.IGNORECASE):
                return True
        
        keywords = language_keywords.get(language, set())
        line_clean = line.rstrip(':;').strip()
        if line_clean.lower() in {k.lower() for k in keywords}:
            return True
        
        if language == 'ruby' and line.strip() == 'end':
            return True
        elif language == 'pascal' and line.strip().lower() in {'begin', 'end'}:
            return True
        elif language in ['haskell', 'elm'] and line.strip() in {'where', 'let', 'in'}:
            return True
        elif language == 'xml' and re.match(r'^\s*<[^>]*>\s*$', line):
            return True
        elif language in ['yaml', 'json'] and line.strip() in {'{', '}', '[', ']', '-'}:
            return True
            
        return False
    
    def get_metrics(self):
        base_metrics = self.base_metric.get_metrics()
        total_lines = base_metrics["total"]["lines"]
        auto_generated_percent = (self.auto_generated['total'] / total_lines * 100) if total_lines > 0 else 0

        return {
            "total": {
                "files": base_metrics["total"]["files"],
                "lines": total_lines,
                "meaningful_lines": self.meaningful_total_lines,
                "meaningful_percent": (self.meaningful_total_lines / total_lines * 100) if total_lines > 0 else 0
            },
            "quality_score": base_metrics["quality_score"],
            "unrealistic_commits": self.unrealistic_commits,
            "auto_generated": {
                **self.auto_generated,
                "percent": auto_generated_percent
            }
        }

    @staticmethod
    def merge_metrics(metrics_list):
        if not metrics_list:
            return {
                "total": {"files": 0, "lines": 0, "meaningful_lines": 0, "meaningful_percent": 0},
                "quality_score": 0,
                "unrealistic_commits": {"large_commits": 0, "rapid_large_commits": 0, "total": 0, "skipped_lines": 0},
                "auto_generated": {"long_sequences": 0, "repeated_chars": 0, "repetitive_patterns": 0, "total": 0, "percent": 0}
            }

        total_files = sum(m.get("total", {}).get("files", 0) for m in metrics_list)
        total_lines = sum(m.get("total", {}).get("lines", 0) for m in metrics_list)
        total_meaningful_lines = sum(m.get("total", {}).get("meaningful_lines", 0) for m in metrics_list)

        total_large_commits = sum(m.get("unrealistic_commits", {}).get("large_commits", 0) for m in metrics_list)
        total_rapid_large_commits = sum(m.get("unrealistic_commits", {}).get("rapid_large_commits", 0) for m in metrics_list)
        total_unrealistic = sum(m.get("unrealistic_commits", {}).get("total", 0) for m in metrics_list)
        total_skipped_lines = sum(m.get("unrealistic_commits", {}).get("skipped_lines", 0) for m in metrics_list)

        total_long_sequences = sum(m.get("auto_generated", {}).get("long_sequences", 0) for m in metrics_list)
        total_repeated_chars = sum(m.get("auto_generated", {}).get("repeated_chars", 0) for m in metrics_list)
        total_repetitive_patterns = sum(m.get("auto_generated", {}).get("repetitive_patterns", 0) for m in metrics_list)
        total_auto_generated = sum(m.get("auto_generated", {}).get("total", 0) for m in metrics_list)

        auto_generated_percent = (total_auto_generated / total_lines * 100) if total_lines > 0 else 0

        return {
            "total": {
                "files": total_files,
                "lines": total_lines,
                "meaningful_lines": total_meaningful_lines,
                "meaningful_percent": (total_meaningful_lines / total_lines * 100) if total_lines > 0 else 0
            },
            "quality_score": 0,  # No longer averaging test/doc percent
            "unrealistic_commits": {
                "large_commits": total_large_commits,
                "rapid_large_commits": total_rapid_large_commits,
                "total": total_unrealistic,
                "skipped_lines": total_skipped_lines
            },
            "auto_generated": {
                "long_sequences": total_long_sequences,
                "repeated_chars": total_repeated_chars,
                "repetitive_patterns": total_repetitive_patterns,
                "total": total_auto_generated,
                "percent": auto_generated_percent
            }
        }