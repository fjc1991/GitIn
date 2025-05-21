from ...logger import get_logger
from ..productivity.base import BaseMetric
import re
from collections import defaultdict

logger = get_logger(__name__)

class QualityCornerstonesMetric(BaseMetric):
    """
    Track documentation and test coverage
    similar to GitClear's Quality Cornerstones graph.
    """
    
    def __init__(self):
        super().__init__()
        self.test_files = 0
        self.test_lines = 0
        self.doc_files = 0
        self.doc_lines = 0
        self.total_files = 0
        self.total_lines = 0
        self.file_stats = {}
        
    def process_commit(self, commit):
        commit_hash = commit.hash
        
        for modified_file in commit.modified_files:
            self.process_modified_file(modified_file.filename, modified_file, 
                                    commit.author.name, commit.committer_date, 
                                    commit_hash)
        return self
    
    def process_modified_file(self, filename, modified_file, author_name, commit_date, commit_hash):
        if filename not in self.file_stats:
            self.file_stats[filename] = {
                'is_test': self._is_test_file(filename),
                'is_doc': self._is_doc_file(filename),
                'lines': 0,
                'doc_lines': 0,
                'test_lines': 0
            }
            self.total_files += 1

            if self.file_stats[filename]['is_test']:
                self.test_files += 1
            if self.file_stats[filename]['is_doc']:
                self.doc_files += 1
        
        source_code = None
        source_lines = 0
        doc_line_count = 0

        try:
            source_code_content = modified_file.source_code
            if modified_file.source_code is None:
                logger.debug(f"Source code is None for {filename} in commit {commit_hash} (before decoding). Treating as empty.")
                source_code = ""

            else:
                if isinstance(source_code_content, bytes):
                    try:
                        source_code = source_code_content.decode('utf-8', errors='replace')
                    except Exception as decode_err:
                        logger.error(f"Unexpected error retrieving source code for {filename} (commit {commit_hash}): {str(e)}. Treating as empty.")
                        source_code = ""
                else:
                    source_code = str(source_code_content)

        except ValueError as e:
            # This catch is for pydriller's internal "SHA could not be resolved"
            logger.debug(f"Could not retrieve source code for {filename} (commit {commit_hash}): {str(e)}. Treating as empty.")
            source_code = ""
        except Exception as e:
            # Catch any other unexpected errors during source_code access
            logger.error(f"Unexpected error retrieving source code for {filename} (commit {commit_hash}): {str(e)}. Treating as empty.")
            source_code = ""
        
        # Ensure source_code is a string before line counting
        if not isinstance(source_code, str):
            logger.debug(f"Source code for {filename} (commit {commit_hash}) is not a string after retrieval attempts. Type: {type(source_code)}. Treating as empty.")
            source_code = ""
            
        if source_code: # Only count lines if source_code is not empty
            source_lines = source_code.count('\n') + 1
        else: # If source_code is empty (either initially or after error handling)
            source_lines = 0
            
        self.file_stats[filename]['lines'] = source_lines
        self.total_lines += source_lines
        
        if self.file_stats[filename]['is_test']:
            self.test_lines += source_lines
            self.file_stats[filename]['test_lines'] = source_lines
            
        if self.file_stats[filename]['is_doc']:
            self.doc_lines += source_lines
            self.file_stats[filename]['doc_lines'] = source_lines
        elif source_code: # Only count doc lines if there's source code
            doc_line_count = self._count_doc_lines(filename, source_code)
            self.doc_lines += doc_line_count
            self.file_stats[filename]['doc_lines'] = doc_line_count
        else: # No source code, so no doc lines from this file content
             self.file_stats[filename]['doc_lines'] = 0
            
        return self
    
    def _is_test_file(self, filename):
        # Common test file patterns identified with help from Claude :)
        test_patterns = [
            r'test_.*\.[a-zA-Z]+$',                  # test_file.ext
            r'.*_test\.[a-zA-Z]+$',                  # file_test.ext
            r'.*Tests?\.[a-zA-Z]+$',                 # FileTest.ext or FileTests.ext
            r'.*Spec\.[a-zA-Z]+$',                   # FileSpec.ext
            r'.*\.spec\.[a-zA-Z]+$',                 # file.spec.ext
            r'.*\.test\.[a-zA-Z]+$',                 # file.test.ext
            r'tests?/.*',                            # test/ or tests/ directory
            r'spec/.*',                              # spec/ directory
            r'__tests__/.*',                         # __tests__/ directory (common in React)
            r'__test__/.*',                          # __test__/ directory
            r'testing/.*',                           # testing/ directory
            r'.*_spec\.[a-zA-Z]+$',                  # file_spec.ext
            r'.*-test\.[a-zA-Z]+$',                  # file-test.ext
            r'.*-spec\.[a-zA-Z]+$',                  # file-spec.ext
            r'test[A-Z].*\.[a-zA-Z]+$',              # testFile.ext (camelCase)
            r'Test[A-Z].*\.[a-zA-Z]+$',              # TestFile.ext (PascalCase)
        ]
        return any(re.match(pattern, filename) for pattern in test_patterns)
    
    def _is_doc_file(self, filename):
        doc_patterns = [
            r'.*\.md$',                              # Markdown
            r'.*\.rst$',                             # reStructuredText
            r'.*\.txt$',                             # Plain text
            r'.*\.adoc$',                            # AsciiDoc
            r'.*\.asciidoc$',                        # AsciiDoc
            r'.*\.wiki$',                            # Wiki markup
            r'.*\.rdoc$',                            # RDoc
            r'.*\.pod$',                             # Perl POD
            r'docs?/.*',                             # doc/ or docs/ directory
            r'documentation/.*',                     # documentation/ directory
            r'README.*',                             # README files
            r'CHANGELOG.*',                          # Changelog files
            r'CONTRIBUTING.*',                       # Contributing guide
            r'LICENSE.*',                            # License file
            r'INSTALL.*',                            # Installation instructions
            r'USAGE.*',                              # Usage instructions
            r'FAQ.*',                                # FAQ
            r'HOWTO.*',                              # How-to guides
            r'MANUAL.*',                             # Manual
            r'TUTORIAL.*',                           # Tutorial
            r'GUIDE.*',                              # Guide
            r'DOC.*',                                # Documentation
            r'.*\.dox$',                             # Doxygen file
            r'.*\.javadoc$',                         # JavaDoc file
            r'.*\.jsdoc$',                           # JSDoc file
            r'.*\.apidoc$',                          # API Doc file
            r'.*\.man$',                             # Man page
            r'man/.*',                               # Man page directory
            r'.*\.html\..*$',                        # HTML doc
            r'.*\.pdf$',                             # PDF doc
            r'.*\.epub$',                            # EPUB doc
            r'.*\.tex$',                             # LaTeX
            r'.*\.docx?$',                           # MS Word
            r'wikis?/.*',                            # Wiki directory
            r'site/.*',                              # Site directory (often docs)
            r'api-docs?/.*',                         # API docs directory
            r'api/.*',                               # API directory (often contains docs)
        ]
        return any(re.match(pattern, filename) for pattern in doc_patterns)
    
    def _count_doc_lines(self, filename, source_code):
        # Language-specific comment patterns by file extension
        single_line_comments = {
            # A-C
            'ada': '--',
            'adb': '--',
            'applescript': '--',
            'as': '//',
            'asm': ';',
            'asp': "'",
            'aspx': "'",
            'au3': ';',
            'bas': "'",
            'bat': 'REM',
            'c': '//',
            'c++': '//',
            'cc': '//',
            'cfc': '//',
            'cfm': '//',
            'clj': ';',
            'cls': '%',
            'cmd': 'REM',
            'coffee': '#',
            'cpp': '//',
            'cs': '//',
            'css': '//',
            # D-H
            'd': '//',
            'dart': '//',
            'elm': '--',
            'erl': '%',
            'ex': '#',
            'exs': '#',
            'f': '!',
            'f90': '!',
            'for': '!',
            'fs': '//',
            'fsi': '//',
            'fsx': '//',
            'go': '//',
            'groovy': '//',
            'h': '//',
            'hpp': '//',
            'hs': '--',
            'htm': '<!--',
            'html': '<!--',
            'hx': '//',
            # I-O
            'java': '//',
            'jl': '#',
            'js': '//',
            'jsx': '//',
            'kt': '//',
            'kts': '//',
            'less': '//',
            'lisp': ';',
            'lua': '--',
            'm': '%',  # MATLAB/Objective-C
            'markdown': '<!--',
            'md': '<!--',
            'nim': '#',
            'nix': '#',
            'ocaml': '(*',
            # P-Z
            'pas': '//',
            'php': '//',
            'pl': '#',
            'pm': '#',
            'ps1': '#',
            'psm1': '#',
            'py': '#',
            'pyw': '#',
            'r': '#',
            'rb': '#',
            'rs': '//',
            's': ';',
            'sass': '//',
            'scala': '//',
            'scm': ';',
            'scss': '//',
            'sh': '#',
            'sql': '--',
            'swift': '//',
            'tcl': '#',
            'tex': '%',
            'ts': '//',
            'tsx': '//',
            'vb': "'",
            'vba': "'",
            'vbs': "'",
            'vhdl': '--',
            'xsl': '<!--',
            'yaml': '#',
            'yml': '#',
            'zig': '//',
        }

        multi_line_comments = {
            # Languages with multi-line comment support
            'c': ('/*', '*/'),
            'c++': ('/*', '*/'),
            'cc': ('/*', '*/'),
            'cpp': ('/*', '*/'),
            'cs': ('/*', '*/'),
            'css': ('/*', '*/'),
            'd': ('/*', '*/'),
            'dart': ('/*', '*/'),
            'go': ('/*', '*/'),
            'groovy': ('/*', '*/'),
            'h': ('/*', '*/'),
            'hpp': ('/*', '*/'),
            'htm': ('<!--', '-->'),
            'html': ('<!--', '-->'),
            'hx': ('/*', '*/'),
            'java': ('/*', '*/'),
            'js': ('/*', '*/'),
            'jsx': ('/*', '*/'),
            'kt': ('/*', '*/'),
            'kts': ('/*', '*/'),
            'less': ('/*', '*/'),
            'lua': ('--[[', ']]'),
            'markdown': ('<!--', '-->'),
            'md': ('<!--', '-->'),
            'nim': ('#[', ']#'),
            'ocaml': ('(*', '*)'),
            'php': ('/*', '*/'),
            'ps1': ('<#', '#>'),
            'psm1': ('<#', '#>'),
            'py': ('"""', '"""'),
            'rs': ('/*', '*/'),
            'sass': ('/*', '*/'),
            'scala': ('/*', '*/'),
            'scss': ('/*', '*/'),
            'sql': ('/*', '*/'),
            'swift': ('/*', '*/'),
            'ts': ('/*', '*/'),
            'tsx': ('<!--', '-->'),
            'xsl': ('<!--', '-->'),
        }

        # Alternative multi-line comment styles
        alt_multi_line_comments = {
            'py': ("'''", "'''"),
            'ruby': ('=begin', '=end'),
            'perl': ('=pod', '=cut'),
            'jl': ('#=', '=#'),  # Julia
            'r': ("'", "'"),
            'hs': ('{-', '-}'),  # Haskell
            'clj': ('(comment', ')'),  # Clojure
            'coffee': ('###', '###'),  # CoffeeScript
        }

        # Documentation comment styles
        doc_comment_styles = {
            # JavaDoc style (/** */)
            'java': ('/**', '*/'),
            'js': ('/**', '*/'),
            'ts': ('/**', '*/'),
            'jsx': ('/**', '*/'),
            'tsx': ('/**', '*/'),
            'c': ('/**', '*/'),
            'cpp': ('/**', '*/'),
            'cc': ('/**', '*/'),
            'h': ('/**', '*/'),
            'hpp': ('/**', '*/'),
            'cs': ('/**', '*/'),
            'php': ('/**', '*/'),
            'scala': ('/**', '*/'),
            'kt': ('/**', '*/'),
            'kts': ('/**', '*/'),
            'groovy': ('/**', '*/'),
            
            # Triple-slash style
            'cs': ('///', ''),  # C# XML comments
            'rs': ('///', ''),  # Rust doc comments
            'swift': ('///', ''),  # Swift DocC
            
            # Python docstrings
            'py': ('"""', '"""'),
            'pyw': ('"""', '"""'),
            
            # R Roxygen
            'r': ("#'", ''),
            
            # Go docs
            'go': ('//', ''),
        }

        doc_line_count = 0
        
        # Get file extension
        file_ext = filename.split('.')[-1].lower() if '.' in filename else ''
        
        # Skip binary files or files with no content
        if not source_code or not file_ext:
            return 0
            
        lines = source_code.split('\n')
        in_multi_line_comment = False
        in_doc_comment = False
        current_multi_line_end = None
        current_doc_end = None
        
        # Check for single-line comments
        single_line_comment = single_line_comments.get(file_ext)
        
        # Check for multi-line comments
        multi_line_start, multi_line_end = None, None
        if file_ext in multi_line_comments:
            multi_line_start, multi_line_end = multi_line_comments[file_ext]
        
        # Check for documentation comments
        doc_start, doc_end = None, None
        if file_ext in doc_comment_styles:
            doc_start, doc_end = doc_comment_styles[file_ext]
        
        # Alternative multi-line comments
        alt_multi_start, alt_multi_end = None, None
        if file_ext in alt_multi_line_comments:
            alt_multi_start, alt_multi_end = alt_multi_line_comments[file_ext]
        
        # Process each line
        for line in lines:
            line_strip = line.strip()
            counted = False
            
            # Check if we're in a multi-line comment
            if in_multi_line_comment:
                doc_line_count += 1
                counted = True
                if current_multi_line_end in line:
                    in_multi_line_comment = False
                    current_multi_line_end = None
            
            # Check if we're in a doc comment
            elif in_doc_comment:
                doc_line_count += 1
                counted = True
                if current_doc_end and current_doc_end in line:
                    in_doc_comment = False
                    current_doc_end = None
            
            # Not in any comment block, check for new comments
            if not counted:
                # Check for doc comments
                if doc_start and doc_start in line_strip:
                    in_doc_comment = True
                    current_doc_end = doc_end
                    doc_line_count += 1
                    counted = True
                
                # Check for multi-line comments
                elif multi_line_start and multi_line_start in line_strip:
                    # Check if the comment ends on the same line
                    if multi_line_end in line_strip[line_strip.find(multi_line_start) + len(multi_line_start):]:
                        doc_line_count += 1
                    else:
                        in_multi_line_comment = True
                        current_multi_line_end = multi_line_end
                        doc_line_count += 1
                    counted = True
                
                # Check for alternative multi-line comments
                elif alt_multi_start and alt_multi_start in line_strip:
                    # Check if the comment ends on the same line
                    if alt_multi_end in line_strip[line_strip.find(alt_multi_start) + len(alt_multi_start):]:
                        doc_line_count += 1
                    else:
                        in_multi_line_comment = True
                        current_multi_line_end = alt_multi_end
                        doc_line_count += 1
                    counted = True
                
                # Check for single line comments
                elif single_line_comment and line_strip.startswith(single_line_comment):
                    doc_line_count += 1
                    counted = True
        
        return doc_line_count
    
    def get_metrics(self):
        test_percent = (self.test_lines / self.total_lines * 100) if self.total_lines > 0 else 0
        doc_percent = (self.doc_lines / self.total_lines * 100) if self.total_lines > 0 else 0
        
        return {
            "test_coverage": {
                "files": self.test_files,
                "lines": self.test_lines,
                "percent": test_percent
            },
            "doc_coverage": {
                "files": self.doc_files,
                "lines": self.doc_lines,
                "percent": doc_percent
            },
            "total": {
                "files": self.total_files,
                "lines": self.total_lines
            },
            "quality_score": (test_percent + doc_percent) / 2
        }
    
    @staticmethod
    def merge_metrics(metrics_list):
        if not metrics_list:
            return {
                "test_coverage": {"files": 0, "lines": 0, "percent": 0},
                "doc_coverage": {"files": 0, "lines": 0, "percent": 0},
                "total": {"files": 0, "lines": 0},
                "quality_score": 0
            }
        
        # Sum up raw counts
        total_test_files = sum(m.get("test_coverage", {}).get("files", 0) for m in metrics_list)
        total_doc_files = sum(m.get("doc_coverage", {}).get("files", 0) for m in metrics_list)
        total_test_lines = sum(m.get("test_coverage", {}).get("lines", 0) for m in metrics_list)
        total_doc_lines = sum(m.get("doc_coverage", {}).get("lines", 0) for m in metrics_list)
        total_files = sum(m.get("total", {}).get("files", 0) for m in metrics_list)
        total_lines = sum(m.get("total", {}).get("lines", 0) for m in metrics_list)
        
        # Calculate percentages
        test_percent = (total_test_lines / total_lines * 100) if total_lines > 0 else 0
        doc_percent = (total_doc_lines / total_lines * 100) if total_lines > 0 else 0
        
        return {
            "test_coverage": {
                "files": total_test_files,
                "lines": total_test_lines,
                "percent": test_percent
            },
            "doc_coverage": {
                "files": total_doc_files,
                "lines": total_doc_lines,
                "percent": doc_percent
            },
            "total": {
                "files": total_files,
                "lines": total_lines
            },
            "quality_score": (test_percent + doc_percent) / 2
        }
