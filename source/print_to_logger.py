import os
import re
import argparse
import sys

def add_logger_import(content):
    """Add logger import if not already present."""
    if "from logger import get_logger" not in content:
        # Find the last import statement
        import_match = list(re.finditer(r'^import\s+.*$|^from\s+.*$', content, re.MULTILINE))
        if import_match:
            last_import = import_match[-1]
            last_import_end = last_import.end()
            # Insert after the last import
            return (
                content[:last_import_end] + 
                "\n\n# Set up logger for this module\nfrom logger import get_logger\nlogger = get_logger(__name__)\n" + 
                content[last_import_end:]
            )
        else:
            # No imports found, add at the beginning
            return "from logger import get_logger\nlogger = get_logger(__name__)\n\n" + content
    return content

def replace_print_statements(content):
    """Replace print statements with logger.info calls."""
    # This pattern matches print statements, considering various forms
    pattern = r'print\s*\((.*?)\)'
    
    # Function to process each match
    def replace_print(match):
        content = match.group(1).strip()
        if not content:
            return "logger.info('')"
        elif content.startswith("f"):
            # Handle f-strings
            return f"logger.debug({content})"
        elif content.startswith('"') or content.startswith("'"):
            # Regular strings
            return f"logger.info({content})"
        else:
            # Other expressions
            return f"logger.debug({content})"
    
    # Replace all print statements
    modified_content = re.sub(pattern, replace_print, content, flags=re.DOTALL)
    
    # Add logger import if needed
    modified_content = add_logger_import(modified_content)
    
    return modified_content

def process_file(file_path, dry_run=False):
    """Process a single Python file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        try:
            content = file.read()
        except UnicodeDecodeError:
            print(f"Error reading {file_path}: Not a text file or encoding issues")
            return False
    
    modified_content = replace_print_statements(content)
    
    if modified_content != content:
        if not dry_run:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(modified_content)
            print(f"Modified: {file_path}")
        else:
            print(f"Would modify: {file_path}")
        return True
    return False

def process_directory(directory, dry_run=False):
    """Process all Python files in a directory recursively."""
    total_files = 0
    modified_files = 0
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                total_files += 1
                if process_file(file_path, dry_run):
                    modified_files += 1
    
    print(f"Summary: {modified_files} of {total_files} files " + 
          ("would be " if dry_run else "") + "modified.")

def main():
    parser = argparse.ArgumentParser(
        description='Replace print statements with logger calls in Python files.'
    )
    parser.add_argument('directory', help='Directory to process')
    parser.add_argument('--dry-run', action='store_true', 
                        help='Show what would be changed without modifying files')
    
    args = parser.parse_args()
    
    if not os.path.isdir(args.directory):
        print(f"Error: {args.directory} is not a valid directory.")
        return 1
    
    process_directory(args.directory, args.dry_run)
    return 0

if __name__ == "__main__":
    sys.exit(main())