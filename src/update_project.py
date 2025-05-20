import os
import sys
import shutil
import subprocess

from logger import get_logger
logger = get_logger(__name__)


def ensure_dir(directory):
    """Create directory if it doesn't exist."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def create_file(filename, content):
    """Create a file with the given content."""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    logger.debug(f"Created {filename}")

def main():
    # Check if current directory looks like the project root
    required_files = ['main.py', 'metrics.py', 'analysis.py', 'utils.py']
    for file in required_files:
        if not os.path.exists(file):
            logger.debug(f"Error: {file} not found. Make sure you run this script from the project root directory.")
            return 1
    
    logger.info("Starting project update...")
    
    # Create backup directory
    backup_dir = "backup_before_logging"
    ensure_dir(backup_dir)
    
    # Backup original files
    for file in required_files:
        shutil.copy2(file, os.path.join(backup_dir, file))
    logger.debug(f"Original files backed up to {backup_dir}/")
    
    # Create logs directory
    logs_dir = "logs"
    ensure_dir(logs_dir)
    logger.debug(f"Created {logs_dir}/ directory")
    
    # Run the print-to-logger.py script to convert all print statements
    logger.info("Converting print statements to logger calls...")
    try:
        subprocess.run([sys.executable, "print_to_logger.py", "."], check=True)
        logger.info("Successfully converted print statements")
    except subprocess.CalledProcessError:
        logger.info("Error converting print statements. Please check the output.")
    
    logger.info("\nUpdate complete! To test run:")
    logger.info("python main.py --folder A --limit 1")
    logger.info("\nCheck the logs directory for detailed logs.")
    return 0

if __name__ == "__main__":
    sys.exit(main())