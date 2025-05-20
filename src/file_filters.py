import os
import re
from .logger import get_logger

# Set up logger for this module
logger = get_logger(__name__)

# List of file patterns to ignore
IGNORED_FILE_PATTERNS = [
    # Solidity JavaScript compiler files
    r'soljson-v\d+\.\d+\.\d+\+commit\.[a-f0-9]+\.js',
    
    # Common large files to ignore
    r'\.min\.js$',
    r'bundle\.js$',
    r'vendor\.js$',
    
    # Add more patterns as needed
    r'\.map$',  # Source maps
    r'\.wasm$',  # WebAssembly binaries
    r'\.zip$',   # Zip archives
    r'\.gz$',    # Gzipped files
    r'\.tar$',   # Tar archives
    r'\.svg$',   # SVG files can be large
    r'\.ttf$',   # Font files
    r'\.woff2?$', # Web fonts
    r'\.eot$',   # Embedded OpenType fonts
    r'\.png$',   # Image files
    r'\.jpe?g$', # JPEG images
    r'\.gif$',   # GIF images
    r'\.ico$',   # Icon files
    r'\.pdf$',   # PDF documents
    r'\.mp[34]$', # Media files
    r'\.wav$',   # Audio files
    r'\.avi$',   # Video files
    r'\.mov$',   # Video files
    r'\.flv$',   # Flash video files
    r'\.swf$',   # Flash files
    r'\.doc[x]?$', # Word documents
    r'\.xls[x]?$', # Excel spreadsheets
    r'\.ppt[x]?$', # PowerPoint presentations
    r'\.bin$',   # Binary files
    r'\.dat$',   # Data files
    r'\.o$',     # Object files
    r'\.so$',    # Shared object files
    r'\.dll$',   # Dynamic link libraries
    r'\.exe$',   # Executable files
    r'\.pyc$',   # Python compiled files
    r'__pycache__', # Python cache directories
    r'node_modules', # Node.js modules
    r'vendor/',  # Vendor directories
    r'dist/',    # Distribution directories
    r'build/',   # Build directories
    r'\.git/',   # Git directories
    r'\.7z$',   # 7z archives
]

# Compiled regex patterns for faster matching
IGNORED_PATTERNS = [re.compile(pattern) for pattern in IGNORED_FILE_PATTERNS]

# Remove file size limit logic

def should_analyze_file(file_path):
    # Skip if file doesn't exist (this shouldn't happen normally)
    if not file_path or (os.path.exists(file_path) == False and not isinstance(file_path, str)):
        logger.debug(f"File does not exist: {file_path}")
        return False
    
    # Extract filename from path if it's a path
    filename = os.path.basename(file_path) if os.path.isfile(file_path) else file_path
    
    # Log the file being checked
    logger.debug(f"Checking file: {filename}")
    
    # Check if filename matches any ignored patterns
    for i, pattern in enumerate(IGNORED_PATTERNS):
        if pattern.search(filename):
            logger.info(f"Ignoring file matching pattern '{IGNORED_FILE_PATTERNS[i]}': {filename}")
            return False
    
    # If none of the filters above excluded the file, it should be analyzed
    logger.debug(f"File passed filters: {filename}")
    return True
