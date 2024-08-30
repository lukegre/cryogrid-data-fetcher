from .. import logger


def is_safe_s3_path(s3_path):
    import re
    
    # Check if the path starts with 's3://'
    logger.debug(f"Checking S3 path: {s3_path}")
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Path does not start with 's3://': {s3_path}")
    
    # Remove the 's3://' prefix for further validation
    path = s3_path[5:]

    # Check for double slashes '//' in the path
    if '//' in path:
        raise ValueError(f"Path contains double slashes: {s3_path}")

    # Split into bucket name and key
    parts = path.split('/', 1)
    bucket_name = parts[0]
    key = parts[1] if len(parts) > 1 else ''

    # Validate bucket name: lowercase letters, numbers, hyphens, periods
    if not re.match(r'^[a-z0-9.-]{3,63}$', bucket_name):
        raise ValueError(f"Invalid bucket name: {bucket_name}")

    # Bucket name should not have consecutive periods or start/end with hyphen
    if '..' in bucket_name or bucket_name.startswith('-') or bucket_name.endswith('-'):
        raise ValueError(f"Invalid bucket name: {bucket_name}")

    # Validate key: avoid control characters and unprintable characters
    if any(ord(char) < 32 or ord(char) > 126 for char in key):
        raise ValueError(f"Invalid key: {key}")

    # Key length should be between 1 and 1024 characters
    if not (1 <= len(key) <= 1024):
        raise ValueError(f"Key length should be between 1 and 1024 characters: {key}")
