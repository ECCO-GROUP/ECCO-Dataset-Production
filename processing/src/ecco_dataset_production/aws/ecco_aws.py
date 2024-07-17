"""
"""

import re

def is_s3_uri(path_or_uri_str):
    """Determines whether or not input string is an AWS S3Uri.

    Args:
        path_or_uri_str (str): Input string.

    Returns:
        True if string matches 's3://', False otherwise.
    """
    if re.search( r's3:\/\/', path_or_uri_str, re.IGNORECASE):
        return True
    else:
        return False

