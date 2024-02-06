from datetime import datetime


def get_current_datetime():
    """Returns the current datetime."""
    return datetime.now()


def format_datetime(dt, format_string="%Y-%m-%d %H:%M:%S"):
    """Returns a formatted string for a datetime object."""
    return dt.strftime(format_string)


def parse_datetime(date_string, format_string="%Y-%m-%d %H:%M:%S"):
    """Parses a string into a datetime object based on the given format."""
    return datetime.strptime(date_string, format_string)

# Add more datetime-related utility functions as needed
