
def fmt_timespan(t_secs: float) -> str:
    """
    Simple timespan formatting method. 

    Args:
        t_secs (float): a timespan in seconds

    Returns:
        a string representation of the timespan
    """
    ts = ""
    if t_secs < 0:
        ts = "0"
    elif t_secs < 60:
        ts = f'{t_secs:.0f} secs'
    elif t_secs < 3600:
        ts = f'{t_secs / 60:.1f} mins'
    else:
        ts = f'{t_secs / 3600:.1f} hours'

    return ts
