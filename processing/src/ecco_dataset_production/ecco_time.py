
import ecco_v4_py
import numpy as np


def adjusted_time_bounds( timestamp=None, timestamp_units='h',
    averaging_period=None, cfg=None):
    """If ECCO analysis start and end times do not align on 24 hour boundaries,
    adjust first and last time averaging period endpoints accordingly.

    Args:
        timestamp (int or str): ECCO analysis endpoint timestamp (e.g.,
            '0000000012' or 12, etc.)
        timestamp_units (str): timestamp argument units, per NumPy Datetime64
            conventions, e.g., hours ('h'), mintes ('m'), etc. (default: 'h')
        averaging_period (str): Interval for which adjusted time bounds will be
            created ('AVG_YEAR', 'AVG_MON', 'AVG_WEEK', or 'AVG_DAY' (case
            insensitive))
        cfg (dict): ECCO Dataset Production configuration data, typically parsed
            from yaml input (see examples). Specifically, this routine looks for
            the keys, 'model_start_time' and 'model_end_time'.

    Returns:
        (tb,center_time): Tuple of tb array (tb[0]=interval start time,
            tb[1]=interval end time), and interval center_time (ref.
            ecco_v4_py.ecco_utils.make_time_bounds_from_ds64 return values)

    """
    # ECCO results averages are usually formed on multiples of 24 hour
    # intervals, from 00:00:00 to 24:00:00.  In the case of the very first and
    # last intervals, however, start and end times may not "align" on 00:00:00
    # (or 24:00:00), and averages, therefore, may include an endpoint interval
    # that is less than 24 hours. Determine these potentially adjusted start and
    # end times so that they may be reported in file metadata.

    # get usual averaging interval start, end, and center times (i.e., based on
    # 24 hour intervals)...:

    tb, center_time = ecco_v4_py.ecco_utils.make_time_bounds_from_ds64(
        np.datetime64(cfg['model_start_time'])+np.timedelta64(int(timestamp),timestamp_units),
        averaging_period.upper())

    # ...and if one of the interval's endpoints lands on the first or last day,
    # adjust time boundaries accordingly:

    if time_bounds[0].astype('datetime64[D]')==np.datetime64(cfg['model_start_time'],'D'):
        # start of averaging period lands in first day's interval:
        tb[0] = np.datetime64(cfg['model_start_time'])
    center_time = (tb[0]+tb[1])/2
    elif time_bounds[1].astype('datetime64[D]')==np.datetime64(cfg['model_end_time'],'D'):
        # end of averaging period lands in last day's interval: move lower bound
        # up by an amount equal to the amount of time left in the last day
        # (because the averaging interval might be a day, month, or year), so it
        # aligns on a 24 hour boundary:
        tb[0] = tb[0] + \
            np.timedelta64(1,'D') - \
            (np.datetime64(cfg['model_end_time'])-np.datetime64(cfg['model_end_time'],'D'))
        center_time = (tb[0]+tb[1])/2

    return (tb,center_time)

