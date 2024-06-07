
import ecco_v4_py
import numpy as np

def make_time_bounds_metadata( granule_time=None,
    model_start_time=None, model_end_time=None,
    model_timestep=None, model_timestep_units=None,
    averaging_period=None):

    """If ECCO model start and end times do not align on 24 hour boundaries,
    adjust first and last time averaging period output endpoints accordingly.

    Args:
        granule_time (int or str): ECCO analysis time value per time string in
            file name (e.g., '0000000012' or 12, etc.).
        model_start_time (str): ECCO model start time, in ISO Date Time format
            ('YYYY-MM-DDThh:mm:ss')
        model_end_time (str): ECCO model end time, in ISO Date Time format
            ('YYYY-MM-DDThh:mm:ss')
        model_timestep (int): Number of analysis time steps per granule_time
            increment (e.g., 3600 for one second time steps and granule_time
            increments of one hour).
        model_timestep_units (str): model_timestep units, per NumPy Datetime64
            conventions, e.g., hours ('h'), minutes ('m'), seconds ('s') etc.
        averaging_period (str): Interval for which adjusted time bounds will be
            created ('AVG_YEAR', 'AVG_MON', 'AVG_WEEK', or 'AVG_DAY' (case
            insensitive)).

    Returns:
        (tb,center_time): Tuple of tb time bounds array (tb[0]=interval start
            time, tb[1]=interval end time), and interval center_time (ref.
            ecco_v4_py.ecco_utils.make_time_bounds_from_ds64 return values)

    """
    # ECCO results averages are usually formed on multiples of 24 hour
    # intervals, from 00:00:00 to 24:00:00.  In the case of the very first and
    # last intervals, however, start and end times may not "align" on 00:00:00
    # (or 24:00:00), and averages, therefore, may include an endpoint interval
    # that is less than 24 hours. Determine these potentially adjusted start and
    # end times so that they may be reported in file metadata.

    # given a granule_time, get interval start, end, and center times:

    tb, center_time = ecco_v4_py.ecco_utils.make_time_bounds_from_ds64(
        np.datetime64(model_start_time) + 
        np.timedelta64(int(granule_time)*model_timestep,model_timestep_units),
        averaging_period.upper())

    # ...and if one of the interval's endpoints lands on either the very first
    # or last day, adjust time boundaries accordingly:

    if tb[0].astype('datetime64[D]')==np.datetime64(model_start_time,'D'):
        # start of averaging period lands in first day's interval:
        tb[0] = np.datetime64(model_start_time)
        center_time = tb[0] + (tb[1]-tb[0])/2   # delta time math to avoid
                                                # exceptions
    elif tb[1].astype('datetime64[D]')==np.datetime64(model_end_time,'D'):
        # end of averaging period lands in last day's interval: move lower bound
        # up by an amount equal to the amount of time left in the last day
        # (because the averaging interval might be a day, month, or year), so it
        # aligns on a 24 hour boundary:
        tb[0] = tb[0] + \
            np.timedelta64(1,'D') - \
            (np.datetime64(model_end_time)-np.datetime64(model_end_time,'D'))
        center_time = tb[0] + (tb[1]-tb[0])/2   # delta time math to avoid
                                                # exceptions

    return (tb,center_time)

