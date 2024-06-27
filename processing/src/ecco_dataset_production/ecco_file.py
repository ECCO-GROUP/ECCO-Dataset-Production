"""
Filename string classes for ECCO MDS results files (ECCOMDSFilestr) and ECCO
production results (granule) distribution files (ECCOGranuleFilestr).

"""

import datetime as dt
import re


class ECCOMDSFilestr(object):
    """Gathers operations on ECCO MDS results file names of the form
    <prefix>_<averaging_period>.<time>.<ext> , where <ext> is either 'data' or
    'meta'.

    Args:
        filestr (str): ECCO MDS filename string. (default=None)
        **kwargs: Instead of filestr, individual file components may be provided
            and can be used to build up file-matching regular expressions,
            returned as re_filestr. Possible arguments include:
                prefix (str): Product prefix/name. (default: r'.*')
                averaging_period (str): Averaging period ('mon_mean',
                    'day_mean', or 'day_inst', case_insensitive). (default:
                    r'.*')
                time (str): Time string with or without zero padding.
                    (default: r'\d{10}')
                ext (str): File extension (default: r'.*')
                #ext (str): File extension (default: 'data')

    Attributes:
        filestr (str): Input filestr, if provided, or raw string created from
            input kwargs (may be a valid filename if all kwargs present and in
            correct formats, but will not be a valid regular expression if not;
            see re_filestr in this case)
        re_filestr (str): filestr regular expression for use in pattern-matching
            operations, based on either filestr or kwargs input.
        prefix (str): 'prefix' from either input filestr or kwarg, r'.*'
            otherwise.
        averaging_period (str): 'averaging_period' from input filestr or kwarg,
            r'.*' otherwise.
        time (str): 'time' from input filestr or kwarg, r'\d{10}' otherwise.
        ext (str): 'ext' from input filestr or kwarg, r'.*' otherwise.

    """
    fmt = '<prefix>_<averaging_period>.<time>.<ext>'

    def __init__(self,filestr=None,**kwargs):
        """Create an instance of the ECCOMDSFilestr class.

        """
        if filestr:
            # use filestr to set all attributes (a little complicated because
            # ECCO variable names may include underscores):
            try:
                re_so = re.search('_day_inst|_day_mean|_mon_mean',filestr)
                self.prefix = filestr[:re_so.span()[0]]
                self.averaging_period = filestr[re_so.span()[0]+1:re_so.span()[1]]
                time_and_ext = filestr[re_so.span()[1]+1:]
                re_mo = re.match(r'\d{10}',time_and_ext)
                self.time = time_and_ext[:re_mo.span()[1]]
                self.ext = time_and_ext[re_mo.span()[1]+1:]
                self.filestr = filestr
            except:
                raise ValueError(
                    f'unrecognized file string format; must be of the form {ECCOMDSFilestr.fmt}')
        else:
            # set attributes that may have been provided, regex placeholders as
            # best as we can for everything else:
            self.prefix = kwargs.pop('prefix',r'.*')
            self.averaging_period = kwargs.pop('averaging_period',r'.*')
            self.time = kwargs.pop('time',r'\d{10}')
            self.ext = kwargs.pop('ext',r'.*')
            # raw filestr:
            self.filestr = \
                self.prefix + '_' + self.averaging_period + '.' + \
                self.time + '.' + self.ext
            # filestr, as a regex:
            self.re_filestr = \
                self.prefix + '_' + self.averaging_period + '\.' + \
                self.time + '\.' + self.ext


class ECCOGranuleFilestr(object):
    """Gathers operations on ECCO dataset production results (granule)
    distribution file names of the form
    <prefix>_<averaging_period>_<date>_ECCO_<version>_<grid_type>_<grid_label>.nc

    Args:
        filestr (str): ECCO file string. (default=None)
        **kwargs: Instead of filestr, individual file components may be provided
            and can be used to build up file-matching regular expressions,
            returned as re_filestr. Possible arguments include:
                prefix (str): Product prefix/name (default: r'.*')
                averaging_period (str): Averaging period ('mon_mean',
                    'day_mean', or 'day_inst', case insensitive). (default:
                    r'.*')
                date (str): Averaging period end date, in ISO Date or Date Time
                    format (e.g., 'YYYY-MM-DD' or 'YYYY-MM-DDThh:mm:ss').
                    Regardless of input, filestr date may be truncated according
                    to averaging period, for example, if
                    averaging_period='mon_mean' and date is in 'YYYY-MM-DD'
                    format, only the 'YYYY-MM' portion will be used in filestr.
                    (default: r'.*')
                version (str): ECCO version, e.g. 'V4r4', 'V4r5', etc. (default:
                    r'.*')
                grid_type (str): Grid, or product type, e.g. 'native' or
                    'latlon'. (default: r'.*')
                grid_label (str): Additiona grid attributes, e.g., 'llc0090',
                    '0p50deg', etc. (default: r'.*')
                ext (str): File extension. (default: 'nc')

    Attributes:
        filestr (str): Input filestr, if provided, or raw string created from
            input kwargs (may be a valid filename if all kwargs present and in
            correct formats, but will not be a valid regular expression if not;
            see re_filestr in this case)
        re_filestr (str): filestr regular expression for use in pattern-matching
            operations, based on either filestr or kwargs input.
        prefix (str): 'prefix' from either input filestr or kwarg, r'.*'
            otherwise.
        averaging_period (str): 'averaging_period' from input filestr or kwarg,
            r'.*' otherwise. See preceding kwargs comment.
        date (str): 'date' from input filestr or kwarg, r'.*' otherwise. See
            preceding kwargs comment.
        version (str): 'version' from input filestr or kwarg, r'.*' otherwise.
        grid_type (str): 'grid_type' from input filestr or kwarg, r'.*'
            otherwise.
        grid_label (str): 'grid_label' from input filestr or kwarg, r'.*'
            otherwise.
        ext (str): 'ext' from input filestr or kwarg, string 'nc' otherwise.

    """
    fmt = '<prefix>_<averaging_period>_<date>_ECCO_<version>_<grid_type>_<grid_label>.nc'

    def __init__(self,filestr=None,**kwargs):
        """Create an instance of the ECCOGranuleFilestr class.

        """
        if filestr:
            # use filestr to set all attributes (a little complicated because
            # ECCO variable names may include underscores):
            try:
                re_so = re.search('_day_inst|_day_mean|_mon_mean',filestr)
                self.prefix = filestr[:re_so.span()[0]]
                self.averaging_period = filestr[re_so.span()[0]+1:re_so.span()[1]]
                date_version_grid_type_grid_label_and_ext = filestr[re_so.span()[1]+1:]
                # remaining fields are reliably separated by '_'; parse accordingly:
                self.date,_,self.version,self.grid_type,grid_label_and_ext = \
                    filestr[re_so.span()[1]+1:].split('_')
                self.grid_label,self.ext = grid_label_and_ext.split('.')
                self.filestr = filestr
            except:
                raise ValueError(
                    f'unrecognized file string format; must be of the form {ECCOGranuleFilestr.fmt}')
        else:
            # set attributes that may have been provided, regex placeholders as
            # best as we can for everything else:
            self.prefix = kwargs.pop('prefix',r'.*')
            self.averaging_period = kwargs.pop('averaging_period',r'.*')
            date = kwargs.pop('date',r'.*')
            try:
                date_as_dt = dt.datetime.fromisoformat(date)
                if 'mon' in self.averaging_period:
                    self.date = '{0:04d}-{1:02d}'.format(
                        date_as_dt.year,date_as_dt.month)
                elif 'day' in self.averaging_period:
                    self.date = '{0:04d}-{1:02d}-{2:02d}'.format(
                        date_as_dt.year,date_as_dt.month,date_as_dt.day)
            except:
                self.date = date
            self.version = kwargs.pop('version',r'.*')
            self.grid_type = kwargs.pop('grid_type',r'.*')
            self.grid_label = kwargs.pop('grid_label',r'.*')
            self.ext = kwargs.pop('ext','nc')
            # raw filestr:
            self.filestr = '_'.join([
                self.prefix, self.averaging_period, self.date, 'ECCO',
                self.version, self.grid_type, self.grid_label])
            self.filestr = '.'.join([self.filestr,self.ext])
            # filestr, as regex:
            self.re_filestr = '_'.join([
                self.prefix, self.averaging_period, self.date, 'ECCO',
                self.version, self.grid_type, self.grid_label])
            self.re_filestr = self.re_filestr + '\.' + self.ext

