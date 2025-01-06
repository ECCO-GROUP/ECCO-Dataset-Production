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
            and can be used to build up file-matching expressions.  Possible
            arguments include:
                prefix (str): Product prefix/name.
                averaging_period (str): Averaging period ('mon_mean',
                    'day_mean', or 'day_inst', case_insensitive).
                time (str): Time string with or without zero padding.
                ext (str): File extension.

    Attributes:
        prefix (str): 'prefix' from either input filestr or kwarg.
        averaging_period (str): 'averaging_period' from input filestr or kwarg.
        time (str): 'time' from input filestr or kwarg.
        ext (str): 'ext' from input filestr or kwarg.

    Properties:
        filestr (str): ECCO mds file string, or shell wildcard expression if
            any of the filestring components are undefined.
        re_filestr (str): ECCO mds file string, or regular expression
            pattern if any of the filestring components are undefined.

    """
    fmt = '<prefix>_<averaging_period>.<time>.<ext>'

    def __init__(self,filestr=None,**kwargs):
        """Create an instance of the ECCOMDSFilestr class.

        """
        if filestr:
            #print('IAN filestr ', filestr)
            # use filestr to set all attributes (a little complicated because
            # ECCO variable names may include underscores):
            try:
                re_so = re.search('_day_snap|_day_mean|_mon_mean',filestr)
                self.prefix = filestr[:re_so.span()[0]]
                self.averaging_period = filestr[re_so.span()[0]+1:re_so.span()[1]]
                time_and_ext = filestr[re_so.span()[1]+1:]
                re_mo = re.match(r'\d{10}',time_and_ext)
                self.time = time_and_ext[:re_mo.span()[1]]
                self.ext = time_and_ext[re_mo.span()[1]+1:]
            except:
                raise ValueError(
                    f'unrecognized file string format; must be of the form {ECCOMDSFilestr.fmt}')
        else:
            # set attributes that may have been provided:
            self.prefix = kwargs.pop('prefix',None)
            self.averaging_period = kwargs.pop('averaging_period',None)
            self.time = kwargs.pop('time',None)
            if self.time:
                # if an integer has been provided, enable subsequent string operations:
                self.time = str(self.time)
            self.ext = kwargs.pop('ext',None)


    @property
    def filestr(self):
        """ECCO mds file string, or shell wildcard expression if any of the
        filestring components are undefined.

        """
        prefix = self.prefix if self.prefix else '*'
        averaging_period = self.averaging_period if self.averaging_period else '*'
        time = f'{int(self.time):010d}' if self.time else '*'
        ext = self.ext if self.ext else '*'
        return \
            prefix + '_' + \
            averaging_period + '.' + \
            time + '.' + \
            ext


    @property
    def re_filestr(self):
        """ECCO mds file string, or regular expression pattern if any of the
        filestring components are undefined.

        """
        prefix = self.prefix if self.prefix else '.*'
        averaging_period = self.averaging_period if self.averaging_period else '.*'
        time = f'{int(self.time):010d}' if self.time else '\d{10}'
        ext = self.ext if self.ext else '.*'
        return \
            prefix + '_' + \
            averaging_period + '\.' + \
            time + '\.' + \
            ext


class ECCOGranuleFilestr(object):
    """Gathers operations on ECCO dataset production results (granule)
    distribution file names of the form
    <prefix>_<averaging_period>_<date>_ECCO_<version>_<grid_type>_<grid_label>.nc

    Args:
        filestr (str): ECCO file string. (default=None)
        **kwargs: Instead of filestr, individual file components may be provided
            and can be used to build up file-matching expressions.  Possible
            arguments include:
                prefix (str): Product prefix/name.
                averaging_period (str): Averaging period ('mon_mean',
                    'day_mean', or 'day_inst', case insensitive).
                date (str): Averaging period end date, in ISO Date or Date Time
                    format (e.g., 'YYYY-MM-DD' or 'YYYY-MM-DDThh:mm:ss').
                    Regardless of input, filestr date may be truncated according
                    to averaging period, for example, if
                    averaging_period='mon_mean' and date is in 'YYYY-MM-DD'
                    format, only the 'YYYY-MM' portion will be used in filestr.
                version (str): ECCO version, e.g. 'V4r4', 'V4r5', etc.
                grid_type (str): Grid, or product type, e.g. 'latlon', 'native'
                    or 'snap'.
                grid_label (str): Additional grid attributes, e.g., 'llc0090',
                    '0p50deg', etc.
                ext (str): File extension. (default: 'nc')

    Attributes:
        prefix (str): 'prefix' from either input filestr or kwarg.
        averaging_period (str): 'averaging_period' from input filestr or kwarg.
        date (str): 'date' from input filestr or kwarg.
        version (str): 'version' from input filestr or kwarg.
        grid_type (str): 'grid_type' from input filestr or kwarg.
        grid_label (str): 'grid_label' from input filestr or kwarg.
        ext (str): 'ext' from input filestr or kwarg, string 'nc' otherwise.

    Properties:
        filestr (str): ECCO granule file string, or shell wildcard expression if
            any of the filestring components are undefined.
        re_filestr (str): ECCO granule file string, or regular expression
            pattern if any of the filestring components are undefined.

    """
    fmt = '<prefix>_<averaging_period>_<date>_ECCO_<version>_<grid_type>_<grid_label>.nc'

    def __init__(self,filestr=None,**kwargs):
        """Create an instance of the ECCOGranuleFilestr class.

        """
        if filestr:
            #print('IAN ECCOGranuleFilestr filestr ', filestr)

            # use filestr to set all attributes (a little complicated because
            # ECCO variable names may include underscores):
            try:
                re_so = re.search('_day_snap|_day_mean|_mon_mean',filestr)
                self.prefix = filestr[:re_so.span()[0]]
                self.averaging_period = filestr[re_so.span()[0]+1:re_so.span()[1]]
                date_version_grid_type_grid_label_and_ext = filestr[re_so.span()[1]+1:]
                # remaining fields are reliably separated by '_'; parse accordingly:
                self.date,_,self.version,self.grid_type,grid_label_and_ext = \
                    filestr[re_so.span()[1]+1:].split('_')
                self.grid_label,self.ext = grid_label_and_ext.split('.')
            except:
                raise ValueError(
                    f'unrecognized file string format; must be of the form {ECCOGranuleFilestr.fmt}')
        else:
            # set attributes that may have been provided:
            self.prefix = kwargs.pop('prefix',None)
            self.averaging_period = kwargs.pop('averaging_period',None)
            date = kwargs.pop('date',None)
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
            self.version = kwargs.pop('version',None)
            self.grid_type = kwargs.pop('grid_type',None)
            self.grid_label = kwargs.pop('grid_label',None)
            self.ext = kwargs.pop('ext','nc')


    @property
    def filestr(self):
        """ECCO granule file string, or shell wildcard expression if any of the
        filestring components are undefined.

        """
        filestr = '_'.join([
            self.prefix if self.prefix else '*',
            self.averaging_period if self.averaging_period else '*',
            self.date if self.date else '*',
            'ECCO',
            self.version if self.version else '*',
            self.grid_type if self.grid_type else '*',
            self.grid_label if self.grid_label else '*',
            ])
        return '.'.join([filestr,self.ext])


    @property
    def re_filestr(self):
        """ECCO granule file string, or regular expression pattern if any of the
        filestring components are undefined.

        """
        filestr = '_'.join([
            self.prefix if self.prefix else '.*',
            self.averaging_period if self.averaging_period else '.*',
            self.date if self.date else '.*',
            'ECCO',
            self.version if self.version else '.*',
            self.grid_type if self.grid_type else '.*',
            self.grid_label if self.grid_label else '.*',
            ])
        return '\.'.join([filestr,self.ext])

