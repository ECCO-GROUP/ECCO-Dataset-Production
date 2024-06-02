"""
ECCO filename string classes for ECCO results files (ECCOFilestr) and ECCO
production results distribution files (ECCOProductionFilestr).
"""

import re

class ECCOFilestr(object):
    """Gathers operations on ECCO raw results file strings of the form
    <varname>_<averaging_period>.<time>.<ext> .

    Args:
        filestr (str): ECCO file string (default=None)
        **kwargs: Instead of filestr, individual file components may be provided
            and can be used to build up file-matching regular expressions,
            returned as filestr. Possible arguments include:
                varname (str): Variable name (default: r'.*')
                averaging_period (str): Averaging period (default: r'.*')
                time (str): Time string with or without zero padding
                    (default: r'\d{10}')
                ext (str): File extension (default: 'data')

    Attributes:
        filestr (str): Input filestr, if provided, regular expression otherwise.
        varname (str): 'varname' from either input filestr or kwarg, r'.*'
            otherwise.
        averaging_period (str): 'averaging_period' from input filestr or kwarg,
            r'.*' otherwise.
        time (str): 'time' from input filestr or kwarg, r'\d{10}' otherwise.
        ext (str): 'ext' from input filestr or kwarg, string 'data' otherwise.

    """
    fmt = '<varname>_<averaging_period>.<time>.<ext>'

    def __init__(self,filestr=None,**kwargs):
        """Create an instance of the ECCOFilestr class.

        """
        if filestr:
            # use filestr to set all attributes (a little complicated because
            # ECCO variable names may include underscores:
            try:
                re_so = re.search('_day_inst|_day_mean|_mon_mean',filestr)
                self.varname = filestr[:re_so.span()[0]]
                self.averaging_period = filestr[re_so.span()[0]+1:re_so.span()[1]]
                time_and_ext = filestr[re_so.span()[1]+1:]
                re_mo = re.match(r'\d{10}',time_and_ext)
                self.time = time_and_ext[:re_mo.span()[1]]
                self.ext = time_and_ext[re_mo.span()[1]+1:]
                self.filestr = filestr
            except:
                raise ValueError(
                    f'unrecognized file string format; must be of the form {ECCOFilestr.fmt}')
        else:
            # set attributes that may have been provided, regex placeholders as
            # best as we can for everything else:
            self.varname = kwargs.pop('varname',r'.*')
            self.averaging_period = kwargs.pop('averaging_period',r'.*')
            self.time = kwargs.pop('time',r'\d{10}')
            self.ext = kwargs.pop('ext',r'data')
            # filestr, as a regex:
            self.filestr = \
                self.varname + '_' + self.averaging_period + '\.' + \
                self.time + '\.' + self.ext


class ECCOProductionFilestr(object):
    """Gathers operations on ECCO dataset production results distribution files
    of the form <varname>_<averaging_period>_<date>_ECCO_<version>_<type>_<>.nc

    Args:

    Attributes:

    """
    fmt = '<varname>_<averaging_period>_<date>_ECCO_<version>_<type>_<>.nc

    def __init__(self,filestr=None,**kwargs):
        """Create an instance of the ECCOFilestr class.

        """
        pass

