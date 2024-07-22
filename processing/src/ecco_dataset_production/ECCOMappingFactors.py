
import boto3
import os
import re
import subprocess
import urllib

# --- utilities, at some point ---

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

# --- end utilities, at some point ---


class ECCOMappingFactors(object):
    """Class to manage access to, and operations using, ECCO mapping factors.

    Args:
        mapping_factors_loc (str): Path to, or AWS S3 location of, mapping
            factors, i.e., a top-level directory containing
            ecco_latlon_grid_mappings_2D.xz, ecco_latlon_grid_mappings_all.xz,
            and the subdirectories 3D, land_mask, latlon_grid, and sparse.
        **kwargs: Depending on ECCOMappingFactors instantiation context:
            keygen (str): If mapping_factors_loc references an AWS S3 endpoint
                and ECCOMappingFactors is to be instantiated in a non-AWS client
                in an SSO environment, keygen can be used to provide the name of
                a requried federated login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64)
            profile_name (str): Optional profile name to be used in combination
                with keygen (e.g., 'saml-pub', 'default', etc.)

    Attributes:
        mapping_factors_loc (str): Local store of input mapping_factors_loc
        s3r (AWS resource): AWS resource if mapping_factors_loc references an
            AWS S3 endpoint

    """
    def __init__(self, mapping_factors_loc, **kwargs):
        """Create an instance of the MappingFactors class to manage access to,
        and operations using, ECCO mapping factors.
        """
        if is_s3_uri(mapping_factors_loc):

            # if running within an SSO environment, make sure credentials are
            # up-to-date:
            if kwargs.get('keygen',None):
                subprocess.run(kwargs.get('keygen'),check=True)
                # use profile_name if one has been provided:
                if kwargs.get('profile_name',None):
                    boto3.setup_default_session(
                        profile_name=kwargs.get('profile_name'))

            s3r = boto3.resource('s3')

            if s3r.Bucket(urllib.parse.urlparse(mapping_factors_loc).netloc).creation_date:
                self.s3r = s3r
                self.mapping_factors_loc = mapping_factors_loc
            else:
                raise RuntimeError(f'{mapping_factors_loc} does not exist')

        elif os.path.isdir(mapping_factors_loc):
            self.s3r = None
            self.mapping_factors_loc = mapping_factors_loc
        else:
            raise RuntimeError(
                'mapping_factors_loc must specify an existing directory or AWS S3 endpoint')


