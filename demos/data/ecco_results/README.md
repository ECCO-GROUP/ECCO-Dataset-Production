## Selected ECCO results test data.

Although already included for test purposes, the selected ECCO results
data included here can also be downloaded via the included script:

    $ download_selected_data.sh --help
    download_selected_data.sh -v ver -n file_pair_count -k keygen -p profile   # ver = V4r4, V4r5, etc., -n 3 default

For example:

    $ download_selected_data.sh -v V4r5 -n 3 -k /usr/local/bin/aws-login-pub.darwin.amd64 -p saml-pub

See `download_selected_data.sh` for the default selected data types.
