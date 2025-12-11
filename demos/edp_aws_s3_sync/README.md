# AWS S3 sync tests

Tests `aws_s3_sync` utility modes: upload (local->remote), download
(remote->local) and S3 copy (remote->remote).

A circularity test can be performed by running the three test cases in
sequence: upload, copy within AWS, and download:

    - `edp_aws_s3_sync_local_remote.sh`
    - `edp_aws_s3_sync_remote_remote.sh`
    - `edp_aws_s3_sync_remote_local.sh'

Note that the above tests require an AWS account with login
privileges.

