#!/bin/bash
# arn:aws:iam::448078824696 is the "ecco_production" AWS account ID
# role/account_owner is Ian
# role/power_user is for non-Ian users
# role/lambda_role is what the lambda non-person has 

# Access-Key-Generation is a JPL github repo: 
# https://github.jpl.nasa.gov/cloud/Access-Key-Generation

## account_owner
# /home5/ifenty/git_repos_others/Access-Key-Generation/aws-login.py -pub -a arn:aws:iam::448078824696:role/account_owner

## power_user
python3 aws-login.py -pub -a arn:aws:iam::448078824696:role/power_user
