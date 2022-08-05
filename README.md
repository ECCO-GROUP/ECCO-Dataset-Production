# ECCO-Dataset-Production
Repo with codes to turn raw model output into glorious self-describing granules for widespread distribution

Developed by Duncan Bark, in collaboration with Dr. Ian Fenty, in Summer 2022

## **Structure**
    /aws/
        configs/
        ecco_grids/
        logs/
        mapping_factors/
        metadata/
        src/
        tmp/
        ecr_push.sh
    /upload_to_AWS/

## **Descriptions**
### **aws/**
- Contains all the code, data, metadata, config, etc. files necessary for processing of ECCO datasets.
- See the README.md in aws/ for more information on the directories and files within.

### **upload_to_AWS/**
- Contains scripts that allow the user to easily upload files from a local directory to an AWS S3 bucket.
- See the README.md in upload_to_AWS/ for more information on the directories and files within.