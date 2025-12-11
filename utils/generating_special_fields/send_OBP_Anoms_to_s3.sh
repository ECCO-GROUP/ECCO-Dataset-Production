#!/bin/bash


# delete whatever is in the Anomaly buckets now
aws s3 rm s3://ecco-model-granules/V4r5_20250417/diags_monthly/OBPGMAPA_mon_mean --recursive
aws s3 rm s3://ecco-model-granules/V4r5_20250417/diags_daily/OBPGMAPA_day_mean   --recursive
aws s3 rm s3://ecco-model-granules/V4r5_20250417/diags_inst/OBPGMAPA_day_snap  --recursive

aws s3 rm s3://ecco-model-granules/V4r5_20250417/diags_monthly/OBPAnoma_mon_mean --recursive
aws s3 rm s3://ecco-model-granules/V4r5_20250417/diags_daily/OBPAnoma_day_mean --recursive
aws s3 rm s3://ecco-model-granules/V4r5_20250417/diags_inst/OBPAnoma_day_snap  --recursive

# now sync the new anomaly files
cd /tmp

rm send_log*txt

aws s3 sync OBPGMAPA_mon_mean s3://ecco-model-granules/V4r5_20250417/diags_monthly/OBPGMAPA_mon_mean > send_log_OBPGMAPA_mon_mean.txt &
aws s3 sync OBPGMAPA_day_mean s3://ecco-model-granules/V4r5_20250417/diags_daily/OBPGMAPA_day_mean > send_log_OBPGMAPA_day_mean.txt &  
aws s3 sync OBPGMAPA_day_snap s3://ecco-model-granules/V4r5_20250417/diags_inst/OBPGMAPA_day_snap  > send_log_OBPGMAPA_day_snap.txt &

aws s3 sync OBPAnoma_mon_mean s3://ecco-model-granules/V4r5_20250417/diags_monthly/OBPAnoma_mon_mean > send_log_OBPAnoma_mon_mean.txt &
aws s3 sync OBPAnoma_day_mean s3://ecco-model-granules/V4r5_20250417/diags_daily/OBPAnoma_day_mean > send_log_OBPAnoma_day_mean.txt &
aws s3 sync OBPAnoma_day_snap s3://ecco-model-granules/V4r5_20250417/diags_inst/OBPAnoma_day_snap  > send_log_OBPAnoma_day_snap.txt &
