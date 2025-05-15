#!/bin/bash
cd /tmp

# for V4, requires about 70 gb

aws s3 sync s3://ecco-model-granules/V4r5_20250417/diags_monthly/OBPGMAP_mon_mean OBPGMAP_mon_mean 
aws s3 sync s3://ecco-model-granules/V4r5_20250417/diags_daily/OBPGMAP_day_mean OBPGMAP_day_mean 
aws s3 sync s3://ecco-model-granules/V4r5_20250417/diags_inst/OBPGMAP_day_snap OBPGMAP_day_snap 

aws s3 sync s3://ecco-model-granules/V4r5_20250417/diags_monthly/OBP_mon_mean OBP_mon_mean 
aws s3 sync s3://ecco-model-granules/V4r5_20250417/diags_daily/OBP_day_mean OBP_day_mean 
aws s3 sync s3://ecco-model-granules/V4r5_20250417/diags_inst/OBP_day_snap OBP_day_snap 
