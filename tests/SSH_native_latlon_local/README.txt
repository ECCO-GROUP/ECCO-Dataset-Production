# two steps

# 1. create task list 
./edp_create_job_task_list_SSH_native_latlon_mon_mean.sh SSH_native_latlon_mon_mean_jobs.txt SSH_native_latlon_mon_mean_tasks.json.sav 

# 2. generate data products from task list
./edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh SSH_native_latlon_mon_mean_tasks.json.sav
