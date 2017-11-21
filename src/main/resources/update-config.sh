#!/usr/bin/env bash

log_path=/home/xiangk/workspace/crontest
property_path=${log_path}/config
scrip_path=${log_path}/cron-job.sh
echo "start crontab update job..."
add_month()
{
    new_config=$(date -d "$1 +1month" +'%Y%m%d')
    echo ${new_config}
}

# log file format: log_%Y-%m-%d
last_log_path=$(ls ${log_path}/log* -t | head -1)
last_file_name=$(basename ${last_log_path})
# split file name by underscore
last_run_date=$(echo ${last_file_name} | awk -F "_" '{print $2}')
last_run_stamp=$(date +%s -d "$last_run_date")
now_stamp=$(date +%s)
echo "now_date: $(date +'%Y%m%d'), last_run_date: $last_run_date"
pass_days=$(((now_stamp-last_run_stamp)/86400))
if [ ${pass_days} -gt 30 ]; then
    echo "passed 30 days, updating config $property_path..."
    last_config=$(sed '/^start_date.*/!d;s/.*= //' ${property_path})
    echo "last_config: $last_config"

    update_config=`add_month ${last_config}`
    sed -i "s/^start_date.*/start_date = $update_config/" ${property_path}
    echo "update config: $update_config, and execute cron-job script..."
    bash ${scrip_path}
else
    echo "do nothing."
fi





