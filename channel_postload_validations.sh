#!/bin/bash

set -x

basedir=/tmp
tmpstmp=`date +"%m-%d-%Y-%H-%M"`

file_name=channel.csv
schema_file_name=channel.schema

cd /tmp
hadoop fs -get /lc/apps/file-import-service/scripts/${schema_file_name}

header=`cat /tmp/${schema_file_name}| grep -i header| cut -d"|" -f2`

channel_loaded=`hadoop fs -ls /import/root/validate/marketing/base/report/channel.csv* | awk -F" " '{print $6" "$7" "$8}'| sort -nr | head -1| cut -d" " -f3`

if [ -z $channel_loaded ]; then
   echo "No files in archive"
   exit 0
fi

file_cnt=`hadoop fs -cat ${channel_loaded} | grep -vi $header| wc -l`
echo $file_cnt

##cleanup flags
hadoop fs -rmr /data/lc/marketing/channel_validations/_CHN_WRN
hadoop fs -rmr /data/lc/marketing/channel_validations/_CHN_NRML 

if [ -n "${HADOOP_TOKEN_FILE_LOCATION}" ]
then
    tokenParam="--hiveconf mapreduce.job.credentials.binary=${HADOOP_TOKEN_FILE_LOCATION} --hiveconf tez.credentials.path=${HADOOP_TOKEN_FILE_LOCATION}"
fi

## warn_reason 01 
tbl_cnt=`hive ${tokenParam} --hiveconf hive.execution.engine=mr —S -e "analyze table marketing.channel_details compute statistics; select count(*) from marketing.channel_details;"`

if [ $file_cnt -ne $tbl_cnt ] ; then
   warn_reason="File count, Table count mismatch"
fi

## warn_reason 02
prev_tbl_cnt=`hive ${tokenParam} --hiveconf hive.execution.engine=mr —S -e "analyze table interim.channel_details_prev compute statistics; select count(*) from interim.channel_details_prev;"`


## warn_reason 03
dup_chk=`hive ${tokenParam} --hiveconf hive.execution.engine=mr —S -e "select referer_id,count(*) from marketing.channel_details group by referer_id having count(*) > 1 limit 5;"`

if [[ ! -z $dup_chk ]] ; then
   warn_reason+="Dups in file ! revert to older channel details content"
   hive ${tokenParam} --hiveconf hive.execution.engine=mr —S -e "insert overwrite table marketing.channel_details select * from interim.channel_details_prev;"
fi

if [[ ! -z "$warn_reason" ]] ; then
	hadoop fs -touchz /data/lc/marketing/channel_validations/_CHN_WRN
	echo "Warn_Reason=$warn_reason"
	exit 0
else
	hadoop fs -touchz /data/lc/marketing/channel_validations/_CHN_NRML
	exit 0
fi
