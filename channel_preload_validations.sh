#!/bin/bash

set -x

basedir=/tmp
tmpstmp=`date +"%m-%d-%Y-%H-%M"`

file_name=channel.csv
schema_file_name=channel.schema

if [ -n "${HADOOP_TOKEN_FILE_LOCATION}" ]
then
    tokenParam="--hiveconf mapreduce.job.credentials.binary=${HADOOP_TOKEN_FILE_LOCATION} --hiveconf tez.credentials.path=${HADOOP_TOKEN_FILE_LOCATION}"
fi

##cleanup flags
hadoop fs -rmr /data/lc/marketing/channel_validations/_CHN_PRE_WRN
hadoop fs -rmr /data/lc/marketing/channel_validations/_CHN_PRE_NRML

rec_cnt=`hadoop fs -cat /import/root/source/marketing/base/report/${file_name} | wc -l`
echo $rec_cnt

if [ $rec_cnt -gt 1 ]; then
	cd /tmp
	rm -rf /tmp/${file_name}
	rm -rf /tmp/${schema_file_name}
	hadoop fs -get /import/root/source/marketing/base/report/${file_name}
	hadoop fs -get /lc/apps/file-import-service/scripts/${schema_file_name}	
else 
   echo "${file_name} has nothing to load"
   exit 0
fi   

delimiter=`cat /tmp/channel.schema| grep -i Delimiter| cut -d"|" -f2`
nooffields=`cat /tmp/channel.schema| grep -i colCount| cut -d"|" -f2`
header=`cat /tmp/channel.schema| grep -i header| cut -d"|" -f2`
field_type=`cat /tmp/channel.schema| grep -i type| cut -d"|" -f2`
f1_type=`echo $field_type | cut -d"," -f1`
f3_type=`echo $field_type | cut -d"," -f3`

delimiter_match=`awk -F"${delimiter}" '{print NF}' /tmp/${file_name} | grep -v $nooffields` ##delimiter match
header_match=`head -1 /tmp/${file_name} | grep -i $header` ##header match

if [ $f1_type == "int" ] ; then
   f1_validation=`awk -F"," '$1 ~ /^[0-9][0-9]*$/ {count++} END{ print count+1 }' /tmp/${file_name}`
fi

if [[ ! -z $delimiter_match ]] ; then
   pre_warn_reason="Delimiter mismatch"
fi

if [[ -z $header_match ]] ; then
   pre_warn_reason+=" | Header mismatch"
fi

if [[ $f1_validation -ne $rec_cnt ]] ; then
   pre_warn_reason+=" | F1:${f1_type} validation failure"
fi


if [[ -z $delimiter_match ]] && [[ ! -z $header_match ]] && [[ $f1_validation -eq $rec_cnt ]] ; then
   echo "****** ${file_name} passed validations ******"
   hive ${tokenParam} --hiveconf hive.execution.engine=mr —S -e "use interim; drop table if exists channel_details_prev; create table interim.channel_details_prev as select * from marketing.channel_details;"
   hadoop fs -touchz /data/lc/marketing/channel_validations/_CHN_PRE_NRML
   exit 0
else
   echo "****** ${file_name} did not pass validations ******"
   hive ${tokenParam} --hiveconf hive.execution.engine=mr —S -e "use interim; drop table if exists channel_details_prev; create table interim.channel_details_prev as select * from marketing.channel_details;"
   hadoop fs -touchz /data/lc/marketing/channel_validations/_CHN_PRE_WRN
   echo "Pre_Warn_Reason=$pre_warn_reason"
   exit 0
fi
