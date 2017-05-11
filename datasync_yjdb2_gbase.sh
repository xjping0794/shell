#!/bin/ksh
# ./datasync_yjdb2_gbase.sh -s srcname -t yyyymmdd -u 1001 -v 1001 -p gd|dg
#  sh datasync_yjdb2_gbase.sh -s test_02008_stat_yyyymmdd -t 20170102 -u 1001 -v 1001 -p dg
#set -x
############################################################################
#   creator:   xiaojp@20170104
#   使用须知： 需在 db2&gbase 中提前创建模板表，且模板表是tablename对应字段
#              目前支持日期表-单表、单表-单表、日期表-日期表间数据倒换
#              暂不支持增量数据倒换
#   模板表
#   create table DWPUB.SYNC_GBASE_CONFIG
#   (
#   rpt_id varchar(100),
#   tablename varchar(100),
#   schema varchar(10),
#   distributekey varchar(30),
#   DELIMITER varchar(10)
#   ) distribute by (rpt_id) in tbs_bass15
#   insert into DWPUB.SYNC_GBASE_CONFIG values ('test_02008_stat','test_02008_stat_yyyymmdd','bass15','USER_ID','01')
#   insert into DWPUB.SYNC_GBASE_CONFIG values ('test_02008_stat_yyyymmdd','test_02008_stat_yyyymmdd','bass15','USER_ID','01')
#   日志表
#   create table dwpub.sync_table_log
#   (
#   tablename varchar(100),
#   proc_date varchar(10),
#   export_start_time varchar(30),
#   export_end_time varchar(30),
#   load_end_time varchar(30),
#   exportnum bigint,
#   loadednum bigint,
#   type varchar(10)
#   )distribute by (tablename) in tbs_bass15
############################################################################
AGENT_LOG_PATH="/home/aibass/agent207/log/applog"

if [ $# -ne 10 ]; then
    echo "please input param: $0 -s srcname -d dstname -t yyyymmdd -u 1001 -v 1001 -p gd|dg"
    exit -1
fi
# get params
while [ $# -ne 0 ] ; do
		if [ x$1 == "x-t" ] ; then
				shift
				procdate=$1
				shift
		elif [[ x$1 == "x-s" ]]; then
		    shift
		    srcname=$1
		    shift
		elif [[ x$1 == "x-p" ]]; then
		    shift
		    synctype=$1
		    shift
		elif [[ x$1 == "x-u" ]]; then
		    shift
		    U_CODE=$1
		    shift
		elif [[ x$1 == "x-v" ]]; then
		    shift
		    V_CODE=$1
		    shift
		else
		   # print_trace "Unrecorgnized prarmter $1, ignore. "
		    shift
		fi
done
LOG_PATH="/opt/datasync/log/${procdate}"

if [ ! -d "$LOG_PATH" ]; then
		mkdir -p $LOG_PATH
fi
currenttimestamp=$(date +%Y%m%d%H%M%S)
curday=$(date +%Y%m%d)
function WRITE_LOG() {
		LOG_INFO=$1
		echo "[`date +%Y-%m-%d` `date +%H:%M:%S`]:		${LOG_INFO}" >> ${LOG_PATH}/${srcname}.${curday}.${currenttimestamp}.log
		echo $LOG_INFO
}

function WRITE_SS_LOG() {
if [ Z$1 == Z1 ];then
    echo "${U_CODE}.${V_CODE}.$1" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
    echo "" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
    echo "[RUNNING]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:${srcname}:$2" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
elif [ Z$1 == Z3 ];then
    echo "${U_CODE}.${V_CODE}.$1" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
    echo "" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
    echo "[SUCESS]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:${srcname}:$2" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
elif [ Z$1 == Z4 ];then
    echo "${U_CODE}.${V_CODE}.$1" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
    echo "" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
    echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:${srcname}:$2" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
else
    echo "${U_CODE}.${V_CODE}.4" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
    echo "" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
    echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:${srcname}:$2" >> ${AGENT_LOG_PATH}/${srcname}_${U_CODE}_${V_CODE}.log
fi
}

function insert_sync_log(){
		db2 connect to ynbiyj user biyjinst using bass1gen_db
		db2 -tx "insert into dwpub.sync_table_log values('$srcname','$procdate','$export_start_time','$export_end_time','$load_end_time','$exportnum','$loadednum','$synctype')"
		db2 terminate
}

function sendsms() {
		warningMsg=$1
		curdate=`date +%Y%m%d%H%M%S`
		db2 connect to ynbiyj user biyjinst using bass1gen_db 
		db2 -tx "
				insert into bass15.etl_sms(phone,content,status,level,starttime) 
				select phone,'${warningMsg}','0','9','${curdate}'
				from bass15.BASS1_DEVELOP_USER_SMS where level in ('7','9')
	  "
		db2 terminate
}

function db2_export(){
		schema=$1
		exporttb=$2
		delimiter=$3
		db2 connect to ynbiyj user biyjinst using bass1gen_db
		WRITE_LOG "start export ${schema}.${exporttb}: export to /opt/datasync/tmpdata/${srcname}.txt of del modified by COLDEL0x${delimiter} timestampformat=\"YYYY-MM-DD HH:MM:SS\" nochardel decplusblank select * from ${schema}.${exporttb}"
		db2 "export to /opt/datasync/tmpdata/${srcname}.txt of del modified by COLDEL0x${delimiter} timestampformat=\"YYYY-MM-DD HH:MM:SS\" nochardel decplusblank select * from ${schema}.${exporttb}" >>${LOG_PATH}/export_${srcname}_${currenttimestamp}
		if [ $? -ne 0 ];
		then
				WRITE_LOG "export $exporttb fail!!!"
				return 1
		else
		    exportnum=$(tail -2 ${LOG_PATH}/export_${srcname}_${currenttimestamp}|awk -F':' '{print $2}'|tr -d ' \n')
		    rm -f ${LOG_PATH}/export_${srcname}_${currenttimestamp}
				WRITE_LOG "export $exporttb records: $exportnum"
				return 0
		fi
}

function db2_load(){
		schema=$1
		loadtb=$2
		delimiter=$3
		part_keys=$4
		template_table=$5
		db2 connect to ynbiyj user biyjinst using bass1gen_db
		WRITE_LOG "start run sql: drop table ${schema}.${loadtb}_TEMP"
		db2 -tx "drop table ${schema}.${loadtb}_TEMP"
		WRITE_LOG "start run sql: create table ${schema}.${loadtb}_TEMP like ${schema}.${template_table} in tbs_bass15 index in tbs_bass15 partitioning key(${part_keys}) using hashing compress yes"
		db2 -tx "create table ${schema}.${loadtb}_TEMP like ${schema}.${template_table} in tbs_bass15 index in tbs_bass15 partitioning key(${part_keys}) using hashing compress yes"
		if [ $? -ne 0 ];
		then
				WRITE_LOG "create ${schema}.${loadtb}_TEMP fail!!!"
				return 1
		else
				WRITE_LOG "create ${schema}.${loadtb}_TEMP success!!!"
		fi
		WRITE_LOG "start run sql: drop table ${schema}.${loadtb}"
		db2 -tx "drop table ${schema}.${loadtb}"
		WRITE_LOG "start load ${loadtb}: LOAD client FROM /opt/datasync/tmpdata/${srcname}.txt of del modified by COLDEL0x${delimiter} nochardel decplusblank insert into ${schema}.${loadtb}_TEMP"
		db2 "LOAD client FROM /opt/datasync/tmpdata/${srcname}.txt of del modified by COLDEL0x${delimiter} nochardel decplusblank insert into ${schema}.${loadtb}_TEMP" >> ${LOG_PATH}/${srcname}.${curday}.${currenttimestamp}.log
		loadednum=$(cat ${LOG_PATH}/${srcname}.${curday}.${currenttimestamp}.log|grep -i 'Number of rows loaded'|tail -1|awk -F'=' '{print $2}'|tr -d ' ')
		if [ $exportnum -ne $loadednum ]; then
				WRITE_LOG "load ${schema}.${loadtb}_TEMP fail!!!"
				sendsms "表${srcname} gbase&db2数据${procdate}号倒换不一致,导出量：${exportnum} 加载量：${loadednum},请核实！"
				return 1
	  else
	      WRITE_LOG "load ${schema}.${loadtb}_TEMP records: $loadednum"
		fi
		WRITE_LOG "start run sql: rename table ${schema}.${loadtb}_TEMP to ${loadtb}"
		db2 -tx "rename table ${schema}.${loadtb}_TEMP to ${loadtb}"
		if [ $? -ne 0 ];
		then
				WRITE_LOG "rename table ${schema}.${loadtb}_TEMP to ${loadtb} fail!!!"
				return 1
		else
				WRITE_LOG "rename table ${schema}.${loadtb}_TEMP to ${loadtb} success!!!"
				load_end_time=$(date +%F\ %T)
				insert_sync_log
				return 0
		fi
}

function gbase_export(){
		schema=$1
		exporttb=$2
		delimiter=$3
		if [ -f "/opt/datasync/tmpdata/${srcname}.txt" ]; then
				rm -f /opt/datasync/tmpdata/${srcname}.txt
		fi
		WRITE_LOG "start export ${schema}.${exporttb}: gccli -c -uaibass -paigbase123$ -D\"${schema}\" -h10.174.64.15 -vvv -e \"rmt:select /*+ first_rows(10000) */ \* from ${schema}.${exporttb} into outfile '/opt/datasync/tmpdata/${srcname}.txt'  COLUMNS TERMINATED BY '\x${delimiter}'   null_value ''\" "
		gccli -c -uaibass -paigbase123$ -D"${schema}" -h10.174.64.15 -vvv -e "rmt:select /*+ first_rows(10000) */ * from ${schema}.${exporttb} into outfile '/opt/datasync/tmpdata/${srcname}.txt'  COLUMNS TERMINATED BY '\x${delimiter}'   null_value ''" >> ${LOG_PATH}/${srcname}.${curday}.${currenttimestamp}.log
		if [ $? -ne 0 ];
		then
				WRITE_LOG "export ${exporttb} fail!!!"
				return 1
		else
		    exportnum=$(cat ${LOG_PATH}/${srcname}.${curday}.${currenttimestamp}.log|grep -i 'Query OK'|awk '{print $3}'|tr -d ' ')
				WRITE_LOG "export $exporttb records: $exportnum"
				return 0
		fi
}


function produce_ctlfile(){
	  schema=$1
		loadtb=$2
		delimiter=$3
		port=$4
		echo "[load_${loadtb}]" > /opt/datasync/ctl/${loadtb}.ctl
		echo "disp_server=10.174.64.33:${port}" >> /opt/datasync/ctl/${loadtb}.ctl
		echo "file_list=/opt/datasync/tmpdata/${srcname}.txt" >> /opt/datasync/ctl/${loadtb}.ctl
		echo "format=3" >> /opt/datasync/ctl/${loadtb}.ctl
		echo "db_name=${schema}" >> /opt/datasync/ctl/${loadtb}.ctl
		echo "table_name=${loadtb}_TEMP" >> /opt/datasync/ctl/${loadtb}.ctl
		echo "delimiter='\x${delimiter}'" >> /opt/datasync/ctl/${loadtb}.ctl
		#echo "string_qualifier='\"'" >>  /opt/datasync/ctl/${loadtb}.ctl
		echo "extra_loader_args=--def_date_format="%Y%m%d" --parallel=4  --def_datetime_format=%Y-%m-%d-%H.%i.%S.%f" >> /opt/datasync/ctl/${loadtb}.ctl
		echo "hash_parallel=4" >>  /opt/datasync/ctl/${loadtb}.ctl
		echo "max_error_records=0" >>  /opt/datasync/ctl/${loadtb}.ctl
		cat /opt/datasync/ctl/${loadtb}.ctl >> ${LOG_PATH}/${srcname}.${curday}.${currenttimestamp}.log
}

function gbase_load(){
	  schema=$1
		loadtb=$2
		delimiter=$3
		template_table=$4
		port=$5
		WRITE_LOG "start run sql: drop table ${schema}.${loadtb}_TEMP"
		gccli -uaibass -paigbase123$ -D"${schema}" -h10.174.64.1 -vvv -e "drop table ${schema}.${loadtb}_TEMP"
		WRITE_LOG "start run sql: create table ${schema}.${loadtb}_TEMP like ${schema}.${template_table}"
		gccli -uaibass -paigbase123$ -D"${schema}" -h10.174.64.1 -vvv -e "create table ${schema}.${loadtb}_TEMP like ${schema}.${template_table}"
		if [ $? -ne 0 ];
		then
				WRITE_LOG "create ${schema}.${loadtb}_TEMP fail!!!"
				return 1
		else
				WRITE_LOG "create ${schema}.${loadtb}_TEMP success!!!"
		fi
		WRITE_LOG "start run sql: drop table ${schema}.${loadtb}"
	  gccli -uaibass -paigbase123$ -D"${schema}" -h10.174.64.1 -vvv -e "drop table ${schema}.${loadtb}"

		WRITE_LOG "start produce control file: produce_ctlfile $schema $loadtb $delimiter $port"
		produce_ctlfile $schema $loadtb $delimiter $port
		WRITE_LOG "start load $loadtb: /opt/dispatch_server/dispcli -h 10.174.64.21,10.174.64.2,10.174.64.10  --log-file=${LOG_PATH}/sidpcli_${loadtb}.log  /opt/datasync/ctl/${loadtb}.ctl -u aibass -p aigbase123$"
		/opt/dispatch_server/dispcli -h 10.174.64.21,10.174.64.2,10.174.64.10  --log-file=${LOG_PATH}/sidpcli_${loadtb}.log  /opt/datasync/ctl/${loadtb}.ctl -u aibass -p aigbase123$ > /dev/null
		if [ $? -ne 0 ];
		then
				WRITE_LOG "load ${schema}.${loadtb}_TEMP fail!!!"
				return 1
		else
		    loadednum=$(tail -1 ${LOG_PATH}/sidpcli_${loadtb}.log |awk '{print $9}')
				WRITE_LOG "load ${schema}.${loadtb}_TEMP records: $loadednum"
				rm -f ${LOG_PATH}/sidpcli_${loadtb}.log
		fi
		WRITE_LOG "start run sql: rename table ${schema}.${loadtb}_TEMP to ${loadtb}"
		gccli -uaibass -paigbase123$ -D"${schema}" -h10.174.64.1 -vvv -e "alter table  ${schema}.${loadtb}_TEMP  rename ${schema}.${loadtb}"
	  if [ $? -ne 0 ];
		then
				WRITE_LOG "rename table ${schema}.${loadtb}_TEMP to ${loadtb} fail!!!"
				return 1
		else
				WRITE_LOG "rename table ${schema}.${loadtb}_TEMP to ${loadtb} success!!!"
				load_end_time=$(date +%F\ %T)
				insert_sync_log
				return 0
		fi
}

WRITE_LOG "PROCESS START !"
WRITE_LOG "connect to ynbiyj ->from dwpub.sync_gbase_config get config info:${srcname}"
db2 connect to ynbiyj user biyjinst using bass1gen_db
db2 -tx "SELECT RPT_ID||','||SCHEMA||','||tablename||','||distributekey||','||DELIMITER FROM DWPUB.SYNC_GBASE_CONFIG WHERE upper(RPT_ID)=upper('${srcname}')"|while read record
do
		rpt_id=$(echo $record|awk -F "," '{print $1}'|tr [A-Z] [a-z] )
		schema=$(echo $record|awk -F "," '{print $2}')
		tablename=$(echo $record|awk -F "," '{print $3}'|tr [A-Z] [a-z])
		distributekey=$(echo $record|awk -F "," '{print $4}')
		delimiter=$(echo $record|awk -F "," '{print $5}')
		srctb=$rpt_id
		dsttb=$tablename
		if [[ $rpt_id =~ "_yyyy" ]]; then
				srctb=${rpt_id%%_yyyymmdd}
				srctb=${srctb%%_yyyymm}
				srctb=${srctb}_${procdate}
		fi

		if [[ $tablename =~ "_yyyy" ]]; then
				dsttb=${tablename%%_yyyymmdd}
				dsttb=${dsttb%%_yyyymm}
				dsttb=${dsttb}_${procdate}
		fi
    db2 terminate
		if [ $synctype == "gd" ]; then
		    export_start_time=$(date +%F\ %T)
				gbase_export $schema $srctb $delimiter
				if [ $? -ne 0 ]; then
				    WRITE_SS_LOG 4 "export $exporttb fail!!!"
						exit -1
				fi
				export_end_time=$(date +%F\ %T)
				WRITE_LOG "DB2 LOAD START:"
				db2_load $schema $dsttb $delimiter $distributekey $tablename
				if [ $? -ne 0 ]; then
				    WRITE_SS_LOG 4 "load $loadtb fail!!!"
						exit -1
				fi
				db2 terminate
		elif [ $synctype == "dg" ]; then
		    export_start_time=$(date +%F\ %T)
		    db2_export $schema $srctb $delimiter
		    if [ $? -ne 0 ]; then
		        WRITE_SS_LOG 4 "export $exporttb fail!!!"
						exit -1
				fi
				export_end_time=$(date +%F\ %T)
				db2 terminate
				WRITE_LOG "check and start gbase dispatchserver:"
				port="$(($RANDOM%10))"
				if [ ${port} -eq '0' ];then
						prots="6667"
				elif [ ${port} -eq '1' ];then
		        prots="6668"
				elif [ ${port} -eq '2' ];then
				    prots="6669"
				elif [ ${port} -eq '3' ];then
				    prots="6670"
				elif [ ${port} -eq '4' ];then
				    prots="6671"
				elif [ ${port} -eq '5' ];then
				    prots="6672"
				elif [ ${port} -eq '6' ];then
				    prots="6673"
				elif [ ${port} -eq '7' ];then
				    prots="6674"
				elif [ ${port} -eq '8' ];then
				    prots="6675"
				elif [ ${port} -eq '9' ];then
				    prots="6676"
				fi
				psnum_check=`ps -ef|grep "${prots}"|grep -cv "grep"`
		    if [ ${psnum_check} -eq '0' ] ;then
		    		/opt/dispatch_server/dispserver --log-file=/home/aibass/dispatcher_"${prots}".log --port="${prots}" --loader-log-dir=/home/aibass/loaderlog &
		    fi
				WRITE_LOG "GBASE LOAD START:"
		    gbase_load $schema $dsttb $delimiter $tablename $prots
		    if [ $? -ne 0 ]; then
		        WRITE_SS_LOG 4 "load $loadtb fail!!!"
						exit -1
				fi
		else
		    db2 terminate
		    echo "datasync only support db2 and gbase....."
		    WRITE_SS_LOG 4 "datasync only support db2 and gbase....."
		    exit -1
		fi
		if [ $exportnum -ne $loadednum ]; then
				sendsms "表${srcname} gbase&db2数据${procdate}号倒换不一致,导出量：${exportnum} 加载量：${loadednum},请核实！"
		fi
done
WRITE_SS_LOG 3 "${srctb} datasync load success!!!"
exit 0