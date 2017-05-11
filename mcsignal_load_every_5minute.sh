#!/usr/bin/ksh
#! author: created by xiaojp @ 20170120
#! sh /opt/etl/mcsignal_load_every_5minute.sh 201701201020
#. ~/.bash_profile
#set -x
# init variable
log_dir=/opt/etl/logs
log_file=mcsignal_load_every_5minute
src_file_dir207=/opt/bassfs/mcdata207
src_file_dir226=/opt/bassfs/mcdata226
dst_file_dir=/opt/bassfs/mcdata
curexectime=$1
endtime=205012312355

function check_cat {
		_IFC_MIN=$1
		_IFC_CNT=0
		
		for loop in A94001 A94002 A94003 A94004 A94005 A94006 A94007; do
		    CHK_FILE=${src_file_dir207}/${loop}${_IFC_MIN}.CHK   # check file
		    LOOP_CNT=0
		    while [[ ${LOOP_CNT} -lt 50 ]]
		    do 
		    		if [[ -f ${CHK_FILE} ]]; then
		    				write_log "cat file ${src_file_dir207}/${loop}${_IFC_MIN}*.AVL to ${dst_file_dir}/${loop}${_IFC_MIN}.AVL. "
		    				cat ${src_file_dir207}/${loop}${_IFC_MIN}*.AVL | awk -v loop="${loop}" 'BEGIN{FS=OFS=",";} {print loop,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13}'> ${dst_file_dir}/${loop}${_IFC_MIN}.AVL
		    				chmod 777 ${dst_file_dir}/${loop}${_IFC_MIN}.AVL
		    				break
		    		else
		    		    write_log "loop_cnt=${LOOP_CNT} Waiting for ${CHK_FILE}. "
		    		    LOOP_CNT=`expr ${LOOP_CNT} + 1`
		    		    sleep 6
		    		fi 
		    done 
		
		    CHK_FILE=${src_file_dir226}/${loop}${_IFC_MIN}.CHK   # check file
		    LOOP_CNT=0
		    while [[ ${LOOP_CNT} -lt 50 ]]
		    do 
		        if [[ -f ${CHK_FILE} ]]; then
		        		write_log "cat file ${src_file_dir226}/${loop}${_IFC_MIN}*.AVL to ${dst_file_dir}/${loop}${_IFC_MIN}.AVL. "
		        		cat ${src_file_dir226}/${loop}${_IFC_MIN}*.AVL | awk -v loop="${loop}" 'BEGIN{FS=OFS=",";} {print loop,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13}'>> ${dst_file_dir}/${loop}${_IFC_MIN}.AVL
		        		chmod 777 ${dst_file_dir}/${loop}${_IFC_MIN}.AVL
		        		break
		        else
		            write_log "loop_cnt=${LOOP_CNT} Waiting for ${CHK_FILE}. "
		            LOOP_CNT=`expr ${LOOP_CNT} + 1`
		        sleep 6
		        fi 
		    done 
		
		    if [[ ${LOOP_CNT} -eq 50 ]]; then
		        return 30
		    fi
		    _IFC_CNT=`expr ${_IFC_CNT} + 1`
		done
		
		write_log "check_cat ${_IFC_MIN} executed successfully. "
		return ${_IFC_CNT}
}

function produce_ctlfile(){ 
		port=$1
		gbase_load_files="${dst_file_dir}/A94001${curexectime}.AVL,${dst_file_dir}/A94002${curexectime}.AVL,${dst_file_dir}/A94003${curexectime}.AVL,${dst_file_dir}/A94004${curexectime}.AVL,${dst_file_dir}/A94005${curexectime}.AVL,${dst_file_dir}/A94006${curexectime}.AVL,${dst_file_dir}/A94007${curexectime}.AVL"
		echo "[load_A9400X_SIGNAL_A_TTACHV2_${curexectime}]" > /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl
		echo "disp_server=10.174.64.36:${port}" >> /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl
		echo "file_list=$gbase_load_files" >> /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl
		echo "format=3" >> /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl
		echo "db_name=DWIFC" >> /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl
		echo "table_name=A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP" >> /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl
		echo "delimiter=','" >> /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl
		echo "extra_loader_args=--def_date_format="%Y%m%d" --parallel=4  --def_datetime_format=%Y-%m-%d-%H.%i.%S.%f" >> /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl
		echo "hash_parallel=4" >>  /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl
		echo "max_error_records=0" >>  /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl
		cat /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl >> ${log_dir}/${log_file}.${curday}.log
}

function gbase_load(){ 
		write_log "check and start gbase dispatchserver:"
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
		write_log "gbase dispatch server is already started,port is $prots"
		write_log "GBASE LOAD START:"
		write_log "start run sql: drop table DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP"
		gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e "drop table DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP"
		write_log "start run sql: create table DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP like DWIFC.A9400x_yyyymmddhhmm"
		gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e "create table DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP like DWIFC.A9400x_yyyymmddhhmm"
		if [ $? -ne 0 ];
		then
				write_log "create DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP fail!!!"
				return 1
		else
				write_log "create DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP success!!!"
		fi
		write_log "start run sql: drop table DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}"
	  gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e "drop table DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}"

		write_log "start produce control file: produce_ctlfile $prots"
		produce_ctlfile $prots
		write_log "start load $loadtb: /opt/dispatch_server/dispcli -h 10.174.64.21,10.174.64.2,10.174.64.10  --log-file=${log_dir}/sidpcli_A9400X_SIGNAL_A_TTACHV2_${curexectime}.log  /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl -u aibass -p aigbase123$"
		/opt/dispatch_server/dispcli -h 10.174.64.21,10.174.64.2,10.174.64.10  --log-file=${log_dir}/sidpcli_A9400X_SIGNAL_A_TTACHV2_${curexectime}.log  /opt/etl/ctl/A9400X_SIGNAL_A_TTACHV2_${curexectime:0:8}.ctl -u aibass -p aigbase123$ > /dev/null
		if [ $? -ne 0 ];
		then
				write_log "load DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP fail!!!"
				return 1
		else
		    loadednum=$(tail -1 ${log_dir}/sidpcli_A9400X_SIGNAL_A_TTACHV2_${curexectime}.log |awk '{print $9}')
				write_log "load DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP records: $loadednum"
				rm -f ${log_dir}/sidpcli_A9400X_SIGNAL_A_TTACHV2_${curexectime}.log
		fi
		write_log "start run sql: rename table DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP to A9400X_SIGNAL_A_TTACHV2_${curexectime}"
		gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e "alter table  DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP  rename DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}"
	  if [ $? -ne 0 ];
		then
				write_log "rename table DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP to A9400X_SIGNAL_A_TTACHV2_${curexectime} fail!!!"
				return 1
		else
				write_log "rename table DWIFC.A9400X_SIGNAL_A_TTACHV2_${curexectime}_TEMP to A9400X_SIGNAL_A_TTACHV2_${curexectime} success!!!"
				load_end_time=$(date +%F\ %T)
				return 0
		fi
}

function rm_file(){
	  write_log "rm -f ${src_file_dir207}/A94001${curexectime}*  ${src_file_dir207}/A94002${curexectime}*  ${src_file_dir207}/A94003${curexectime}*  ${src_file_dir207}/A94004${curexectime}*  ${src_file_dir207}/A94005${curexectime}*  ${src_file_dir207}/A94006${curexectime}*  ${src_file_dir207}/A94007${curexectime}*"
	  write_log "rm -f ${src_file_dir226}/A94001${curexectime}*  ${src_file_dir226}/A94002${curexectime}*  ${src_file_dir226}/A94003${curexectime}*  ${src_file_dir226}/A94004${curexectime}*  ${src_file_dir226}/A94005${curexectime}*  ${src_file_dir226}/A94006${curexectime}*  ${src_file_dir226}/A94007${curexectime}*"
	  write_log "rm -f ${dst_file_dir}/A94001${minute120ago}*  ${dst_file_dir}/A94002${minute120ago}*  ${dst_file_dir}/A94003${minute120ago}*  ${dst_file_dir}/A94004${minute120ago}*  ${dst_file_dir}/A94005${minute120ago}*  ${dst_file_dir}/A94006${minute120ago}*  ${dst_file_dir}/A94007${minute120ago}*"
		rm -f ${src_file_dir207}/A94001${curexectime}*  ${src_file_dir207}/A94002${curexectime}*  ${src_file_dir207}/A94003${curexectime}*  ${src_file_dir207}/A94004${curexectime}*  ${src_file_dir207}/A94005${curexectime}*  ${src_file_dir207}/A94006${curexectime}*  ${src_file_dir207}/A94007${curexectime}*
		rm -f ${src_file_dir226}/A94001${curexectime}*  ${src_file_dir226}/A94002${curexectime}*  ${src_file_dir226}/A94003${curexectime}*  ${src_file_dir226}/A94004${curexectime}*  ${src_file_dir226}/A94005${curexectime}*  ${src_file_dir226}/A94006${curexectime}*  ${src_file_dir226}/A94007${curexectime}*
		minute120ago=$(get_min ${curexectime} -120)
		rm -f ${dst_file_dir}/A94001${minute120ago}*  ${dst_file_dir}/A94002${minute120ago}*  ${dst_file_dir}/A94003${minute120ago}*  ${dst_file_dir}/A94004${minute120ago}*  ${dst_file_dir}/A94005${minute120ago}*  ${dst_file_dir}/A94006${minute120ago}*  ${dst_file_dir}/A94007${minute120ago}*
}

function insert_load_info(){
	  flag=$1
	  write_log "gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e \"delete from dwpub.mcsignal_load_info where rpt_id='DWIFC.A9400X_SIGNAL_A_TTACHV2' and data_date='${curexectime}'\""
	  gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e "delete from dwpub.mcsignal_load_info where rpt_id='DWIFC.A9400X_SIGNAL_A_TTACHV2' and data_date='${curexectime}'"  2>&1 > /dev/null
	  if [ $flag -eq 0 ];then
	      write_log "gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e \"insert into dwpub.mcsignal_load_info values ('DWIFC.A9400X_SIGNAL_A_TTACHV2','${curexectime}','$catfilestarttime','$catfileendtime','$load_end_time',${loadednum})\""
	  		gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e "insert into dwpub.mcsignal_load_info values ('DWIFC.A9400X_SIGNAL_A_TTACHV2','${curexectime}','$catfilestarttime','$catfileendtime','$load_end_time',${loadednum})"  2>&1 > /dev/null
	  elif [ $flag -eq 1 ]; then
	      write_log "gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e \"insert into dwpub.mcsignal_load_info select rpt_id,'${curexectime}',CATFILESTARTTIME,CATFILEENDTIME,LOADENDTIME,LOADEDRECORD from dwpub.mcsignal_load_info where data_date='$lastexectime'\""
	      gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e "insert into dwpub.mcsignal_load_info select rpt_id,'${curexectime}',CATFILESTARTTIME,CATFILEENDTIME,LOADENDTIME,LOADEDRECORD from dwpub.mcsignal_load_info where data_date='$lastexectime'"  2>&1 > /dev/null
	  fi
	  write_log "start remove file:"
	  rm_file
	  #判断上一个点标识是否存在,201702082220出现过标识不存在导致调度未跑,故增加该判断
	  write_log "start check last time point flag"
	  write_log "gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -Ns -e \"SELECT count (1) FROM dwpub.mcsignal_load_info WHERE rpt_id='DWIFC.A9400X_SIGNAL_A_TTACHV2' AND data_date='${lastexectime}'\""
	  gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -Ns -e "SELECT count (1) FROM dwpub.mcsignal_load_info WHERE rpt_id='DWIFC.A9400X_SIGNAL_A_TTACHV2' AND data_date='${lastexectime}'">/opt/etl/logs/A9400X_SIGNAL_A_TTACHV2.lst
	  interface_FLAG=`cat /opt/etl/logs/A9400X_SIGNAL_A_TTACHV2.lst|tr -d ' '`
	  write_log "last time point flag is $interface_FLAG"
	  if [[ $interface_FLAG -eq 0 ]]; then
	      write_log "use current time point to create last time point flag"
	  		gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e "insert into dwpub.mcsignal_load_info values ('DWIFC.A9400X_SIGNAL_A_TTACHV2','${lastexectime}','$catfilestarttime','$catfileendtime','$load_end_time',${loadednum})"  2>&1 > /dev/null
	  fi 
}

function write_log() {
		LOG_INFO=$1
		curday=${curexectime:0:8}
		echo "[`date +%Y-%m-%d` `date +%H:%M:%S`]:		${LOG_INFO}" >> ${log_dir}/${log_file}.${curday}.log
		echo $LOG_INFO
}

function get_min(){
	  curtime=$1
		offset=$2
		convtimestamp="${curtime:0:4}-${curtime:4:2}-${curtime:6:2} ${curtime:8:2}:${curtime:10:2}:00"
		echo `date -d "$offset minute $convtimestamp "  +%Y%m%d%H%M`
}

function sendsms() {
		warningMsg=$1
		curdate=`date +%Y%m%d%H%M%S`
		db2 connect to ynbiyj user biyjinst using bass1gen_db 
		db2 -tx "
				insert into bass15.etl_sms(phone,content,status,level,starttime) 
				select phone,'${warningMsg}','0','9','${curdate}'
				from bass15.BASS1_DEVELOP_USER_SMS where level in ('7')
	  "
		db2 terminate
}

function create_fake_data {
    _CURT_TIME=$1
    _LAST_TIME=$2
    
    write_log "start create fake data:"
    write_log "gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e  \"drop table DWIFC.A9400X_SIGNAL_A_TTACHV2_${_CURT_TIME}\" 2>&1 > /dev/null"
    write_log "gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e  \"create table DWIFC.A9400X_SIGNAL_A_TTACHV2_${_CURT_TIME} as select * from DWIFC.A9400X_SIGNAL_A_TTACHV2_${_LAST_TIME}\" 2>&1 > /dev/null"
    gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e  "drop table DWIFC.A9400X_SIGNAL_A_TTACHV2_${_CURT_TIME}" 2>&1 > /dev/null
    gccli -uaibass -paigbase123$ -D"DWIFC" -h10.174.64.1 -vvv -e  "create table DWIFC.A9400X_SIGNAL_A_TTACHV2_${_CURT_TIME} as select * from DWIFC.A9400X_SIGNAL_A_TTACHV2_${_LAST_TIME}" 2>&1 > /dev/null
    if [[ $? -ne 0 ]]; then
        write_log "create fake data DWIFC.A9400X_SIGNAL_A_TTACHV2_${_CURT_TIME} failed. "
        return 30
    fi
    return 0
}

while [[ ${curexectime} -le ${endtime} ]]; do 
                
		lastexectime=$(get_min ${curexectime} -5)
		nextexectime=$(get_min ${curexectime} 5)
		write_log "\************************************************************"
		write_log "start exec: current exec time is $curexectime,last exec time is $lastexectime,next exec time is $nextexectime"
		## 1. check_cat
		write_log "call function check_cat ${curexectime}."
		catfilestarttime=$(date +%F\ %T)
		check_cat ${curexectime}
		if [[ $? -ne 7 ]]; then
		    write_log "error:function check_cat ${curexectime} return failed. "
		    create_fake_data ${curexectime} ${lastexectime}
		    if [ $? -ne 0 ]; then
		    		write_log "MC口信令【${curexectime}】造数据报错,程序退出,需马上核实!!!!"
		    		sendsms "MC口信令【${curexectime}】造数据报错,程序退出,需马上核实!!!!"
		    		exit -1
		    else
		        write_log "MC口信令【${curexectime}】数据cat失败,已使用【$lastexectime】造当前数据,需尽快核实原因!!!!"
		        sendsms "MC口信令【${curexectime}】数据cat失败,已使用【$lastexectime】造当前数据,需尽快核实原因!!!!"
		    fi
		    write_log "insert load flag:${curexectime}"
		    insert_load_info 1
		    write_log "stat_time=${curexectime} exec successfully with warning. 5s later to continue $nextexectime"
		    curexectime=$nextexectime
		    sleep 5
		    continue
		fi
		catfileendtime=$(date +%F\ %T)
		## 2. interface load
		write_log "[2] calling function gbase_load ${curexectime}. "  
		gbase_load
		
		if [[ $? -ne 0 ]]; then
		    create_fake_data ${curexectime} ${lastexectime}
		    if [ $? -ne 0 ]; then
		    		write_log "MC口信令【${curexectime}】造数据报错,程序退出,需马上核实!!!!"
		    		sendsms "MC口信令【${curexectime}】造数据报错,程序退出,需马上核实!!!!"
		    		exit -1
		    else
		        write_log "MC口信令【${curexectime}】数据load失败,已使用【$lastexectime】造当前数据,需尽快核实原因!!!!"
		        sendsms "MC口信令【${curexectime}】数据load失败,已使用【$lastexectime】造当前数据,需尽快核实原因!!!!"
		    fi
		    write_log "insert load flag:${curexectime}"
		    insert_load_info 1
		    write_log "stat_time=${curexectime} exec successfully with warning. 5s later to continue $nextexectime"
		    curexectime=$nextexectime
		    sleep 5
		    continue
		fi
		loadendtime=$(date +%F\ %T)
		write_log "insert load flag:${curexectime}"
		insert_load_info 0
		write_log "stat_time=${curexectime} exec successfully . 5s later to continue $nextexectime"
		curexectime=$nextexectime
		sleep 5
done

exit 0
