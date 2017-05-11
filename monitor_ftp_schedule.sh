#/usr/bin/ksh
#set -x
#writor/Create date:xiaojp/20170207
#===================================================================================================
# 功能描述： 20170207 监控大数据平台推送给外部平台的数据及时情况，并短信告警
#===================================================================================================
tracelogdir="/biyjetl/aibassyj/user/xiaojp/log"
getDay(){
		# $1 : source date:yyyymmdd
		# $2 : time interval :natural number should be lt 28 or gt -28
		# determine input parameter cannot be null
		if [ -z "$1" ]; then
				printf "please input date[yyyymmdd] "
				exit 1
		fi
		if [ -z "$2" ]; then
				printf "please input natural number,for example 5 "
				exit 1
		fi
		# determine the length of $1 ,value of $2
		length1=`expr length $1`
		if [ $length1 -ne 8 ]; then
				printf "date format is wrong,eg is yyyymmdd"
		fi
		if [ $2 -gt 28 -o $2 -lt -28 ]; then
				printf "time interval :natural number lt 28 day"
				exit 1
		fi
		if [ -n "$1" -a -n "$2" ]; then
		    month=`echo $1|cut -c 5-6`
		    day=`echo $1|cut -c 7-8`
		    year=`echo $1|cut -c 1-4`
		    # subtract  $2 from $1
		    day=`expr $day + $2`
		    #if $day gt monthbottom then determine the next month
		    #get day of the next month
		    case $month in
		    		04|06|09|11)
		    				if [ $day -gt 30 ];then
		    						day=`expr $day - 30`
		    						month=`expr $month + 1`
		    				else
		    						break
		    				fi;;
		    		01|03|05|07|08|10)
		    				if [ $day -gt 31 ]; then
		    						day=`expr $day - 31`
		    						month=`expr $month + 1`
		    				else
		    						break
		    				fi;;
		    		02)
		    				if [ `expr $year % 4` -eq 0 -a `expr $year % 100` -ne 0 -o `expr $year % 400` -eq 0 ]; then
		    				    if [ $day -gt 29 ]; then
		    				    		day=`expr $day - 29`
		    				    		month=`expr $month + 1`
		    				    else
		    				    		break
		    				    fi
		    				else
		    				    if [ $day -gt 28 ]; then
		    				     		day=`expr $day - 28`
		    				     		month=`expr $month + 1`
		    				    else
		    				    		break
		    				    fi
		    				fi;;
		    		12)
		       			if [ $day -gt 31 ]; then
		       					day=`expr $day - 31`
		       					month=01
		       					year=`expr $year + 1`
		       			else
		       					break
		       			fi;;
		    esac

		    # if the day is 0 then determine the last month
		    # get day of the previous month
		    if [ $day -eq 0 ]; then
		    		month=`expr $month - 1`
		        # if the month is 0 then it is dec 31
		    		if [ $month -eq 0 ]; then
		    				month=12
		    				day=31
		    				year=`expr $year - 1`
		    		else
		    				case $month in
		    						1|3|5|7|8|10) day=31;;
		    						4|6|9|11) day=30;;
		    						2)
		    						if [ `expr $year % 4` -eq 0 -a `expr $year % 100` -ne 0 -o `expr $year % 400` -eq 0 ]; then
		    								day=29
		    						else
		    								day=28
		    						fi
		    						;;
		    				esac
		    		fi
		    fi
		    if [ $day -lt 0 ]; then
		    		month=`expr $month - 1`
		    		if [ $month -eq 0 ]; then
		    				month=12
		    				day=`expr 31 + $day`
		    				year=`expr $year - 1`
		    		else
		    				case $month in
		    						1|3|5|7|8|10) day=`expr 31 + $day`;;
		    						4|6|9|11) day=`expr 30 + $day`;;
		    						2)
		    								if [ `expr $year % 4` -eq 0 -a `expr $year % 100` -ne 0 -o `expr $year % 400` -eq 0 ]; then
		    										day=`expr 29 + $day`
		    								else
		    										day=`expr 28 + $day`
		    								fi
		    						;;
		    				esac
		    		fi
		    fi
				daylen=`expr length $day`
				monthleg=`expr length $month`
				if [ $daylen -lt 2 ]; then
						day=0$day
				fi
				if [ $monthleg -lt 2 ]; then
						month=0$month
				fi
				echo ${year}${month}${day}
		fi
}
getMonth(){
		if [ -z "$1" ]; then
				printf "please input current month...."
				exit 1
		fi
		if [ -z "$2" ]; then
				printf "please input natural number,for example 5 "
				exit 1
		fi
		year=`echo $1|cut -c 1-4`
		month=`echo $1|cut -c 5-6`
		Ymonth=`echo $1|cut -c 1-6`
		month=`expr $month + $2`
		if [ $month -eq 0 ]; then
				month=12
				year=`expr $year - 1`
				Ymonth=${year}${month}
		fi
		if [ $month -lt 0 ]; then
				month=`expr 12 + $month`
				year=`expr $year - 1`
				Ymonth=${year}${month}
				if [ $month -le 0 ]; then
						month=`expr 12 + $month`
						year=`expr $year - 1`
						Ymonth=${year}${month}
				fi
		fi
		monthlen=`expr length $month`
		if [ $monthlen -lt 2 ]; then
				month=0${month}
		fi
		Ymonth=${year}${month}
		echo ${Ymonth}
}
getStdate(){
		year=`echo $1 |cut -c 1-4`
		month=`echo $1 |cut -c 5-6`
		day=`echo $1 |cut -c 7-9`
		echo $year-$month-$day
}

sendSms(){
		warningMsg=$1
		db2 connect to ynbiyj user biyjinst using bass1gen_db 2>&1 >/dev/null
		db2 -tx "insert into bass15.etl_sms(phone,content,status,level,starttime) 
		         select phone,'${warningMsg}','0','9',to_char(current_timestamp,'YYYY-MM-DD HH24:MI:SS')
		         from bass15.BASS1_DEVELOP_USER_SMS where level in ('10') 
		"
		db2 terminate 2>&1 >/dev/null
}

sendmms(){
	  title=$1
		warningMsg=$2
		db2 "connect to WEBDB user bisuite using 'ailk!123'" 2>&1 >/dev/null
		db2 -tx "
		         insert into localapp.loop_mms_send_yn (phone_number, mms_title, create_date, status, level, mms_content, mms_content_a)
             select 
                     phone
                      ,'$title'
                      ,to_char(current_timestamp,'YYYY-MM-DD HH24:MI:SS')
                      ,'0'
                      ,'9'
                      ,'A'
                     ,'${warningMsg}'
             from BASS15.BASS1_DEVELOP_USER_SMS where level in ('10') 
     "
		db2 terminate 2>&1 >/dev/null
}
trimSP(){
		echo $1 |sed "s/^[ \t]*//g" | sed "s/[ \t]*$//g"
}
write_trace(){
    msg=$1
    echo "[`date +%Y-%m-%d` `date +%H:%M:%S`]:  $msg " >> ${tracelogdir}/monitor_ftp_schedule.`date +%Y%m%d`.log
    echo $msg
}
day_monitor(){
		year=`date +%Y`
		month=`date +%m`
		day=`date +%d`
		hour=`date +%H`
		curymd=`date +%Y%m%d`
		db2 connect to YJSSDB user SS using ailk1234 2>&1 >/dev/null
		sql1="
		     select e.SRC||';'||g.title||';'||NEXTTIME||';'||value(b.maxdate,'')||';'||c.TYPE||';'||TIMEINTERVAL||';'||trim(DATAINTERVAL)
         from schedule.FLOWITEM a
         inner join (select flowid,NEXTTIME,TYPE,TIMEINTERVAL,DATAINTERVAL from schedule.FLOWTIMER where substr(NEXTTIME,1,10)>='`date +%F`' ) c on a.flowid=c.flowid
         inner join (select COMMANDID from SCHEDULE.COMMANDPARAM where TITLE='-d' and SRC='ftp') d on a.COMMANDID=d.COMMANDID
         inner join (select COMMANDID,SRC from SCHEDULE.COMMANDPARAM where TITLE='-s' ) e on d.COMMANDID=e.COMMANDID
         inner join schedule.flow g on a.flowid=g.flowid
         left join (select FLOWID,max(DATATIME) maxdate from SCHEDULE.RUNTIMEFLOWLOG  where STATUS in (3,13) group by FLOWID) b on a.flowid=b.flowid
    "
		write_trace "start run sql:$sql1"
		db2 -tx $sql1 > ${tracelogdir}/result_list.txt
    db2 terminate
    d=1
    m=1
    ld=1
    set -A day
    set -A month
    set -A eom
    day[0]=""
    month[0]=""
    eom[0]=""
    daynum=0
    monthnum=0
    eomnum=0
    cat ${tracelogdir}/result_list.txt | while read line
		do
				ftpid=$(echo $line|awk -F ";" '{print $1}')
				flowtitle=$(echo $line|awk -F ";"  '{print $2}')
				NEXTTIME=$(echo $line|awk -F ";"  '{print $3}')
				maxtime=$(echo $line|awk -F ";"  '{print $4}')
				type=$(echo $line|awk -F ";"  '{print $5}')
				timeinterval=$(echo $line|awk -F ";"  '{print $6}')
				datainterval=$(echo $line|awk -F ";" '{print $7}')

				if [[ $type -eq 1 ]]; then
						nextymd=`expr substr "$NEXTTIME" 1 4``expr substr "$NEXTTIME" 6 2``expr substr "$NEXTTIME" 9 2`
						maxymd=`expr substr "$maxtime" 1 4``expr substr "$maxtime" 6 2``expr substr "$maxtime" 9 2`
						lastymd=`getDay $curymd -1`
						if [[ $nextymd -gt $curymd ]]; then
								shouldupdateymd=`getDay $curymd -$datainterval`
						else
						    shouldupdateymd=`getDay $lastymd -$datainterval`
						fi
						if [[ $shouldupdateymd -gt $maxymd ]]; then
						    if [ z$maxtime == "z" ]; then
						    		write_trace "日FTP-【$flowtitle】才推送至【$maxymd】,实际应推送至【$shouldupdateymd】,请尽快核实!!!!"
										sendSms "截止`date +%T`:日FTP-新推外部数据【$flowtitle】还未更新过,正常应推送至$shouldupdatetime,请尽快核实!!!!"
								else
								    write_trace "日FTP-【$flowtitle】才推送至【$maxymd】,实际应推送至【$shouldupdateymd】,请尽快核实!!!!"
										sendSms "截止`date +%T`:日FTP-【$flowtitle】才推送至$maxymd,实际应推送至$shouldupdateymd,请尽快核实!!!!"
						    fi
						    day[$d]="[$d]. 日FTP-【$flowtitle】推送至$maxymd,实际应推送至$shouldupdateymd "
						    d=`expr $d + 1`

						else
						    write_trace "日FTP-【$flowtitle】已更新至最新日期【${shouldupdateymd}】,正常"
						fi

						daynum=`expr $daynum + 1`
						continue
				elif [[ $type -eq 2 ]]; then
						nextymd=`expr substr "$NEXTTIME" 1 4``expr substr "$NEXTTIME" 6 2``expr substr "$NEXTTIME" 9 2`
						maxym=`expr substr "$maxtime" 1 4``expr substr "$maxtime" 6 2`
						curym=`date +%Y%m`
						lastym=`getMonth $curym -1`
						if [[ $nextymd -gt $curymd ]]; then
								shouldupdateym=`getMonth $curym -$datainterval`
								if [[ `expr substr "$nextymd" 1 6` == `expr substr "$curymd" 1 6` ]]; then
										shouldupdateym=`getMonth $lastym -$datainterval`
								fi
						else
						    shouldupdateym=`getMonth $lastym -$datainterval`
						fi

						write_trace "【$flowtitle】 current update time is $maxym <should update time:$shouldupdateym>"
						if [[ $shouldupdateym -gt $maxym ]]; then
								if [ z$maxtime == "z" ]; then
										write_trace "月FTP-新推外部数据【$flowtitle】还未更新过,正常应推送至【$shouldupdateym】,请尽快核实!!!!"
										sendSms "截止`date +%T`:月FTP-新推外部数据【$flowtitle】还未更新过,正常应推送至$shouldupdateym,请尽快核实!!!!"
								else
								    write_trace "月FTP-【$flowtitle】才推送至【$maxym】,实际应推送至【$shouldupdateym】,请尽快核实!!!!"
										sendSms "截止`date +%T`:月FTP-【$flowtitle】才推送至$maxym,实际应推送至$shouldupdateym,请尽快核实!!!!"
								fi
								month[$m]="[$m]. 月FTP-【$flowtitle】推送至$maxym,实际应推送至$shouldupdateym "
						    m=`expr $m + 1`
						else
						    write_trace "月FTP-【$flowtitle】已更新至最新日期【$shouldupdateym】,正常"
						fi
						monthnum=`expr $monthnum + 1`
						continue
				elif [[ $type -eq 9 ]]; then
						nextymd=`expr substr "$NEXTTIME" 1 4``expr substr "$NEXTTIME" 6 2``expr substr "$NEXTTIME" 9 2`
						maxym=`expr substr "$maxtime" 1 4``expr substr "$maxtime" 6 2`
						curym=`date +%Y%m`
						lastym=`getMonth $curym -1`
						shouldupdateym=`getMonth $lastym -$datainterval`
        		if [[ $timeinterval -eq 1 ]]; then
								if [[ $shouldupdateym -gt $maxym ]]; then
										write_trace "【$flowtitle】-仅月末推送一次,才推送至【$maxym】,实际应推送至【$shouldupdateym】,请尽快核实!!!!"
										sendSms "截止`date +%T`:月末FTP-【$flowtitle】,才推送至$maxym,实际应推送至$shouldupdateym,请尽快核实!!!!"
										eom[$ld]="[$ld]. 月末FTP-【$flowtitle】推送至$maxym,实际应推送至$shouldupdateym "
						        ld=`expr $ld + 1`
								else
										write_trace "【$flowtitle】已更新至最新日期【$shouldupdateym】,正常"
								fi
					  elif [[ $timeinterval -eq 3 ]]; then
					      nextym=`expr substr "$NEXTTIME" 1 4``expr substr "$NEXTTIME" 6 2`
					  		shouldupdateym=`getMonth $nextym -3`
					  		shouldupdateym=`getMonth $shouldupdateym -$datainterval`
					  		if [[ $shouldupdateym -gt $maxym ]]; then
					  				write_trace "【$flowtitle】-仅月末推送一次,才推送至【$maxym】,实际应推送至【$shouldupdateym】,请尽快核实!!!!"
										sendSms "截止`date +%T`:季度末FTP-【$flowtitle】,才推送至$maxym,实际应推送至$shouldupdateym,请尽快核实!!!!"
										eom[$ld]="[$ld]. 月末FTP-【$flowtitle】推送至$maxym,实际应推送至$shouldupdateym "
						        ld=`expr $ld + 1`
								else
										write_trace "【$flowtitle】已更新至最新日期【$shouldupdateym】,正常"
					  		fi
        		fi
        		eomnum=`expr $eomnum + 1`
        		continue
				fi
		done
		####汇总对外ftp整体推送情况
    checkpoint="09 10 11 15 16 17 18 20 21 22"
    echo $checkpoint|grep $hour
    if [[ $? -eq 0 ]]; then
				abnormalday=${day[@]}
				abnormalmonth=${month[@]}
				abnormaleom=${eom[@]}
				echo "尚未推送完的日数据如下：${abnormalday}"
				echo "尚未推送完的月数据如下：${abnormalmonth}"
				echo "尚未推送完的月末数据如下：${abnormaleom}"
				totalnum=`expr $daynum + $monthnum + $eomnum`
				abnormaldayftpnum=`expr $d - 1`
				normaldayftpnum=`expr $daynum - $abnormaldayftpnum`
				abnormalmonthftpnum=`expr $m - 1`
				normalmonthftpnum=`expr $monthnum - $abnormalmonthftpnum`
				abnormaleomftpnum=`expr $ld - 1`
				normaleomftpnum=`expr $eomnum - $abnormaleomftpnum`
				####日ftp整体情况通报
				if [[ $abnormaldayftpnum -gt 0 ]]; then
				    write_trace "当前共需对外推送$daynum个日数据,已成功推完$normaldayftpnum个,还剩$abnormaldayftpnum个未推送：${abnormalday}"
						sendmms "截止`date +%F\ %T`:大数据平台对外推送日周期数据情况" "当前共需对外推送$daynum个日数据,已成功推完$normaldayftpnum个,还剩$abnormaldayftpnum个未推送：${abnormalday};以上,维护人员正保障中,请各位知晓!!!!"
				else
						grep day${curym}ok ${tracelogdir}/finish
						if [[ $? -eq 0 ]]; then
								write_trace "${curymd}日数据,之前即已短信通知推送完毕,不再短信提醒！"
						else
								write_trace "当前共需对外推送$daynum个日数据,已成功推完$normaldayftpnum个,当天日数据已推送完毕,请知晓！"
								sendmms "截止`date +%F\ %T`:大数据平台对外推送日周期数据情况" "当前共需对外推送$daynum个日数据,已成功推完$normaldayftpnum个,当天日数据已推送完毕,请知晓！"
								echo "day${curymd}ok" >> ${tracelogdir}/finish
						fi
				fi
				####月ftp整体情况通报
				if [[ $abnormalmonthftpnum -gt 0 ]]; then
						write_trace "当前共需对外推送$monthnum个月数据,已成功推完$normalmonthftpnum个,还剩$abnormalmonthftpnum个未推送：${abnormalmonth}"
						sendmms "截止`date +%F\ %T`:大数据平台对外推送月周期数据情况" "当前共需对外推送$monthnum个月数据,已成功推完$normalmonthftpnum个,还剩$abnormalmonthftpnum个未推送：${abnormalmonth};以上,维护人员正保障中,请各位知晓!!!!"
				else
						grep month${curym}ok ${tracelogdir}/finish
						if [[ $? -eq 0 ]]; then
								write_trace "${curym}月数据,之前即已短信通知推送完毕,不再短信提醒！"
						else
								write_trace "当前共需对外推送$monthnum个月数据,已成功推完$normalmonthftpnum,当月月数据已推送完毕,请知晓！"
								sendmms "截止`date +%F\ %T`:大数据平台对外推送月周期数据情况" "当前共需对外推送$monthnum个月数据,已成功推完$normalmonthftpnum,当月月数据已推送完毕,请知晓！"
								echo "month${curym}ok" >> ${tracelogdir}/finish
						fi
				fi
				####月末ftp整体情况通报
				if [[ $abnormaleomftpnum -gt 0 ]]; then
						write_trace "当前共需对外推送$eomnum个月数据,已成功推完$normaleomftpnum个,还剩$abnormaleomftpnum个未推送：${abnormaleom}"
						sendmms "截止`date +%F\ %T`:大数据平台对外推送数据情况-仅月末一次" "当前共需对外推送$eomnum个月数据,已成功推完$normaleomftpnum个,还剩$abnormaleomftpnum个未推送：${abnormaleom};以上,维护人员正保障中,请各位知晓!!!!"
				else
						grep eom${curym}ok ${tracelogdir}/finish
						if [[ $? -eq 0 ]]; then
								write_trace "${curym}月末一次数据,之前即已短信通知推送完毕,不再短信提醒！"
						else 
								write_trace "当前共需对外推送$eomnum个月末数据,已成功推完$normaleomftpnum个,当天日数据已推送完毕,请知晓！"
								sendmms "截止`date +%F\ %T`:大数据平台对外推送数据情况-仅月末一次" "当前共需对外推送$eomnum个月末数据,已成功推完$normaleomftpnum,当天日数据已推送完毕,请知晓！"
								echo "eom${curym}ok" >> ${tracelogdir}/finish
						fi
				fi
		else
				write_trace "当前时间点: $hour 不在检查点($checkpoint)内，故不作汇总检查"
		fi
		rm -f ${tracelogdir}/result_list.txt
}

while [ 1 -eq 1 ]
do
		write_trace "开始本次推送外部数据质量监控"
		if [[ ! -f ${tracelogdir}/finish ]]; then
				touch ${tracelogdir}/finish
		fi
		day_monitor
		write_trace "结束本次循环,3小时后进入下轮监控"
		sleep 10800
done