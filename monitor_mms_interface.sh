#/usr/bin/ksh
#writor/Create date:xiaojp/20140524
#===================================================================================================
# 功能描述：20140524 监控每日重要接口数据内容，并短信告警
#　　　　　 20140805 增加全量接口监控，当天量>前天量，违反规则，将表rename成带bak的表，使调度挂起 
#           20140826 增加表是否存在的校验
#  13759559546 15096614337
#===================================================================================================
msisdn="15198907708 15911649424 13668728913"
getDay()
  {
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
               fi ;;
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
executesqlschedulestmt()
{ 
	stmtsql=$1 
	db2 connect to yjssdb user ss using ailk1234 |tee -a monitor_mms_interface.sql
	if [ $? -ne 0 ]; then
	printf "database connection error..........."
	exit -1
	fi
	i=0
	db2 -tx $stmtsql |while read line
	do
	arr[$i]=$line
	i=`expr $i + 1`
	done
	db2 terminate |tee -a monitor_mms_interface.sql 
}  
executesqlstmt()
{ 
	stmtsql=$1 
	db2 connect to ynbidb user aibass using newbi1001 |tee -a monitor_mms_interface.sql
	if [ $? -ne 0 ]; then
	printf "database connection error..........."
	exit -1
	fi
	i=0
	db2 -tx $stmtsql |while read line
	do 
	arr[$i]=$line
	i=`expr $i + 1`
	done
	db2 terminate |tee -a monitor_mms_interface.sql 
}
sendSms()
{
	warningMsg=$1
	echo $warningMsg
	curdate=`date +%Y%m%d%H%m%d`
	db2 connect to ynbiyj user biyjinst using bass1gen_db
	if [ $? -ne 0 ]; then
	printf "database connection error..........."
	sendSms "10.173.252.139 连接失败，请核查"  
	exit -1
	fi
	for phone in ${msisdn[*]}
	do
	sql1="insert into bass15.etl_sms(phone,content,status,level,starttime) values('$phone','${warningMsg}','0','9','${curdate}')"
	db2 -tv $sql1  
	 if [ $? -ne 0 ]; then                                     
   printf "ynbiyj connect fail....."                     
   exit 1                                                
   else                                                  
   printf "ynbiyj connect success....."                  
   fi  
  done
	db2 terminate
}
check_table_exist()
{ 
	stmtsql=$1 
	deal_date=$2
	db2 connect to ynbidb user aibass using newbi1001 |tee -a monitor_mms_interface.sql
	if [ $? -ne 0 ]; then
	printf "database connection error..........."
	exit -1
	fi
	sqlck="select count(1) from syscat.tables where tabname like '%${stmtsql}%${deal_date}'"
	echo $sqlck |tee -a monitor_mms_interface.sql 
	db2 -tx $sqlck |read line 
	db2 terminate |tee -a monitor_mms_interface.sql 
}
getStdate()
{
	year=`echo $1 |cut -c 1-4`
	month=`echo $1 |cut -c 5-6`
	day=`echo $1 |cut -c 7-9`
	echo $year-$month-$day
}	


trimSP()
{ 
	echo $1 |sed "s/^[ \t]*//g" | sed "s/[ \t]*$//g"
}
abs () 
{ 
	echo $1 | sed 's/^-//'
}
deal()
{
curday=`date +%Y%m%d`
proc_date=`getDay $curday -1`
echo ${proc_date} >>11.sql
db2 connect to ynbidb user aibass using newbi1001
sql1="select CODE||';'||codename||';'||kpi_sql||';'||depend_sql||';'||chk_rate||';'||complete_date||';'||IF_FULLMARK||';'||TABNAME 
from bass15.MMS_INTERFACE_MONITOR  where complete_date<'${proc_date}'  and del_flag='0' order by code;" 
echo ${sql1} >>11.sql
db2 -tx $sql1| while read line
do 
code1=$(echo $line|awk -F ";" '{print $1}')
codename1=$(echo $line|awk -F ";"  '{print $2}')
kpi_sql=$(echo $line|awk -F ";"  '{print $3}')
depend_sql=$(echo $line|awk -F ";"  '{print $4}')
complete_date=$(echo $line|awk -F ";"  '{print $6}') 
check_rate=$(echo $line|awk -F ";" '{print $5}')
IF_FULLMARK=$(echo $line|awk -F ";" '{print $7}')
TABNAME1=$(echo $line|awk -F ";" '{print $8}')
code=`trimSP $code1`
codename=`trimSP $codename1`
tabname=`trimSP $TABNAME1` 
if [ -z "$complete_date" ]; then 
   complete_date=$proc_date
elif [ $complete_date -lt $proc_date ]; then
   complete_date=`getDay $complete_date 1`
elif [ $complete_date -ge $proc_date ]; then
   echo "当天完成进行下一天校验"
   continue
fi 
  do_date=$complete_date
	pro_date=`getStdate $do_date`
	predate=`getDay $do_date -1`
	pre_date=`getStdate $predate`
	echo "--开始${do_date}日接口${code}-${codename} 的校验\n" |tee -a monitor_mms_interface.sql
	c_t_sql=$(echo $kpi_sql |sed "s/curday/${do_date}/g")
	c_t_sql=$(echo $c_t_sql |sed "s/cur_date/${pro_date}/g")
	l_t_sql=$(echo $kpi_sql |sed "s/curday/${predate}/g")
	l_t_sql=$(echo $l_t_sql |sed "s/cur_date/${pre_date}/g")
  echo "--$do_date KPI指标核查： \n $c_t_sql  \n $l_t_sql "   >> monitor_mms_interface.sql
  c_dp_sql=$(echo $depend_sql |sed "s/curday/${do_date}/g")
	c_dp_sql=$(echo $c_dp_sql |sed "s/cur_date/${pro_date}/g")
	l_dp_sql=$(echo $depend_sql |sed "s/curday/${predate}/g")
	l_dp_sql=$(echo $l_dp_sql |sed "s/cur_date/${pre_date}/g")
	echo "--$do_date 依赖条件判断：\n $c_dp_sql  \n $l_dp_sql "  >> monitor_mms_interface.sql
  executesqlschedulestmt "$c_dp_sql"
  depend=${arr[*]} 
  if [ $depend -eq 0 ]; then
  echo "------日期$do_date:接口 ${code}-${codename}-波动校验-条件未满足\n" |tee -a monitor_mms_interface.sql
	continue
  fi
  echo "开始校验${code1}对应${do_date}号和${predate}的表是否存在"|tee -a monitor_mms_interface.sql 
  check_table_exist $code1 $do_date
  if [ $line -eq 0 ]; then 
	warning="${stmtsql}对应${deal_date}号表不存在，不作波动性检查" 
	echo "${stmtsql}对应${deal_date}号表不存在，不作波动性检查"|tee -a monitor_mms_interface.sql 
  sendSms "$warning"
	continue  
	fi
  check_table_exist $code1 $predate
  if [ $line -eq 0 ]; then 
	warning="${stmtsql}对应${deal_date}号表不存在，不作波动性检查" 
	echo "${stmtsql}对应${deal_date}号表不存在，不作波动性检查"|tee -a monitor_mms_interface.sql 
  sendSms "$warning"
	continue  
	fi
  executesqlstmt "$c_t_sql"
  value=${arr[*]} 
  val1=`trimSP $value`
  executesqlstmt "$l_t_sql"
  value=${arr[*]}
  val2=`trimSP $value` 
  echo "----val1 is $val1----\n---val2 is $val2---" >> monitor_mms_interface.sql
  if [ $IF_FULLMARK -eq 0 ]; then 
       if [ $val1 -le 0 ]; then
       echo "------波动率校验数据为0\n" |tee -a monitor_mms_interface.sql
       warning="接口$code：$codename 为0,请核查" 
       sendSms "$warning"
       updatesql="update bass15.MMS_INTERFACE_MONITOR set complete_date='$do_date' where code='$code' and codename='$codename'"
       executesqlstmt "$updatesql"
       echo "----结束日期$do_date:接口 ${code}-${codename}: 波动率校验" |tee -a monitor_mms_interface.sql
       continue
       elif [ $val1 -ne 0 -a $val2 -ne 0 ]; then  
        rate1=$(echo "scale=2; ($val1/$val2-1)*100" | bc) 
        rate2=$(echo "scale=0; $rate1/1" | bc)  
        rate=`abs $rate2`
        echo "\n rate1 is $rate1 \n rate2 is $rate2 \n rate is $rate " >> monitor_mms_interface.sql
        updatesql="update bass15.MMS_INTERFACE_MONITOR set complete_date='$do_date' where code='$code' and codename='$codename'"
        echo "更新SQL日期：\n $updatesql" |tee -a monitor_mms_interface.sql |tee -a monitor_mms_interface.sql
        executesqlstmt "$updatesql"
        echo "----结束日期$do_date:接口 ${code}-${codename}: 波动率校验" |tee -a monitor_mms_interface.sql
       fi 
       if [ $rate -gt $check_rate ]; then
       echo "日期$do_date:接口 ${code}-${codename}: \n波动${rate}%(阀值${check_rate}%),本期:$val1 上期:$val2" |tee -a monitor_mms_interface.sql
       warning="日期$do_date:接口 ${code}-${codename}: 波动${rate}%(阀值${check_rate}%),本期:$val1 上期:$val2,请核查"
       sendSms "$warning"
       else
       echo "日期$do_date:接口 ${code}-${codename}: 波动正常" |tee -a monitor_mms_interface.sql
       fi
  elif [ $IF_FULLMARK -eq 1 ]; then        
       if [ $val1 -le $val2 ]; then 
       echo "------全量接口数据当天小于前天\n" |tee -a monitor_mms_interface.sql
       warning="接口$code：$codename 数据量<=前一天,异常请核查（已将接口rename成带_bak的表）" 
       echo "rename table dwifc.${tabname}_${do_date} to ${tabname}_${do_date}_bak" |tee -a monitor_mms_interface.sql
       renamesql="rename table dwifc.${tabname}_${do_date} to ${tabname}_${do_date}_bak"
       executesqlstmt "$renamesql"
       sendSms "$warning"
       updatesql="update bass15.MMS_INTERFACE_MONITOR set complete_date='$do_date' where code='$code' and codename='$codename'"
       executesqlstmt "$updatesql"
       echo "----结束日期$do_date:接口 ${code}-${codename}: 环比校验" |tee -a monitor_mms_interface.sql
       continue
       else
       updatesql="update bass15.MMS_INTERFACE_MONITOR set complete_date='$do_date' where code='$code' and codename='$codename'"
       executesqlstmt "$updatesql"
       echo "----结束日期$do_date:接口 ${code}-${codename}: 环比校验正常" |tee -a monitor_mms_interface.sql
       fi
  fi
done
db2 terminate
cur_end_t=`date +%Y%m%d%H%M`
echo "------------时间$cur_end_t------监控结束----------------------------\n" 
return
}

while [ 1 -eq 1 ]
do
    cur_t1=`date +%H`
    if [ $cur_t1 -ge "00" -a $cur_t1 -le "04" ]; then
       t_sleep=10
    elif [ $cur_t1 -ge "05" -a $cur_t1 -le "12" ]; then
       t_sleep=10
    elif [ $cur_t1 -ge "13" -a $cur_t1 -le "15" ]; then
       t_sleep=15
    else
       t_sleep=15
    fi 
    cur_start_t=`date +%Y%m%d%H%M`
    echo "------------时间$cur_start_t------监控开始----------------------------\n"
		deal
		sleep `expr 60 \* $t_sleep`
done
