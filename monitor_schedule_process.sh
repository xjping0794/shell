#/usr/bin/ksh
#writor/Create date:xiaojp/20140530
#===================================================================================================
# ����������20140531 ���ÿ����Ҫ���������ʱ�ԣ������Ÿ澯
#���������� 20140610 ����ǰ��̨�������ڸ���ͬ���˲�澯
#          20140611 �������±���ʱ�Եļ��
#          20140701 �������³����˵��ӿ�PN5050�޷��������±�����ʱ���жϣ���PN5050�ӿ�Ӱ��ı�����2��18��ǰ�����ж�
#  13759559546 15096614337
#===================================================================================================

msisdn="15198907708 15911649424"
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
 getMonth()
  {
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
executesqlstmt()
{
	stmtsql=$1
	db2 connect to YJSSDB user SS using ailk1234 |tee -a monitor_schedule_process.sql
	if [ $? -ne 0 ]; then
	printf "database connection error..........."
	sendSms "10.173.252.213����ʧ�ܣ���˲�"
	exit -1
	fi
	i=0
	db2 -tx $stmtsql |while read line
	do
	arr[$i]=$line
	i=`expr $i + 1`
	done
	db2 terminate |tee -a monitor_schedule_process.sql
}
getStdate()
{
	year=`echo $1 |cut -c 1-4`
	month=`echo $1 |cut -c 5-6`
	day=`echo $1 |cut -c 7-9`
	echo $year-$month-$day
}

sendSms()
{
	warningMsg=$1
	echo $warningMsg
	curdate=`date +%Y%m%d%H%m%d`
	db2 connect to ynbiyj user biyjinst using bass1gen_db
	if [ $? -ne 0 ]; then
	printf "database connection error..........."
	sendSms "10.173.252.139 ����ʧ�ܣ���˲�"
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
trimSP()
{
	echo $1 |sed "s/^[ \t]*//g" | sed "s/[ \t]*$//g"
}
abs()
{
	echo $1 | sed 's/^-//'
}
get_report_date()
{
	rpt_id=$1
	db2 connect to WEBDB209 user biqt using biqt123
	if [ $? -ne 0 ]; then
  printf "YJSSDB connect fail....."
  sendSms "10.173.252.209����ʧ�ܣ���˲�"
  exit -1
  fi
	echo "select MAX(ts_fmt(RPT_MAXDATE,'yyyymmdd')) from dwpub.td_na_rpt_maxdate where RPT_NAME='${rpt_id}'" >> monitor_schedule_process.sql
	db2 -tx "select MAX(ts_fmt(RPT_MAXDATE,'yyyymmdd')) RPT_MAXDATE from dwpub.td_na_rpt_maxdate where RPT_NAME='${rpt_id}'"| read reportdate
	db2 terminate
}
deal_day()
{
  curday=`date +%Y%m%d`
  proc_date=`getDay $curday -1`
  getcurdaytime=`date +%d%H%M`
  db2 connect to YJSSDB user SS using ailk1234
  if [ $? -ne 0 ]; then
  printf "YJSSDB connect fail....."
  sendSms "10.173.252.213����ʧ�ܣ���˲�"
  exit -1
  fi
  sql1="select CODE||';'||codename||';'||kpi_sql||';'||START_TIME||';'||NORMAL_TIME||';'||complete_date||';'||RPTID from dwctr.monitor_schedule_process
  where complete_date<'${proc_date}' and VALID_FLAG='Y' and CYCLE_TYPE='D' and DEPEND_PN5050_TIME_FLAG<='${getcurdaytime}'  order by code;"
  echo $sql1 | tee -a monitor_schedule_process.sql
  db2 -tx $sql1| while read line
  do
  code1=$(echo $line|awk -F ";" '{print $1}')
  codename1=$(echo $line|awk -F ";"  '{print $2}')
  kpi_sql=$(echo $line|awk -F ";"  '{print $3}')
  START_TIME=$(echo $line|awk -F ";"  '{print $4}')
  complete_date=$(echo $line|awk -F ";"  '{print $6}')
  NORMAL_TIME=$(echo $line|awk -F ";" '{print $5}')
  rptid=$(echo $line|awk -F ";" '{print $7}')
  code=`trimSP $code1`
  codename=`trimSP $codename1`
  NORMAL_TIME=`trimSP $NORMAL_TIME`
  rptid=`trimSP $rptid`
  if [ z$complete_date == "z" ]; then
     complete_date=$proc_date
  elif [ $complete_date -lt $proc_date ]; then
     complete_date=`getDay $complete_date 1`
  elif [ $complete_date -ge $proc_date ]; then
     echo "������ɽ�����һ��У��"
     continue
  fi
  do_date=$complete_date
	pro_date=`getStdate $do_date`
	echo "--��ʼ${do_date}�յ�������${code}-${codename} ��У��\n" |tee -a monitor_schedule_process.sql
	c_t_sql=$(echo $kpi_sql |sed "s/curday/${do_date}/g")
	c_t_sql=$(echo $c_t_sql |sed "s/cur_date/${pro_date}/g")
	curtime=`date +%H%M`
	curtime1=`date +%H:%M`
	echo "--$do_date ����ʱ���жϣ�\n $c_dp_sql  \n $l_dp_sql "  >> monitor_schedule_process.sql
  if [ $curtime -lt $START_TIME ]; then
  echo "------����$do_date:�������� ${code}-${codename}-����ʱ��δ��-��ȴ�\n" |tee -a monitor_schedule_process.sql
	continue
  fi
  executesqlstmt "$c_t_sql"
  result1=${arr[*]}
  echo "----result1 is $result1----" >> monitor_schedule_process.sql
  if [ $result1 -le 0 ]; then
  echo "------����$do_date:�������� ${code}-${codename}: ��ֹʱ��${curtime1}���������${NORMAL_TIME}��ɳ�������δ����OK,�뾡��˲�" |tee -a monitor_schedule_process.sql
  warning="����${do_date}:${codename}��ǰʱ��${curtime1}����������ʱ��${NORMAL_TIME}����δ����,�뾡��˲飡"
  sendSms "$warning"��
  echo "----��������$do_date:�������� ${code}-${codename}: �������" |tee -a monitor_schedule_process.sql
  continue
  else
    if [ x$rptid != "x" ]; then
    echo "-----${codename}: ǰ��̨����һ����У��" |tee -a monitor_schedule_process.sql
    get_report_date "$rptid"
    report_date=`trimSP $reportdate`
    report_date=$(echo $report_date|tr -d " -./")
    echo "����${codename}ǰ̨չ��������$report_date" >> monitor_schedule_process.sql
    echo "����${codename}�ֿ����������$do_date" >> monitor_schedule_process.sql
       if [ $report_date == $do_date ]; then
       warning="����${do_date}:${codename}������ϣ���֪����"
      # sendSms "$warning"
       echo $warning >>monitor_schedule_process.sql
       else
       warning="����${do_date}:${codename}ǰ��̨���ڲ�һ�£��뾡��˲飡"
       sendSms "$warning"
       fi
    else
    warning="����${do_date}:${codename}������ϣ���֪����"
    # sendSms "$warning"
    echo $warning >>monitor_schedule_process.sql
    fi
  updatesql="update dwctr.monitor_schedule_process set complete_date='$do_date' where code='$code' and codename='$codename'"
  echo "����SQL���ڣ�\n $updatesql" |tee -a monitor_schedule_process.sql
  executesqlstmt "$updatesql"
  echo "----��������$do_date:�������� ${code}-${codename}: �������" |tee -a monitor_schedule_process.sql
  fi
  done
  db2 terminate
  cur_end_t=`date +%Y%m%d%H%M`
  echo "------------ʱ��$cur_end_t------�յ��ȼ�ؽ���----------------------------\n"
  return
}
deal_month()
{
	curMonth=`date +%Y%m`
	proc_month=`getMonth $curMonth -1`
	proc_mfirstday=${proc_month}01
	db2 connect to YJSSDB user SS using ailk1234
	if [ $? -ne 0 ]; then
  printf "YJSSDB connect fail....."
  sendSms "10.173.252.213����ʧ�ܣ���˲�"
  exit -1
  fi
  sql1="select CODE||';'||codename||';'||kpi_sql||';'||START_TIME||';'||NORMAL_TIME||';'||complete_date||';'||RPTID from dwctr.monitor_schedule_process
  where complete_date<'${proc_month}' and VALID_FLAG='Y' and CYCLE_TYPE='M' order by code;"
  echo $sql1 | tee -a monitor_schedule_process.sql
  db2 -tx $sql1| while read line
  do
  code1=$(echo $line|awk -F ";" '{print $1}')
  codename1=$(echo $line|awk -F ";"  '{print $2}')
  kpi_sql=$(echo $line|awk -F ";"  '{print $3}')
  START_TIME=$(echo $line|awk -F ";"  '{print $4}')
  complete_date=$(echo $line|awk -F ";"  '{print $6}')
  NORMAL_TIME=$(echo $line|awk -F ";" '{print $5}')
  rptid=$(echo $line|awk -F ";" '{print $7}')
  code=`trimSP $code1`
  codename=`trimSP $codename1`
  NORMAL_TIME=`trimSP $NORMAL_TIME`
  rptid=`trimSP $rptid`
  if [ z$complete_date == "z" ]; then
     complete_date=$proc_month
  elif [ $complete_date -lt $proc_month ]; then
     complete_date=`getMonth $complete_date 1`
  elif [ $complete_date -ge $proc_month ]; then
     echo "������ɽ�������У��"
     continue
  fi
  echo $complete_date
  do_date=${complete_date}01
	pro_date=`getStdate $do_date`
  echo "--��ʼ${complete_date}�µ�������${code}-${codename} ��У��\n" |tee -a monitor_schedule_process.sql
	c_t_sql=$(echo $kpi_sql |sed "s/curday/${do_date}/g")
	c_t_sql=$(echo $c_t_sql |sed "s/cur_date/${pro_date}/g")
  getcurday=`date +%d%H%M`
  curtime1=`date +%H:%M`
  echo $getcurday
	echo "--$complete_date ����ʱ���жϣ�\n $c_dp_sql  \n $l_dp_sql "  >> monitor_schedule_process.sql
  if [ $getcurday -lt $START_TIME ]; then
  echo "------����$complete_date:�������� ${code}-${codename}-����ʱ��δ��-��ȴ�\n" |tee -a monitor_schedule_process.sql
	continue
  fi
  executesqlstmt "$c_t_sql"
  result1=${arr[*]}
   echo "----result1 is $result1----" >> monitor_schedule_process.sql
  if [ $result1 -le 0 ]; then
  echo "------����$complete_date:��������${codename}: ��ֹʱ��${curtime1}���������${NORMAL_TIME}��ɳ�������δ����OK,�뾡��˲�" |tee -a monitor_schedule_process.sql
  warning="����${complete_date}:${codename}��ǰʱ��${curtime1}����������ʱ��${NORMAL_TIME}����δ����,�뾡��˲飡"
  sendSms "$warning"��
  echo "----��������$complete_date:��������${codename}: �������" |tee -a monitor_schedule_process.sql
  continue
  else
    if [ x$rptid != "x" ]; then
    echo "-----${codename}: ǰ��̨����һ����У��" |tee -a monitor_schedule_process.sql
    get_report_date "$rptid"
    report_date=`trimSP $reportdate`
    report_date=$(echo $report_date|tr -d " -./")
    echo "����${codename}ǰ̨չ��������$report_date" >> monitor_schedule_process.sql
    echo "����${codename}�ֿ����������$do_date" >> monitor_schedule_process.sql
       if [ $report_date == $do_date ]; then
       warning="����${complete_date}:${codename}������ϣ���֪����"
       sendSms "$warning"
       else
       warning="����${complete_date}:${codename}ǰ��̨���ڲ�һ�£��뾡��˲飡"
       sendSms "$warning"
       fi
    else
    warning="����${complete_date}:${codename}������ϣ���֪����"
    sendSms "$warning"
    fi
  updatesql="update dwctr.monitor_schedule_process set complete_date='$complete_date' where code='$code' and codename='$codename'"
  echo "����SQL���ڣ�\n $updatesql" |tee -a monitor_schedule_process.sql
  executesqlstmt "$updatesql"
  echo "----��������$do_date:�������� ${code}-${codename}: �������" |tee -a monitor_schedule_process.sql
  fi
  done
  db2 terminate
  cur_end_t=`date +%Y%m%d%H%M`
  echo "------------ʱ��$cur_end_t------�µ��ȼ�ؽ���----------------------------\n"
}
while [ 1 -eq 1 ]
do
    cur_t1=`date +%H`
    if [ $cur_t1 -ge "00" -a $cur_t1 -le "07" ]; then
       t_sleep=60
    elif [ $cur_t1 -ge "07" -a $cur_t1 -le "19" ]; then
       t_sleep=20
    else
       t_sleep=60
    fi
    cur_start_t=`date +%Y%m%d%H%M`
    echo "------------ʱ��$cur_start_t------�յ��ȼ�ؿ�ʼ----------------------------\n"
		deal_day
		cur_start_t=`date +%Y%m%d%H%M`
    echo "------------ʱ��$cur_start_t------�µ��ȼ�ؿ�ʼ----------------------------\n"
    deal_month
    current_date=`date +%Y-%m-%d`
    current_time=`date +%T`
    echo "��ǰʱ�䣺$current_date $current_time----$t_sleep���Ӻ������һ��ѭ��" |tee -a monitor_schedule_process.sql
		sleep `expr 60 \* $t_sleep`
done
