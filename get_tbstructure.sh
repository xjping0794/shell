#!/usr/bin/ksh
#set -x   启动"-x"选项 要跟踪的程序段
##############################################################################################
# creator\time: xiaojp \201403  
# for example : get_tbstructure.sh -t1 dwctr.TC_CUST_GROUPMEB_SELF_SUM_DAY_20140228 -t2 dwmid.table2 -s tbs_dw -p 6
# get_tbstructure.sh -t1 dwmid.TM_USER_FARM_GROUP_NET_PAYBIL_DAY_YYYYMMDD -t2 dwmid.TM_USER_FARM_GROUP_NET_PAYBIL_DAY_YYYYMMDD -s tbs_dw -p 6
# get_tbstructure.sh -t1 dwmid.TM_USR_PRO_AVAILABLE_GPRS_DAY_YYYYMMDD -t2 dwmid.TM_USR_PRO_AVAILABLE_GPRS_DAY_YYYYMMDD -s tbs_dwd -p 6
#
# 说明：   export_load_2.sh -t1 export_tb1  -t2 load_tb1 -s tbs_space -p type
#                             -t1:需要导出的源表
#                             -t2:需要装入的目标表
#                             -s :目标表所处的空间
#                             -p ： 2为一经导二经 ,1为二经导一经,3为二经导数据集市
##############################################################################################


echo "get command variable......"
if [ $# -eq 8 ]; then
  while [ $# -ne 0 ] 
  do
        if [ x$1 == 'x-t1' ]; then 
        shift
        export_tb1=$1
        shift 
        elif [ x$1 == 'x-t2' ]; then
        shift
        load_tb1=$1
        shift 
        elif [ x$1 == 'x-s' ]; then
        shift
        tbs_space=$1
        shift
        elif [ x$1 == 'x-p' ]; then
        shift
        type=$1
        shift
        fi
  done
  printf "${export_tb1}---${load_tb1}---${tbs_space} \n"
else 
  printf "Usage:$0 -t1 export_tb1 -t2 load_tb1 -s tbs_space -p type \n"
  exit 1
fi  

echo "set database parameter.......\n"
if [ $type == '1' ]; then
    ExportDbName="YNBIDB"
    ExportDbUser="aibass"
    ExportDbPassword="newbi1001"
    LoadDbName="YNBIYJ"
    LoadDbUser="biyjinst" 
    LoadDbPassword="bass1gen_db"
    echo "---${export_tb1} start to export from bass2node to bass1node---and tablename of bass1node is ${load_tb1}------\n"
    elif [ $type == '2' ]; then
    ExportDbName="YNBIYJ"
    ExportDbUser="biyjinst"
    ExportDbPassword="bass1gen_db"
    LoadDbName="YNBIDB"
    LoadDbUser="aibass"
    LoadDbPassword="newbi1001"
    echo "---${export_tb1} start to export from bass1node to bass2node---and tablename of bass2node is ${load_tb1}------\n"
    elif [ $type == '3' ]; then
    ExportDbName="YNBIDB"
    ExportDbUser="aibass"
    ExportDbPassword="newbi1001"
    LoadDbName="YNBI203"
    LoadDbUser="bimart"
    LoadDbPassword="bimart123"
    echo "---${export_tb1} start to export from bass2node to datamart---and tablename of datamart is ${load_tb1}------\n"
    elif [ $type == '4' ]; then
    ExportDbName="YNBIDB"
    ExportDbUser="aibass"
    ExportDbPassword="newbi1001"
    LoadDbName="WEBDB209"
    LoadDbUser="biqt"
    LoadDbPassword="biqt123"
    echo "---${export_tb1} start to export from bass2node to report---and tablename of report is ${load_tb1}------\n"
    elif [ $type == '5' ]; then
    ExportDbName="YNBI203"
    ExportDbUser="bimart"
    ExportDbPassword="bimart123"
    LoadDbName="YNBIDB"
    LoadDbUser="aibass"
    LoadDbPassword="newbi1001"
    echo "---${export_tb1} start to export from datamart to bass2node---and tablename of datamart is ${load_tb1}------\n"
    elif [ $type == '6' ]; then
    ExportDbName="YNBIDB"
    ExportDbUser="aibass"
    ExportDbPassword="newbi1001"
    LoadDbName="ynbihis"
    LoadDbUser="aibass"
    LoadDbPassword="newbi1001"
    echo "---${export_tb1} start to export from bass2node to HISDB---and tablename of report is ${load_tb1}------\n"
fi
ExportDbName=$(echo $ExportDbName|tr 'a-z' 'A-Z')
ExportDbUser=$(echo $ExportDbUser|tr 'a-z' 'A-Z') 
LoadDbName=$(echo $LoadDbName|tr 'a-z' 'A-Z')
LoadDbUser=$(echo $LoadDbUser|tr 'a-z' 'A-Z') 
echo "ExportDbName is ${ExportDbName}"
echo "ExportDbUser is ${ExportDbUser}"
echo "ExportDbPassword is ${ExportDbPassword}"
echo "LoadDbName is ${LoadDbName}"
echo "LoadDbUser is ${LoadDbUser}"
echo "LoadDbPassword is ${LoadDbPassword} \n"

source_schema=$(echo $export_tb1|cut -d "." -f 1|tr 'a-z' 'A-Z')
source_table=$(echo $export_tb1|cut -d "." -f 2|tr 'a-z' 'A-Z')
target_schema=$(echo $load_tb1|cut -d "." -f 1|tr 'a-z' 'A-Z')
target_table=$(echo $load_tb1|cut -d "." -f 2|tr 'a-z' 'A-Z')
tbs_space=$(echo $tbs_space|tr 'a-z' 'A-Z')
trimspace()
{
        echo $1|sed "s/^[ ]*//g"|sed "s/[ ]*$//g"
}
file_deal()
{
	db2 "connect to ${ExportDbName} user ${ExportDbUser} using ${ExportDbPassword}"    
	sqlstmt="select tbsp_name from sysibmADM.TBSP_utilization group by tbsp_name order by tbsp_name desc" 
	db2 -tx $sqlstmt|while read line
	do
	soure_space=`trimspace $line`
	echo "------grep -i $soure_space ${source_table}.sql-----"
	flag=`grep -i -l $soure_space ${source_table}.sql` 
	if [ x$flag == "x" ]; then
	continue
	else
	echo "cat ${source_table}.sql|sed -e "s/\"$soure_space\"/\"$tbs_space\"/g" -e "s/$source_schema/$target_schema/g" "
	cat ${source_table}.sql|sed -e "s/\"$soure_space\"/\"$tbs_space\"/g" -e "s/$source_schema/$target_schema/g" -e "s/$source_table/$target_table/g" -e "s/CONNECT TO ${ExportDbName} USER ${ExportDbUser}/CONNECT TO ${LoadDbName} USER ${LoadDbUser} using ${LoadDbPassword}/g" |tee ${source_table}.sql
	fi 
	done
}
 Get_Tbstructure()
 { 
     db2look -d ${ExportDbName} -z $source_schema -noview -e -I ${ExportDbUser} -W ${ExportDbPassword} -t ${source_table} |tr 'a-z' 'A-Z' |tee ${source_table}.sql 
     #chmod 755 ${source_table}.sql 
     file_deal 
     if [ $? -ne 0 ] 
     then
         echo "get tbs_structure of ${source_table} fail...."
         exit -1
     fi
    ##cat yj_online_user_201402.sql |awk '$1 !~/--/ && NF>0 && $NF !~/;$/' |while read line
    # echo "get fields type of ${export_tb1}........\n"
    # cat ${source_table}.sql|awk '{if($0~/^CREATE TABLE/){has=1;} else if ($NF==")"){has=2;} else {if (has!=1) has=0;} if(has>=1 && $1!~/^CREATE/) {gsub(/"/,""); print $0}}'|tee fields.sql
    # val1=$(cat fields.sql)
    # printf "${val1} \n"
    # echo "get partitioning key of ${export_tb1}........\n"
    # val2=$(cat ${source_table}.sql|awk '{if($1~/^DISTRIBUTE/){has=1;} else if ($NF ~/")$/ || $NF==";" ){has=2;} else {if (has!=1) has=0;} if(has && ($1~/^DISTRIBUTE/ || $1~/^"/)) {gsub(/"/,""); print $0} }')
    # printf "${val2}\n" 
 }
  
 Create()
 {
 	db2 "connect to ${LoadDbName} user ${LoadDbUser} using ${LoadDbPassword}" 
 	if [ $? -ne 0 ]
 	then 
 	  printf "connect to ${LoadDbName} fail......"
 	  return 1
   fi
   echo "${LoadDbName} connect success ........\n"
   db2 "drop table ${load_tb1}"
   echo "drop table ${load_tb1} success ........\n"
   printf "create table ${load_tb1}..................." 
   db2 -tvf ${source_table}.sql 
   echo $? 
   if [ $? -ne 0 ] 
   then
         printf "create table name :${load_tb1} fail."
         return 1
   fi
      db2 terminate 
 }	

Get_Tbstructure
Create