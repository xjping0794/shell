#!/usr/bin/ksh
################################################################
# author:xiaojp
# create date:2015-03-27 2015-04-18
# description：查找程序的后置依赖，按表层次分级
#              剔除以"#"注释的依赖
#              生成最终依赖文件： ${find_soure_tb}.csv
#              20150421 剔除以"--"注释的依赖
#              20150422 ,完善查询依赖，如tc_usr_interest_bill_yyyymm.tcl后续依赖tc_usr_interest_bill_tobass_yyyymm.tcl
#              /sharedata/user/xiaojp/bin/find_depend_relation.sh -t M06074
################################################################
set -xv 
code_find_path="/home/aibass/dwapp/bin"
log_path="/sharedata/user/xiaojp/log"
file_path="/sharedata/user/xiaojp"
if [ $# -eq 2 ]; then
   while [ $# -ne 0 ]; do
      if [ "x$1" == "x-t" ]; then
         shift
         find_soure_tb=`echo $1|sed 's/[[:space:]]*//g'`
         shift
      fi
   done
else
   printf "usage proc: $0 -t table \n"
   exit -1
fi

## record detail log info
#write_log $msg


function write_log {
	if [ ! -d "$log_path" ]
	then
	mkdir -p $log_path
	fi
	msg=$1
	currenttime=`date "+%F %T"`
	echo "${currenttime} : $msg" >>${log_path}/${find_soure_tb}.log`date +%Y%m%d`
}
# remove file
function drop_file {
	if [ -f "$1" ]; then
	rm -f $1
	write_log "delete文件$1成功......"
	else
	write_log "文件$1不存在，无需删除......"
	fi
}
function write_file {
	file1=$1
	echo "$file1" >>${log_path}/${find_soure_tb}.txt`date +%Y%m%d`
}

function produce_csv {
	file1=$1
	serial_no=$2
	echo "$file1" >>${log_path}/${find_soure_tb}_${serial_no}.csv 
}

function file_uniq {
	file=$1  
	cat ${log_path}/${file} |sort -u|tee -a ${log_path}/tmp_${file}
	drop_file ${log_path}/$file
	mv ${log_path}/tmp_${file} ${log_path}/${file}
}


function grep_file {
	file2=$1
	flag=$(grep -i "${file2}" ${log_path}/${find_soure_tb}.txt`date +%Y%m%d`)
	if [ -z "$flag" ]
	then
	echo true
	else
	echo false
	fi
}

# delete "#" head note depend to get correct depend relation
function get_correct_depend {
file_name=$1
findlevel=$2
write_log "delete file ${file_path}/${find_soure_tb}_${findlevel}_${findtb}_tmp.txt......"
drop_file ${file_path}/${find_soure_tb}_${findlevel}_${findtb}_tmp.txt
cat ${file_path}/${file_name}|while read line
do
echo $line|awk -F ":" '{if($2 !~ /^#/ && $2 !~ /^[[:space:]]*--[[:space:]]*/) print $1}'|awk -F "/" '{gsub(/[[:blank:]]*/,"");print $7}'|tee -a ${file_path}/${find_soure_tb}_${findlevel}_${findtb}_tmp.txt
done
write_log "delete file ${file_path}/${find_soure_tb}_${findlevel}_${findtb}.txt......"
drop_file ${file_path}/${find_soure_tb}_${findlevel}_${findtb}.txt
drop_file ${file_path}/${find_soure_tb}_${findlevel}_${findtb}_tmp1.txt
cat ${file_path}/${find_soure_tb}_${findlevel}_${findtb}_tmp.txt|sort -u|tee -a ${file_path}/${find_soure_tb}_${findlevel}_${findtb}_tmp1.txt
for procname in `cat ${file_path}/${find_soure_tb}_${findlevel}_${findtb}_tmp1.txt`
do
      procname1=${procname}
      procname=${procname%%.tcl}
	    procname=${procname%%.sh}
	    procname=${procname%%_yyyymmdd}
	    procname=${procname%%_yyyymm}
	    procname=${procname%%_yyyy}
	    if [ "${procname}" != "${findtb}" ]
	    then
	       echo $procname1 >>${file_path}/${find_soure_tb}_${findlevel}_${findtb}.txt
	    fi
done
write_log "delete file ${file_path}/${find_soure_tb}_${findlevel}_${findtb}_tmp.txt......"
drop_file ${file_path}/${find_soure_tb}_${findlevel}_${findtb}_tmp.txt
drop_file ${file_path}/${find_soure_tb}_${findlevel}_${findtb}_tmp1.txt
}
# to find table direct depend
#
function find_direct_depend {
	findtb=$1
	level=$2
	write_log "delete file ${file_path}/${findtb}_${level}.txt......"
	drop_file ${file_path}/${findtb}_${level}.txt
	write_log "find ${code_find_path} -group aigrp -name "*.tcl" -o -name "*.sh"|grep -vE \"test|bass1gen|offline\"|xargs grep -i \"${findtb}\"|tr [A-Z] [a-z]|tee -a ${file_path}/${findtb}_${level}.txt"
	find ${code_find_path} -group aigrp -name "*.tcl" -o -name "*.sh"|grep -vE "test|bass1gen|offline"|xargs grep -i "${findtb}"|tr [A-Z] [a-z]|tee -a ${file_path}/${findtb}_${level}.txt
	write_log "delete # note depend to get correct depend relation"
	get_correct_depend ${findtb}_${level}.txt ${level}
	write_log "delete file ${file_path}/${findtb}_${level}.txt......"
	drop_file ${file_path}/${findtb}_${level}.txt
}
drop_file ${log_path}/${find_soure_tb}.log`date +%Y%m%d`
drop_file ${log_path}/${find_soure_tb}.txt`date +%Y%m%d`
drop_file ${log_path}/${find_soure_tb}_0.csv
ftb=${find_soure_tb}
i=1
write_log "starting find $find_soure_tb  depend relations"
write_log "======find $find_soure_tb $i level depend relations====="
find_direct_depend ${ftb} $i
write_file ${ftb}
cd $file_path
filesize=$(cat ${find_soure_tb}_${findlevel}_${findtb}.txt|wc -c)
if [ $filesize -ne 0 ]
then
	    j=`expr $i + 1`
	    write_log "======find $find_soure_tb $j level depend relations====="
	    filename=${find_soure_tb}_${findlevel}_${findtb}.txt
	    findtb_substr=${filename##${find_soure_tb}_${findlevel}_}
	    findtb_pre=$(echo "${findtb_substr}"|awk -F "." '{print $1}') 
	    drop_file ${log_path}/${find_soure_tb}_$i.csv
	    for ftb in `cat ${find_soure_tb}_${findlevel}_${findtb}.txt` 
	    do
	    produce_csv "${findtb_pre},${ftb}" $i
	    ftb=${ftb%%.tcl}
	    ftb=${ftb%%.sh}
	    ftb=${ftb%%_yyyymmdd}
	    ftb=${ftb%%_yyyymm}
	    ftb=${ftb%%_yyyy}
	    if $(grep_file $ftb)
	    then
	      find_direct_depend ${ftb} $j
	      write_file ${ftb}
	    else
	      write_log "${ftb}已经处理，不再查找....." 
	    fi
	    done
	    file_uniq ${find_soure_tb}_${i}.csv
else
	    write_log "${findtb} 查无依赖，退出......"
	    produce_csv "${find_soure_tb}" 0
	    file_uniq ${find_soure_tb}_0.csv
	    findlevel=1
fi
write_log "======findlevel is $findlevel ,should greate 1 to continue find next level depend relation ,otherwise end find====="
while [ $findlevel -gt 1 ]
do 
   flag=$(print -n `ls -l ${find_soure_tb}_${findlevel}_*.txt|awk '{if($5>0)print $9}'`)
   if [ -z "$flag" ]; then
       findlevel=0
       write_log "${findtb} 查无依赖，退出......"
   else 
       drop_file ${log_path}/${find_soure_tb}_${findlevel}.csv
       for filename in `ls -l ${find_soure_tb}_${findlevel}_*.txt|awk '{print $9}'`
       do 
          findtb_substr=${filename##${find_soure_tb}_${findlevel}_}
	        findtb_pre=$(echo "${findtb_substr}"|awk -F "." '{print $1}') 
	        serial_no=$(expr ${findlevel} - 1)
          filesize=$(cat ${filename}|wc -c)
          if [ $filesize -ne 0 ]
          then  
                  findtb_pre1=$(cat "${log_path}/${find_soure_tb}_${serial_no}.csv"|grep -i "${findtb_pre}")
                  for ftb in `cat ${filename}`
	                do
	                    produce_csv "${findtb_pre1},${ftb}" ${findlevel}
	                    ftb=${ftb%%.tcl}
	                    ftb=${ftb%%.sh}
	                    ftb=${ftb%%_yyyymmdd}
	                    ftb=${ftb%%_yyyymm}
	                    ftb=${ftb%%_yyyy}
	                    if $(grep_file $ftb)
	                    then
	                      find_direct_depend ${ftb} `expr $findlevel + 1`
	                      findlevel=`expr $findlevel - 1`
	                      write_file ${ftb}
	                    else
	                      write_log "${ftb}已经处理，不再查找....."
	                    fi
	                done
	         else
	               output=$(cat "${log_path}/${find_soure_tb}_${serial_no}.csv"|grep -i "${findtb_pre}")
	               produce_csv "${output}" ${findlevel}
	         fi
       done 
   fi
   serial_no=$(expr ${findlevel} - 1)
   write_log "将${find_soure_tb}_${serial_no}.csv中无后续依赖的放在${find_soure_tb}_${findlevel}.csv中....."
   for filename in `cat ${log_path}/${find_soure_tb}_${serial_no}.csv`
   do
      flag=$(grep -i "${filename}" ${log_path}/${find_soure_tb}_${findlevel}.csv)
      if [ -z "$flag" ]
	    then
	       produce_csv "${filename}" ${findlevel} 
	    fi
   done 
   file_uniq ${find_soure_tb}_${findlevel}.csv
   findlevel=`expr $findlevel + 1`
done
cd $log_path
write_log "${find_soure_tb} get final depend relation ${find_soure_tb}.csv......"
flag=$(print -n `ls -l ${find_soure_tb}*.csv|awk '{print $9}'`)
lastfilename=$(echo $flag|awk '{print $NF}')
mv $lastfilename ${find_soure_tb}.csv
write_log "${find_soure_tb} delete template file......"
for filename in `ls -l ${file_path}/${find_soure_tb}_*.txt|awk '{print $9}'`
do
drop_file $filename
done
for filename in `ls -l ${log_path}/${find_soure_tb}_*.csv|awk '{print $9}'`
do
drop_file $filename
done 
write_log "${find_soure_tb} execute successful......"
exit 0
