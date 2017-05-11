#!/usr/bin/ksh
#set -x   启动"-x"选项 要跟踪的程序段
#set -o errexit 
# operator system is aix
##############################################################################################
# creator\time: xiaojp \201405
# 功能描述：找出表空间下的类表\大小及个数
# for example : find_sample_table.sh
#############x#################################################################################
    ## where TBSPACE in ('TBS_BASS15')
    ## where upper(tabschema) in ('DWSTR','DWPUB','DWCTR','DWAPP','DWMID','VGOP15_SAOP','VGOP15_MAOP','VGOP','BISUITEC10','BISUITE','BIMART')
    echo "all table export to file....." 
    tbsql="select cast(tabschema as char(15))||'.'||cast(tabname as char(100))||','||case when tabspace<1 and tabspace>0 then '0'||tabspace 
    when tabspace=0 then 0 else tabspace end
    from (select tabschema,tabname,sum(fpages)*32*1.0/1024/1024 tabspace
    from syscat.tables  
    group by  tabschema,tabname
    )
    "
    db2 connect to ynbidb user aibass using newbi1001
    ##db2 connect to ynbiyj user biyjinst using bass1gen_db
    if [ $? -ne 0 ]; then
        printf "ynbidb connect fail....."
        exit 1
        else
        printf "ynbidb connect success....."
    fi
  # awk 正则表达式 结合类型表示符没有了：不具备：”\d,\D,\s,\S,\t,\v,\n,\f,\r”其它功能基本一样的。
    db2 -x ${tbsql} >alltables.sql
    cat alltables.sql|awk -F "," '{if($1 ~/_[0-9]{8}$/) print $1}'
    echo "all day table export to file....."
    cat alltables.sql|awk '{gsub(/[[:blank:]]*/,""); print $0}'|awk -F "," '{if($1 ~/_[0-9]{8}$/) print $0}'|
    awk -F "," '{gsub(/[0-9]{8}$/,"yyyymmdd",$1);print $0}'|
    awk '{arr[$1]+=$2;frr[$1]++} END{for (col in arr) print col,"\t" arr[col],"\t",frr[col]}'|sort -k 1 |tee alltables1.sql
    echo "all month table export to file....."
    cat alltables.sql|awk '{gsub(/[[:blank:]]*/,""); print $0}'|awk -F "," '{if($1 ~/_[0-9]{6}$/) print $0}'|
    awk -F "," '{gsub(/[0-9]{6}$/,"yyyymm",$1);print $0}'|
    awk '{arr[$1]+=$2;frr[$1]++} END{for (col in arr) print col,"\t" arr[col],"\t",frr[col]}'|sort -k 1 |tee alltables2.sql
    echo "all hour table export to file....."
    cat alltables.sql|awk '{gsub(/[[:blank:]]*/,""); print $0}'|awk -F "," '{if($1 ~/_[0-9]{10}$/) print $0}'|
    awk -F "," '{gsub(/[0-9]{10}$/,"yyyymmddhh",$1);print $0}'|
    awk '{arr[$1]+=$2;frr[$1]++} END{for (col in arr) print col,"\t" arr[col],"\t",frr[col]}'|sort -k 1 |tee alltables3.sql
    echo "all nodate table export to file....."
    cat alltables.sql|awk '{gsub(/[[:blank:]]*/,""); print $0}'|awk -F "," '{if($1 !~/_[0-9]{10}$/ && $1 !~/_[0-9]{6}$/ && $1 !~/_[0-9]{8}$/ && $1!="") print $1,"\t",$2}'|
    sort -k 1| tee alltables4.sql
