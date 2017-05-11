#!/bin/ksh
set -x

. ${ETL_PATH}/etc/db.cfg
. ${ETL_PATH}/etc/path.cfg

SHELL_PATH=${ETL_PATH}/shell
INI_PATH=${ETL_PATH}/shell/inifiles
. ${INI_PATH}/waiwei_ftp.ini

while [ $# -ne 0 ] ; do
    if [ x$1 == "x-u" ] ; then
        shift
        U_CODE=$1
        shift
    elif [ x$1 == "x-v" ] ; then
        shift
        V_CODE=$1
        shift
    elif [ x$1 == "x-p" ] ; then
        shift
        ITFC_CODE=$(echo $1 | tr -s ", \r\n\t" " ")
        shift
    elif [ x$1 == "x-t" ] ; then
        shift
        ITFC_DATE=`echo $1 | tr -d " -./"`
        shift
    elif [ x$1 == "x-r" ] ; then
        shift
        RE_DO=`echo $1`
        shift
    else
        echo "Unrecorgnized prarmter $1, ignore" >&2
        shift
        echo "${U_CODE}.${V_CODE}.4" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_DATE}.log
        echo "" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_DATE}.log
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files :the params is not right !!!" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_DATE}.log
        exit -1
    fi
done

delline()
{
vi $1 <<EOF
:g/$2/d
:wq
EOF
}

function INSERT_SMS_INFO {
db2 connect to ${APDB_NAME} user ${APDB_USER} using ${APDB_PASSWORD}

if [ `echo ${APDB_NAME} | tr [a-z] [A-Z]` == "YNBIYJ" ];then
    SMS_SCHEMA="BASS15"
elif [ `echo ${APDB_NAME} | tr [a-z] [A-Z]` == "WEBDB209" ];then
    SMS_SCHEMA="LOCALAPP"
fi

db2 -tx "
SELECT SMS_PHONE_NUMBER
  FROM ${SMS_SCHEMA}.ETL_SMS_PHONE
 WHERE SMS_TYPE = 'ITFC_P08077_CHECK';
" | while read SMS_PHONE_NUMBER
do

if [ `echo ${APDB_NAME} | tr [a-z] [A-Z]` == "YNBIYJ" ];then
    SMS_CONTENT0="一经"
elif [ `echo ${APDB_NAME} | tr [a-z] [A-Z]` == "WEBDB209" ];then
case ${ITFC_CODE} in
    "P08077"|"AN5062"|"PN1091"|"PN1014"|"AN1901"|"A01101" |"PN4002"|"P04018"|"P08059"|"P03001"|"PN1205"|"PN1201"|"PN1002"|"PN4005"|"A05081"|"PN5050" |"AN5028"|"P01020"|"AN5053"|"A01055"|"P07061"|"A01074" |"A01059"|"P07064"|"AN1015"|"P03005"|"A15016"|"A15020"|"A05001"|"A15013") 
    SMS_CONTENT0="二经彩信";;
    *) 
    SMS_CONTENT0="二经";;
esac
fi

db2 -tx "
INSERT INTO ${SMS_SCHEMA}.ETL_SMS (PHONE,
                              CONTENT,
                              STATUS,
                              STARTTIME,
                              SENDTIME,
                              LEVEL)
VALUES ('${SMS_PHONE_NUMBER}',
        '【严重告警】截至'||to_char(current timestamp ,'YYYY-MM-DD HH24:MI:SS')||'网管NOP的基站信息数据,[${ITFC_DATE_A1}]数据日期没有发出,请尽快核查处理,谢谢.经分组',
        '0',
        CURRENT TIMESTAMP,
        NULL,
        '9');
"
done
db2 terminate
}

ITFC_DATE_A1=`${SHELL_PATH}/date_g -a 1 ${ITFC_DATE}`
ITFC_DATE_B1=`${SHELL_PATH}/date_g -b 1 ${ITFC_DATE}`
#YEAR_A2=`expr substr ${ITFC_DATE} 1 4`
#MONTH_A2=`expr substr ${ITFC_DATE} 5 2`
#DAY_A2=`expr substr ${ITFC_DATE_A1} 7 2`
#ITFC_DATE_A2=${YEAR_A2}"-"${MONTH_A2}"-"${DAY_A2}
#echo "ITFC_DATE_A2"

#----------CHECK ITFC_DATE----------#
if [ `expr length ${ITFC_DATE}` == 8 ] ; then
        DES_DATA_PATH=/sharedata/bassfs/backupload/${ITFC_DATE}
        LAST_DATA_PATH=/sharedata/bassfs/backupload/${ITFC_DATE_B1}
elif [ `expr length ${ITFC_DATE}` == 6 ] ; then
        DES_DATA_PATH=/sharedata/bassfs/backupmonth/${ITFC_DATE}
        YEAR=`expr substr ${ITFC_DATE} 1 4`
        MONTH=`expr substr ${ITFC_DATE} 5 2`
        LAST_MONTH_DAY=${YEAR}${MONTH}`cal ${MONTH} ${YEAR}|xargs |awk '{print $NF}'`
else 
        echo "${U_CODE}.${V_CODE}.4" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} is error!" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        exit -1
fi

#----------CHECK DES_DATA_PATH----------#
if [ ! -d ${DES_DATA_PATH} ];then
        mkdir -p ${DES_DATA_PATH}
fi

if [ ! -d ${DES_DATA_PATH} ];then
        echo "${U_CODE}.${V_CODE}.4" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files:the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} :the path:${DES_DATA_PATH} is not exists !!!" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        exit -1
fi

FTP_IP=$(eval echo $`echo $ITFC_CODE`_FTP_IP)
FTP_USER_NAME=$(eval echo $`echo $ITFC_CODE`_FTP_USER_NAME)
FTP_PASS_WORD=$(eval echo $`echo $ITFC_CODE`_FTP_PASS_WORD)
FTP_DATA_PATH=$(eval echo $`echo $ITFC_CODE`_FTP_DATA_PATH)
FTP_FILE_NUMS=$(eval echo $`echo $ITFC_CODE`_FTP_FILE_NUMS)


if [ ${ITFC_CODE} == A09027 ]; then
        FTP_FILE_LIST="${ITFC_DATE}*.gz"
elif [ ${ITFC_CODE} == A14021 ]; then
        FTP_FILE_LIST="umts_mm_${ITFC_DATE}*.ack"
elif [ ${ITFC_CODE} == M96019 ]; then            
        FTP_FILE_LIST="P96019${ITFC_DATE}*.AVL"        
elif [ ${ITFC_CODE} == P08077 ]; then
        FTP_FILE_LIST="rms_jf_cell_23G_${ITFC_DATE_A1}*.unl"
elif [ ${ITFC_CODE} == P08078 ]; then
        FTP_FILE_LIST="cell_gt_report_${ITFC_DATE_A1}*.unl"
elif [ ${ITFC_CODE} == P08079 ]; then
        FTP_FILE_LIST="cell_gw_report_${ITFC_DATE_A1}*.unl"
elif [ ${ITFC_CODE} == P08080 ]; then
        FTP_FILE_LIST="cell_tw_report_${ITFC_DATE_A1}*.unl"
elif [ ${ITFC_CODE} == P08085 ]; then
        FTP_FILE_LIST="rms_jf_eutrancell_${ITFC_DATE_A1}*.unl"
elif [ ${ITFC_CODE} == A09028 ]; then
       FTP_FILE_LIST="TBANKLK${ITFC_DATE}*.DAT"
elif [ ${ITFC_CODE} == I40401 ]; then
        FTP_FILE_LIST="i_12900_${ITFC_DATE}_40401_00.verf"
elif [ ${ITFC_CODE} == I40402 ]; then
        FTP_FILE_LIST="i_12900_${ITFC_DATE}_40402_00.verf"
elif [ ${ITFC_CODE} == I40403 ]; then
        FTP_FILE_LIST="i_12900_${ITFC_DATE}_40403_00_000.dat"
elif [ ${ITFC_CODE} == A40404 ]; then
        FTP_FILE_LIST="s_12900_${ITFC_DATE}_40404_00.verf"
elif [ ${ITFC_CODE} == A40405 ]; then
        FTP_FILE_LIST="s_12900_${ITFC_DATE}_40405_00.verf"
elif [ ${ITFC_CODE} == A40406 ]; then
        FTP_FILE_LIST="i_12900_${ITFC_DATE}_40406_00_000.dat"
elif [ ${ITFC_CODE} == I40407 ]; then
        FTP_FILE_LIST="i_12900_${ITFC_DATE}_40407_00_000.dat"
elif [ ${ITFC_CODE} == M91002 ]; then
        FTP_FILE_LIST="i_30000_BAS_91002_${ITFC_DATE}*.dat"
elif [ ${ITFC_CODE} == M91003 ]; then
        FTP_FILE_LIST="i_30000_BAS_91003_${ITFC_DATE}*.dat"
elif [ ${ITFC_CODE} == A09029 ]; then
        FTP_FILE_LIST="BASS_${ITFC_DATE}.csv"
elif [ ${ITFC_CODE} == A09031 ]; then
        FTP_FILE_LIST="cellkpi_hour_gsm${ITFC_DATE}.csv"
elif [ ${ITFC_CODE} == A09032 ]; then                           
        FTP_FILE_LIST="mrkpi_hour_gsm${ITFC_DATE}.csv"
elif [ ${ITFC_CODE} == A09033 ]; then                           
        FTP_FILE_LIST="dapkpi_hour${ITFC_DATE}.csv"
elif [ ${ITFC_CODE} == A09034 ]; then                           
        FTP_FILE_LIST="utrancellkpi_td${ITFC_DATE}.csv"
elif [ ${ITFC_CODE} == A09035 ]; then                           
        FTP_FILE_LIST="carrierkpi_td${ITFC_DATE}.csv"
elif [ ${ITFC_CODE} == A09036 ]; then                           
        FTP_FILE_LIST="eutrancellkpi_lte${ITFC_DATE}.csv"
elif [ ${ITFC_CODE} == M09037 ]; then                           
        FTP_FILE_LIST="tdmr_${ITFC_DATE}01.csv"
elif [ ${ITFC_CODE} == A09038 ]; then
        FTP_FILE_LIST="${ITFC_DATE_A1}_xinxi_alarm*.csv"
elif [ ${ITFC_CODE} == A09039 ]; then
        FTP_FILE_LIST="TDOutOfServiceCellDetail_xinxi_${ITFC_DATE_A1}.unl"            
elif [ ${ITFC_CODE} == P09040 ]; then
        FTP_FILE_LIST="wirless_useage_td${ITFC_DATE}.csv"
elif [ ${ITFC_CODE} == P09041 ]; then
        FTP_FILE_LIST="wirless_useage_gsm${ITFC_DATE}.csv"
elif [ ${ITFC_CODE} == P08638 ]; then
        FTP_FILE_LIST="i_he_subs_18_${ITFC_DATE}_000001.txt"
elif [ ${ITFC_CODE} == M09204 ]; then                           
        FTP_FILE_LIST="miguvipdaoda_yunnan_${ITFC_DATE}*.txt"          
elif [ ${ITFC_CODE} == M09229 ]; then                           
        FTP_FILE_LIST="tonebox_yunnan_${ITFC_DATE}*.txt"        
elif [ ${ITFC_CODE} == M09230 ]; then                           
        FTP_FILE_LIST="cailing_yunnan_${ITFC_DATE}*.txt"        
elif [ ${ITFC_CODE} == M09231 ]; then                           
        FTP_FILE_LIST="kehuduanshiyong_yunnan_${ITFC_DATE}*.txt"
elif [ ${ITFC_CODE} == M09232 ]; then                           
        FTP_FILE_LIST="zhenling_yunnan_${ITFC_DATE}*.txt"       
elif [ ${ITFC_CODE} == M09234 ]; then                           
        FTP_FILE_LIST="gaojihuiyuan_yunnan_${ITFC_DATE}*.txt"

elif [ ${ITFC_CODE} == P08074 ]; then
        FTP_FILE_LIST="HOME_CELL_INSTALL_REPORT#${ITFC_DATE}*#1.csv"
else
        echo "${U_CODE}.${V_CODE}.4" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} is error!" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        exit -1
fi

#----------WAITING FTP LOOP----------#

TIME_NUMBERS=24
TIME_INTERVALE=`expr 15 \* 60`
TIME_INIT=0

#----------INTERFACE P08077 SPECIAL DEAL----------#
#----------IF THE CURRENT DAY DATAFILE IS NOT REACH,THEN USE THE LAST DAY DATAFILES----------#
if [ Z${ITFC_CODE} == 'ZP08077' ];then
    TIME_NUMBERS=2
fi

while [ ${TIME_INIT} -le ${TIME_NUMBERS} ] ; do

if [ Z${FTP_MODE} == "ZPASV" ];then

ftp -i -n ${FTP_IP} <<EOF> ${DES_DATA_PATH}/waiwei_ftp_${ITFC_CODE}_${ITFC_DATE}.list
user ${FTP_USER_NAME} ${FTP_PASS_WORD}
passive
cd ${FTP_DATA_PATH}
ls ${FTP_FILE_LIST}
bye
EOF

delline ${DES_DATA_PATH}/waiwei_ftp_${ITFC_CODE}_${ITFC_DATE}.list Passive >/dev/null 2>&1
FILE_NUMS_TEMP=`grep -v "Passive" ${DES_DATA_PATH}/waiwei_ftp_${ITFC_CODE}_${ITFC_DATE}.list | awk 'END {print NR}' `

else

ftp -i -n ${FTP_IP} <<EOF> ${DES_DATA_PATH}/waiwei_ftp_${ITFC_CODE}_${ITFC_DATE}.list
user ${FTP_USER_NAME} ${FTP_PASS_WORD}
cd ${FTP_DATA_PATH}
ls ${FTP_FILE_LIST}
bye
EOF

FILE_NUMS_TEMP=`awk 'END {print NR}' ${DES_DATA_PATH}/waiwei_ftp_${ITFC_CODE}_${ITFC_DATE}.list`

fi


REMOTE_PATH_ERROR=`grep "Failed to change directory" ${DES_DATA_PATH}/waiwei_ftp_${ITFC_CODE}_${ITFC_DATE}.list |wc -l`
if [ ${REMOTE_PATH_ERROR} -ge 1 ];then
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} :change remote path is error !!!"
        echo "${U_CODE}.${V_CODE}.4" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} :change remote path is error !!!" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        exit -1
fi

NO_FILE_ERROR_1=`grep "Bad directory components" ${DES_DATA_PATH}/waiwei_ftp_${ITFC_CODE}_${ITFC_DATE}.list |wc -l`
NO_FILE_ERROR_2=`grep "No such file or directory" ${DES_DATA_PATH}/waiwei_ftp_${ITFC_CODE}_${ITFC_DATE}.list |wc -l`
if [ ${NO_FILE_ERROR_1} -ge 1 -o ${NO_FILE_ERROR_2} -ge 1 ];then
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} :no file error !!!"
        TIME_INIT=`expr ${TIME_INIT} + 1`
        sleep ${TIME_INTERVALE}
        continue
fi

for SRC_FILE_NAME in `cat ${DES_DATA_PATH}/waiwei_ftp_${ITFC_CODE}_${ITFC_DATE}.list`
do 

        #----------CHECK SRC_FILE_NAME----------#
        if [ -z ${SRC_FILE_NAME} ] ;then
                continue
        fi
        
        if [ ${ITFC_CODE} == A09027 ]; then
                DES_FILE_NAME=${ITFC_CODE}`echo ${SRC_FILE_NAME} | awk -F. '{print $1}'`.AVL.gz
        elif [ ${ITFC_CODE} == A14021 ]; then
                SRC_FILE_NAME=${SRC_FILE_NAME%%.*}.dat
                DES_FILE_NAME=${ITFC_CODE}`echo ${SRC_FILE_NAME} | awk -F. '{print substr($1,9)}'`.AVL
        elif [ ${ITFC_CODE} == M96019 ]; then
               SRC_FILE_NAME=${SRC_FILE_NAME%%.*}.AVL
               DES_FILE_NAME=${ITFC_CODE}`echo ${SRC_FILE_NAME} | awk -F. '{print substr($1,7)}'`.AVL
        elif [ ${ITFC_CODE} == P08077 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == P08078 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == P08079 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == P08080 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == P08085 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == A09028 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == I40401 ]; then
                SRC_FILE_NAME=${SRC_FILE_NAME%%.*}_000.dat
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000000.AVL
        elif [ ${ITFC_CODE} == I40402 ]; then
                SRC_FILE_NAME=${SRC_FILE_NAME%%.*}_000.dat
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000000.AVL
        elif [ ${ITFC_CODE} == I40403 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000000.AVL
        elif [ ${ITFC_CODE} == A40404 ]; then
                SRC_FILE_NAME=${SRC_FILE_NAME%%.*}_000.dat
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000000.AVL
        elif [ ${ITFC_CODE} == A40405 ]; then
                SRC_FILE_NAME=${SRC_FILE_NAME%%.*}_000.dat
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000000.AVL
        elif [ ${ITFC_CODE} == A40406 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000000.AVL
        elif [ ${ITFC_CODE} == I40407 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000000.AVL
        elif [ ${ITFC_CODE} == M91002 ]; then
                DES_FILE_NAME=${ITFC_CODE}${LAST_MONTH_DAY}000001.AVL
        elif [ ${ITFC_CODE} == M91003 ]; then
                DES_FILE_NAME=${ITFC_CODE}${LAST_MONTH_DAY}000001.AVL
        elif [ ${ITFC_CODE} == A09029 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL 
        elif [ ${ITFC_CODE} == A09039 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL                                
        elif [ ${ITFC_CODE} == A09038 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == A09029 ]; then                   
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == A09031 ]; then                   
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == A09032 ]; then                   
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == A09033 ]; then                   
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == A09034 ]; then                   
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == A09035 ]; then                   
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == A09036 ]; then                   
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == M09037 ]; then                   
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == P09040 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == P09041 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == P08638 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
        elif [ ${ITFC_CODE} == M09204 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}00000001.AVL
        elif [ ${ITFC_CODE} == M09229 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}00000001.AVL        
        elif [ ${ITFC_CODE} == M09230 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}00000001.AVL
        elif [ ${ITFC_CODE} == M09231 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}00000001.AVL
        elif [ ${ITFC_CODE} == M09232 ]; then
                DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}00000001.AVL
        elif [ ${ITFC_CODE} == M09234 ]; then
               DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}00000001.AVL
        elif [ ${ITFC_CODE} == P08074 ]; then
               DES_FILE_NAME=${ITFC_CODE}${ITFC_DATE}000001.AVL
 
        else
                echo "${U_CODE}.${V_CODE}.4" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
                echo "" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
                echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} is error!" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
                exit -1
        fi
        
        DES_AVL_FILE=${DES_FILE_NAME%%.*}.AVL
        DES_CHK_FILE=${DES_FILE_NAME%%.*}.CHK
        
        if [ ! -f ${DES_DATA_PATH}/${DES_AVL_FILE}* ] || [ -z ${RE_DO} -a ${RE_DO} == "true" ];then

if [ Z${FTP_MODE} == "ZPASV" ];then

#----------FTP DES_FILE_NAME---------#
ftp -i -n ${FTP_IP} <<EOF
user ${FTP_USER_NAME} ${FTP_PASS_WORD}
passive
cd ${FTP_DATA_PATH}
lcd ${DES_DATA_PATH}
bin
get ${SRC_FILE_NAME} ${DES_FILE_NAME}
bye
EOF

else

#----------FTP DES_FILE_NAME---------#
ftp -i -n ${FTP_IP} <<EOF
user ${FTP_USER_NAME} ${FTP_PASS_WORD}
cd ${FTP_DATA_PATH}
lcd ${DES_DATA_PATH}
bin
get ${SRC_FILE_NAME} ${DES_FILE_NAME}
bye
EOF

fi

                #----------CHECK FTP DES_FILE_NAME----------#
                if [ ! -f ${DES_DATA_PATH}/${DES_FILE_NAME} ];then
                        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data file: ${SRC_FILE_NAME} to ${DES_FILE_NAME} is error !!!"
                elif [ ${DES_FILE_NAME##*.} == gz ] ;then
                        gzip -d ${DES_DATA_PATH}/${DES_FILE_NAME}
                fi
                
                #----------CREATE CHK FILE----------#
                cd ${DES_DATA_PATH}
                if [ ! -f ${DES_DATA_PATH}/${DES_CHK_FILE} ];then
                        wc ${DES_AVL_FILE} |awk 'BEGIN {OFS=","} {print $4,$3,$1}' > ${DES_DATA_PATH}/${DES_CHK_FILE}
                fi
        else
                #----------UNGZIP DES_FILE_NAME----------#
                if [ -f ${DES_DATA_PATH}/${DES_FILE_NAME} -a ${DES_FILE_NAME##*.} == gz ] ;then
                        gzip -d ${DES_DATA_PATH}/${DES_FILE_NAME}
                fi
                
                #----------CREATE CHK FILE----------#
                cd ${DES_DATA_PATH}
                if [ ! -f ${DES_DATA_PATH}/${DES_CHK_FILE} ];then
                        wc ${DES_AVL_FILE} |awk 'BEGIN {OFS=","} {print $4,$3,$1}' > ${DES_DATA_PATH}/${DES_CHK_FILE}
                fi
        fi

done

#----------CHECK FILE_NUMS----------#
if [ ${FILE_NUMS_TEMP} -ne ${FTP_FILE_NUMS} ];then
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} :the file nums:${FILE_NUMS_TEMP} is not match the config nums:${FTP_FILE_NUMS} !!!"
        TIME_INIT=`expr ${TIME_INIT} + 1`
        sleep ${TIME_INTERVALE}
        continue
else

db2 connect to ${CFG_DBNAME} user ${CFG_USERNAME} using ${CFG_PASSWORD}
db2 -t "
INSERT INTO ETL.INTERFACE_FTP_INFO
       (RUN_FTP_ID,
        INTERFACECODE,
        FILEDATE,
        FTP_START_TIME,
        FTP_END_TIME,
        COMPLETEFLAG,
        DATAFILENUMS,
        FTPID,
        FILEROWCOUNTS,
        FILESIZE)
VALUES (
          999999,
          '${ITFC_CODE}',
          '${ITFC_DATE}',
          TO_CHAR(current timestamp,'YYYY-MM-DD HH:MI:SS'),
          TO_CHAR(current timestamp,'YYYY-MM-DD HH:MI:SS'),
          1,
          ${FTP_FILE_NUMS},
          '200_b_display_day',
          99999999,
          99999999
       );
"
db2 terminate

        echo "${U_CODE}.${V_CODE}.3" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "[FINISH]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} is finished !!!" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        break;
fi

done

if [ ${NO_FILE_ERROR_1} -ge 1 ];then
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} :no file error !!!"
        echo "${U_CODE}.${V_CODE}.4" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} :no file error !!!" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        exit -1
fi

if [ ${FILE_NUMS_TEMP} -ne ${FTP_FILE_NUMS} -a Z${ITFC_CODE} != 'ZP08077' ] || [ ${NO_FILE_ERROR_2} -ge 1 -a Z${ITFC_CODE} != 'ZP08077' ] ;then
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} :the file nums:${FILE_NUMS_TEMP} is not match the config nums:${FTP_FILE_NUMS} !!!"
        echo "${U_CODE}.${V_CODE}.4" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        echo "[ERROR]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} :the file nums:${FILE_NUMS_TEMP} is not match the config nums:${FTP_FILE_NUMS} !!!" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
        exit -1
elif [ ${FILE_NUMS_TEMP} -ne ${FTP_FILE_NUMS} -a Z${ITFC_CODE} == 'ZP08077' ] || [ ${NO_FILE_ERROR_2} -ge 1 -a Z${ITFC_CODE} == 'ZP08077' ] ;then
    SRC_AVL_FILE=P08077${ITFC_DATE_B1}000001.AVL
    DES_AVL_FILE=P08077${ITFC_DATE}000001.AVL
    DES_CHK_FILE=P08077${ITFC_DATE}000001.CHK
    cp ${LAST_DATA_PATH}/${SRC_AVL_FILE} ${DES_DATA_PATH}/${DES_AVL_FILE}
    cd ${DES_DATA_PATH}
    wc ${DES_AVL_FILE} |awk 'BEGIN {OFS=","} {print $4,$3,$1}' > ${DES_DATA_PATH}/${DES_CHK_FILE}

INSERT_SMS_INFO 1

db2 connect to ${CFG_DBNAME} user ${CFG_USERNAME} using ${CFG_PASSWORD}
db2 -t "
INSERT INTO ETL.INTERFACE_FTP_INFO
       (RUN_FTP_ID,
        INTERFACECODE,
        FILEDATE,
        FTP_START_TIME,
        FTP_END_TIME,
        COMPLETEFLAG,
        DATAFILENUMS,
        FTPID,
        FILEROWCOUNTS,
        FILESIZE)
VALUES (
          999999,
          '${ITFC_CODE}',
          '${ITFC_DATE}',
          TO_CHAR(current timestamp,'YYYY-MM-DD HH:MI:SS'),
          TO_CHAR(current timestamp,'YYYY-MM-DD HH:MI:SS'),
          1,
          ${FTP_FILE_NUMS},
          '200_b_display_day',
          99999999,
          99999999
       );
"
db2 terminate
    echo "${U_CODE}.${V_CODE}.3" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
    echo "" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
    echo "[FINISH]:[`date +%Y-%m-%d` `date +%H:%M:%S`]:ftp waiwei data files the interface code:${ITFC_CODE} :the interface date:${ITFC_DATE} is finished !!!" >> ${AGENT_LOG_PATH}/waiwei_ftp_${U_CODE}_${V_CODE}_${ITFC_CODE}.log
fi