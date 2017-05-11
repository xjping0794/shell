#!/usr/bin/ksh
for comdatanode in `more hostnew`
do
   echo $comdatanode
   ssh $comdatanode "userdel ynhajob;useradd ynhajob;echo "'yncmcc2016$'"|passwd --stdin ynhajob"
   echo "user aibass  in $comdatanode create success..."
done