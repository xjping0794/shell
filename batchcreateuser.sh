#!/usr/bin/ksh
for user in `cat /root/userlist`
do
    useradd $user
    echo "${user}!987"|passwd --stdin $user
    echo "user $user create success..."
done
