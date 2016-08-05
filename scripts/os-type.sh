#!/bin/sh
os=$(grep  "^NAME=" "/etc/os-release" | cut -d'=' -f 2 | cut -d '"' -f 2)
if [ "$os" == "Ubuntu" ]; then
 echo "Ubuntu"
elif [ "$os" == "CentOS Linux" ]; then
 echo "CentOS"
else
 echo $os
fi

