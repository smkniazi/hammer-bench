#!/bin/bash
#
#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to You under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# Author: Salman Niazi 2015


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $DIR/experiment-env.sh

if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters. Usage  upload-results {remote-loaction}   {compressed-file-name}"
    echo "i.e. upload-results user@machine.com:/home/user/   results.tgz"
    exit 0
fi


echo "Compressing $All_Results_Folder"
tar zcf $2 $All_Results_Folder
scp $2 $1
rm $2


