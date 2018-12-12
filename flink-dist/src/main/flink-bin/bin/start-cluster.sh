#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# 知识点：下面两行用于获取该文件所在的目录
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# Start the JobManager instance(s)

# 知识点：Bash 有一个 shopt内建命令可用来设置或取消设置许多 shell 选项。
# 其中一个选项是 nocasematch，如果设置了该选项，它会告诉 shell 在字符串匹配中忽略大小写。
shopt -s nocasematch
# 由于设置了nocasematch,所以如果HIGH_AVAILABILITY是"ZOOKEEPER" ，那么下面的判断条件也是true的
if [[ $HIGH_AVAILABILITY == "zookeeper" ]]; then
    echo $HIGH_AVAILABILITY"@@@@"
    # HA Mode
    readMasters

    echo "Starting HA cluster with ${#MASTERS[@]} masters."

    for ((i=0;i<${#MASTERS[@]};++i)); do
        master=${MASTERS[i]}
        webuiport=${WEBUIPORTS[i]}

        if [ ${MASTERS_ALL_LOCALHOST} = true ] ; then
            "${FLINK_BIN_DIR}"/jobmanager.sh start "${master}" "${webuiport}"
        else
            ssh -n $FLINK_SSH_OPTS $master -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/jobmanager.sh\" start ${master} ${webuiport} &"
        fi
    done

else
    echo "Starting cluster."

    # Start single JobManager on this machine
    # nocasematch效果在jobmanager.sh也会体现
    "$FLINK_BIN_DIR"/jobmanager.sh start
fi
shopt -u nocasematch

# Start TaskManager instance(s)
TMSlaves start
