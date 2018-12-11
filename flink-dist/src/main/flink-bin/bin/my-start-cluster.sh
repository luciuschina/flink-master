#!/usr/bin/env bash
# 我自己的脚本，加一些环境变量.

export FLINK_CONF_DIR=/data/work/luciuschina/flink-master/flink-dist/src/main/resources

bin=`dirname "$0"`
# 获取该文件所在的目录
bin=`cd "$bin"; pwd`

. "$bin"/start-cluster.sh


