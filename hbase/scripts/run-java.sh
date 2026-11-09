#!/usr/bin/env bash
# 运行 HBase Java API 演示：修改 Zhangsan 的成绩时由标准输入提供新值 95
set -euo pipefail
. /etc/profile.d/bigdata.sh   # 载入 JAVA_HOME / HADOOP/HBASE/SPARK 环境
cd "$(dirname "$0")/.."
echo 95 | java -cp "build:$(hbase classpath)" lab3.HBaseApi
