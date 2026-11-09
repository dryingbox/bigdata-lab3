#!/usr/bin/env bash
# 编译 HBase Java API 示例：javac -cp "$(hbase classpath)"
set -euo pipefail
. /etc/profile.d/bigdata.sh   # 载入 JAVA_HOME / HADOOP/HBASE/SPARK 环境
cd "$(dirname "$0")/.."
mkdir -p build
javac -encoding UTF-8 -cp "$(hbase classpath)" -d build src/HBaseApi.java
echo "[build] 编译完成：hbase/build/lab3/HBaseApi.class"
