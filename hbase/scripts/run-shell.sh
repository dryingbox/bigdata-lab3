#!/usr/bin/env bash
# 运行 HBase Shell 的 5 项任务并把完整输出保存到 output/shell-output.txt
set -euo pipefail
. /etc/profile.d/bigdata.sh   # 载入 JAVA_HOME / HADOOP/HBASE/SPARK 环境
cd "$(dirname "$0")/.."
hbase shell shell-tasks.hrb 2>&1 | tee output/shell-output.txt
