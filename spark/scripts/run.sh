#!/usr/bin/env bash
# 运行 Spark 版 PageRank
#   bash spark/scripts/run.sh            # 默认 local[2]
#   bash spark/scripts/run.sh yarn       # Hadoop YARN 模式（需先 start-yarn.sh）
set -euo pipefail
. /etc/profile.d/bigdata.sh   # 载入 JAVA_HOME / SPARK_HOME / HADOOP_CONF_DIR
cd "$(dirname "$0")/../.."    # lab3/
MODE="${1:-local[2]}"
mkdir -p spark/output
# 本机默认文件系统是 HDFS，本地文件必须显式加 file:// 前缀
if [[ "${MODE}" == yarn* ]]; then
    INPUT="hdfs://localhost:9000/lab3/input/links.txt"
else
    INPUT="file://$(pwd)/data/links.txt"
fi
if [[ "${MODE}" == yarn* ]]; then
    spark-submit --master yarn --deploy-mode client \
        --driver-memory 512m --executor-memory 512m --executor-cores 1 \
        --conf spark.executor.memoryOverhead=256m \
        spark/src/pagerank_spark.py "${INPUT}" spark/output/pagerank-result.txt 10
else
    spark-submit --master "${MODE}" \
        spark/src/pagerank_spark.py "${INPUT}" spark/output/pagerank-result.txt 10
fi
