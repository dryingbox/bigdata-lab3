#!/usr/bin/env bash
# 编译 MapReduce 版 PageRank 并打包为可被 hadoop jar 运行的 jar
set -euo pipefail
. /etc/profile.d/bigdata.sh   # 载入 JAVA_HOME / HADOOP_HOME 等环境
cd "$(dirname "$0")/.."
rm -rf build lab3-pagerank-mr.jar
mkdir -p build
javac -encoding UTF-8 -cp "$(hadoop classpath)" -d build src/*.java
jar cf lab3-pagerank-mr.jar -C build .
echo "[build] 生成 $(pwd)/lab3-pagerank-mr.jar"
