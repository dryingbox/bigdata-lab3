# 大数据管理与分析 实验三：PageRank 算法实现

本仓库包含《实验三 PageRank算法实现》的全部代码与可复现步骤：

* **第一部分 HBase**：HBase Shell 完成 5 项操作、Java API 实现 5 个函数（`createTable` / `addRecord` / `scanColumn` / `modifyData` / `deleteRow`）；
* **第二部分 PageRank**：分别用 Hadoop MapReduce 与 Spark（PySpark RDD）实现，迭代 10 轮，输出 `(网页,PR值)`，PR 值保留 10 位小数。

## 目录结构

```
lab3/
├── data/
│   ├── links.txt                 # 输入：15 个大数据组件网页及其出链，格式 "网页<TAB>出链1,出链2"
│   └── expected-output.txt       # 本地参考实现（tools/reference_pagerank.py）算出的标准答案
├── hbase/
│   ├── src/HBaseApi.java         # Java API：题目要求的 5 个函数 + 演示 main
│   ├── shell-tasks.hrb           # HBase Shell 5 项任务脚本
│   ├── scripts/{build,run-java,run-shell}.sh
│   └── output/                   # 运行输出（shell-output.txt、java-api-output.txt）
├── mapreduce/
│   ├── src/PageRankIteration.java  # 一轮迭代的 Mapper / Reducer
│   ├── src/PageRankDriver.java     # 初始化图、迭代 10 轮、格式化输出
│   ├── scripts/build.sh            # javac + jar 打包
│   └── output/pagerank-result.txt  # MapReduce 运行结果
├── spark/
│   ├── src/pagerank_spark.py     # PySpark 实现（RDD 编程接口）
│   ├── scripts/run.sh            # local[2] 或 yarn 模式启动
│   └── output/pagerank-result*.txt
└── tools/
    ├── reference_pagerank.py     # 单机版参考实现（生成 expected-output.txt）
    └── compare.py                # 与参考结果逐网页比对误差
```

## 运行环境

| 组件 | 版本 |
| --- | --- |
| 操作系统 | Ubuntu 22.04.5 LTS（VirtualBox 虚拟机，6 vCPU / 3.8 GiB 内存 / 63 GB 磁盘） |
| JDK | OpenJDK 1.8.0_504（`/usr/lib/jvm/java-8-openjdk-amd64`） |
| Hadoop | 3.3.6，伪分布式（HDFS + YARN，`hdfs://localhost:9000`） |
| HBase | 2.4.18，分布式模式，ZooKeeper 由 HBase 自带（`HBASE_MANAGES_ZK=true`） |
| Spark | 3.3.2（Scala 2.12.15），`--master local[2]` 或 YARN |
| Python | 3.10.12（PySpark 运行环境） |

> 安装位置：`/usr/local/hadoop-3.3.6`、`/usr/local/hbase-2.4.18`、`/usr/local/spark-3.3.2-bin-hadoop3`，
> 软链接为 `/usr/local/{hadoop,hbase,spark}`；环境变量集中在 `/etc/profile.d/bigdata.sh`，新开终端后自动生效。

## 快速开始

```bash
# 0) 启动服务（HBase 与 YARN 内存占用较大，本机 3.8 GiB 内存下建议按需启动）
start-dfs.sh
start-hbase.sh        # 第一部分使用
# start-yarn.sh       # MapReduce / Spark on YARN 使用

# 1) HBase Shell：5 项操作
cd hbase && bash scripts/run-shell.sh          # 输出保存在 hbase/output/shell-output.txt

# 2) HBase Java API：编译并运行（会创建 Student/Course/SC 三张表并演示 5 个函数）
bash scripts/build.sh
bash scripts/run-java.sh                       # 修改成绩时从标准输入读取新值 95

# 3) PageRank（MapReduce）
cd ../mapreduce && bash scripts/build.sh
hdfs dfs -mkdir -p /lab3/input && hdfs dfs -put -f ../data/links.txt /lab3/input/
hadoop jar lab3-pagerank-mr.jar PageRankDriver /lab3/input/links.txt /lab3/state /lab3/output-mr 10
hdfs dfs -cat /lab3/output-mr/pagerank-result.txt

# 4) PageRank（Spark）
cd ../spark && bash scripts/run.sh local[2]     # 或 bash scripts/run.sh yarn
cat output/pagerank-result.txt

# 5) 与参考结果比对
python3 ../tools/compare.py ../data/expected-output.txt ../mapreduce/output/pagerank-result.txt \
    output/pagerank-result.txt
```

## 数据集与预期结果

`data/links.txt` 以“大数据技术栈”为主题构造了 15 个网页（Hadoop、HDFS、MapReduce、YARN、HBase、
ZooKeeper、Hive、Pig、Spark、RDD、PageRank、HCatalog、Oozie、Flume、Sqoop），每个网页的出链写在
同一行的制表符之后，用英文逗号分隔。该图是**闭合图**（每个出链目标都在图中作为源出现，没有悬挂节点），
因此每一轮迭代后 PR 值之和恒为 1.0。

`data/expected-output.txt` 由 `tools/reference_pagerank.py`（纯 Python，d=0.85，迭代 10 轮，
处理悬挂节点质量）独立计算，用于校验分布式实现的正确性：

```
(HDFS,0.1552132651)      (Hadoop,0.1520604520)   (MapReduce,0.1301747961)
(YARN,0.1054100926)      (Spark,0.0907506614)    (Hive,0.0868408003)
(HBase,0.0841672225)     (ZooKeeper,0.0334370196) (Pig,0.0318024519)
(PageRank,0.0254419982)  (RDD,0.0254419982)      (HCatalog,0.0247778680)
(Flume,0.0194766448)     (Oozie,0.0194766448)    (Sqoop,0.0155280843)
```

MapReduce、Spark（local[2] 与 YARN 两种模式）三种运行方式的结果与参考实现**逐网页一致，最大绝对误差 0.000e+00**。

## 设计说明

* **HBase 行键设计**：题目数据中 2015003 同时对应 Mary 与 Lisi，若用学号做 Student 表行键会互相覆盖；
  SC 表又以学生姓名关联，因此 Student 与 SC 的行键统一取姓名，学号作为普通列（Info:S_No）保存；
  Course 表行键取课程号 C_No。
* **HBase 与 YARN 不同时常驻**：本机只有 3.8 GiB 内存，PageRank 在 YARN 上运行时先 `stop-hbase.sh`，
  避免 RegionServer 与 NodeManager 争抢内存。
* **PageRank 每轮一个 MapReduce 作业**：Mapper 把 `PR/出链数` 作为投票发给每条出链，同时把网页自身的
  出链集合以 `#` 前缀带到 Reducer；Reducer 按 `PR=(1-d)/N+d*(Σ投票+悬挂质量/N)` 重新计算并保留图结构。
  悬挂节点质量通过计数器传给下一轮（本数据集为闭合图，悬挂质量为 0）。
* **Spark 版使用 RDD 接口**：`links.join(ranks)` 得到每条出链的贡献，`reduceByKey` 汇总；
  用 `links.keys()` 补 0 保证没有任何入链的网页不会从 RDD 中消失。
