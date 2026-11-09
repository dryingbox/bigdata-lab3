#!/usr/bin/env python3
"""实验三 第二部分：用 Spark 实现 PageRank（RDD 编程接口）。

输入（HDFS 或本地文件，每行）：
    网页 \t 出链1,出链2,...

算法（与 MapReduce 版完全相同的公式）：
    PR(p) = (1-d)/N + d * ( Σ_{q→p} PR(q)/L(q) + D/N )
其中 d=0.85，N 为网页总数，D 为悬挂节点（无出链网页）PR 值之和。

输出（每行）：
    (网页,PR值)          PR 值保留 10 位小数，按 PR 从大到小排列

运行：
    spark-submit --master local[2] spark/src/pagerank_spark.py \
        lab3/data/links.txt spark/output/pagerank-result.txt [迭代次数]
"""
import sys

from pyspark import SparkContext

DAMPING = 0.85
ITERATIONS = 10


def parse_line(line):
    """一行文本 -> (网页, [出链列表])。"""
    line = line.strip()
    if not line:
        return None
    page, _, links = line.partition("\t")
    outs = [x.strip() for x in links.split(",") if x.strip()]
    return page, outs


def parse_state(line):
    """迭代状态行（网页 \t PR）-> (网页, float(PR))。"""
    page, _, rank = line.partition("\t")
    return page, float(rank)


def main():
    if len(sys.argv) < 3:
        print("用法: pagerank_spark.py <输入路径> <输出文件> [迭代次数]")
        sys.exit(1)
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    iterations = int(sys.argv[3]) if len(sys.argv) > 3 else ITERATIONS

    sc = SparkContext(appName="PageRank-Spark")

    # links: RDD[(网页, [出链列表])]；悬挂节点是出链为空的网页
    links = (sc.textFile(input_path)
             .map(parse_line)
             .filter(lambda x: x is not None)
             .cache())
    n = links.count()
    dangling_pages = set(links.filter(lambda kv: not kv[1]).keys().collect())

    # 初始 PR 值 = 1/N
    ranks = links.keys().map(lambda page: (page, 1.0 / n))

    for i in range(1, iterations + 1):
        dangling_mass = ranks.filter(lambda kv: kv[0] in dangling_pages) \
                             .map(lambda kv: kv[1]).sum()

        # 把 PR/出链数 作为投票发给每条出链
        contributions = links.join(ranks).flatMap(
            lambda kv: [(target, kv[1][1] / len(kv[1][0])) for target in kv[1][0]])

        # 为没有收到投票的网页补 0，保证所有网页保留在 RDD 中
        base = links.keys().map(lambda page: (page, 0.0))
        ranks = (base.union(contributions)
                 .reduceByKey(lambda a, b: a + b)
                 .mapValues(lambda s: (1.0 - DAMPING) / n
                            + DAMPING * (s + dangling_mass / n)))
        print("[PageRank-Spark] 第 %d 轮完成，悬挂质量=%.10f" % (i, dangling_mass))

    result = ranks.collect()
    result.sort(key=lambda kv: (-kv[1], kv[0]))
    lines = ["(%s,%.10f)" % (page, rank) for page, rank in result]

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("[PageRank-Spark] 结果已写入 %s，共 %d 个网页" % (output_path, len(lines)))
    for line in lines:
        print(line)

    sc.stop()


if __name__ == "__main__":
    main()
