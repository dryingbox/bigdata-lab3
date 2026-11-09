#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PageRank 本地参考实现（不依赖 Hadoop / Spark）。

用途：
  1. 生成与 MapReduce、Spark 实现对照的 expected-output.txt；
  2. 校验两个分布式实现的结果格式与数值。

算法：PR(p) = (1-d)/N + d * ( sum_{q->p} PR(q)/L(q) + D/N )
其中 d 为阻尼系数（0.85），L(q) 为网页 q 的出链数，
D 为上轮所有“无出链（悬挂）网页”的 PR 之和。本数据集每个网页都有出链，D 恒为 0。

用法：python3 tools/reference_pagerank.py [links 文件] [迭代次数]
"""
import sys
from collections import defaultdict

DAMPING = 0.85
ITERATIONS = 10


def load(path):
    links = defaultdict(list)
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            page, _, out = line.partition("\t")
            links[page] = [x for x in out.split(",") if x]
    # 闭合性检查：每条出链的目标都必须是数据集中的网页
    missing = sorted({t for outs in links.values() for t in outs} - set(links))
    if missing:
        raise SystemExit("数据集不闭合，缺少网页定义: " + ", ".join(missing))
    return links


def pagerank(links, iterations=ITERATIONS, damping=DAMPING):
    n = len(links)
    pr = {p: 1.0 / n for p in links}
    for _ in range(iterations):
        dangling = sum(pr[p] for p in links if not links[p])
        contrib = defaultdict(float)
        for p, outs in links.items():
            if not outs:
                continue
            share = pr[p] / len(outs)
            for t in outs:
                contrib[t] += share
        pr = {p: (1.0 - damping) / n + damping * (contrib[p] + dangling / n) for p in links}
    return pr


def format_result(pr):
    return "".join("(%s,%.10f)\n" % (p, v) for p, v in sorted(pr.items(), key=lambda kv: (-kv[1], kv[0])))


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "data/links.txt"
    iters = int(sys.argv[2]) if len(sys.argv) > 2 else ITERATIONS
    links = load(path)
    pr = pagerank(links, iters)
    out = format_result(pr)
    sys.stdout.write(out)
    with open("data/expected-output.txt", "w", encoding="utf-8") as f:
        f.write(out)
    print("\n网页数: %d  迭代次数: %d  总和: %.12f" % (len(links), iters, sum(pr.values())), file=sys.stderr)
