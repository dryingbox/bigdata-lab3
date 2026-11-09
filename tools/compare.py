#!/usr/bin/env python3
"""比较两份 PageRank 输出：解析 (网页,PR值) 行，报告缺失/多余网页与最大绝对误差。

用法: compare.py 参考文件 结果文件 [结果文件...]
"""
import re
import sys

LINE = re.compile(r"^\((?P<page>[^,]+),(?P<pr>-?[0-9.]+)\)$")


def load(path):
    data = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = LINE.match(line.strip())
            if m:
                data[m.group("page")] = float(m.group("pr"))
    return data


def main():
    ref_path = sys.argv[1]
    ref = load(ref_path)
    print("参考结果: %s（%d 个网页，PR 总和 %.10f）" % (ref_path, len(ref), sum(ref.values())))
    ok = True
    for path in sys.argv[2:]:
        got = load(path)
        missing = sorted(set(ref) - set(got))
        extra = sorted(set(got) - set(ref))
        diffs = {p: abs(ref[p] - got[p]) for p in ref if p in got}
        max_diff = max(diffs.values()) if diffs else float("nan")
        print("\n结果文件: %s（%d 个网页，PR 总和 %.10f）"
              % (path, len(got), sum(got.values())))
        print("  缺失网页: %s" % (missing or "无"))
        print("  多余网页: %s" % (extra or "无"))
        print("  最大绝对误差: %.3e" % max_diff)
        bad = [p for p, d in diffs.items() if d > 5e-10]
        if missing or extra or bad:
            ok = False
            print("  不满足 1e-9 精度要求的网页: %s" % (bad or "无"))
        else:
            print("  通过：与参考结果一致（误差 < 5e-10）")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
