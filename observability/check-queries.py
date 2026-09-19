#!/usr/bin/env python3
"""静态核对：面板 JSON 里用到的指标名/标签，是否真的出现在 /actuator/prometheus 的抓取文本里。
用法：check-queries.py <dashboard.json> <scrape.txt>
不需要 Prometheus 在跑，能在起栈之前就把拼错的指标名筛出来。
"""
import json
import re
import sys

METRIC_RE = re.compile(r"(?<![\w:])([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}")
BARE_RE = re.compile(r"(?<![\w:])(dianping_[a-z_]+|executor_[a-z_]+|http_server_requests_[a-z_]+)")


def load_series(path):
    series = {}
    with open(path) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s", line)
            if not m:
                continue
            name, labels = m.group(1), m.group(2) or ""
            parsed = dict(re.findall(r'(\w+)="([^"]*)"', labels))
            series.setdefault(name, []).append(parsed)
    return series


def main(dashboard_path, scrape_path):
    series = load_series(scrape_path)
    dash = json.load(open(dashboard_path))
    problems, checked = [], 0

    for panel in dash["panels"]:
        for target in panel.get("targets", []):
            expr = target["expr"]
            for name, label_str in METRIC_RE.findall(expr):
                checked += 1
                labels = dict(re.findall(r'(\w+)\s*=\s*"([^"]*)"', label_str))
                if name not in series:
                    problems.append((panel["title"], name, "指标不存在"))
                    continue
                for key, val in labels.items():
                    if key in ("le",):  # 直方图函数用，值由 bucket 提供
                        continue
                    op = re.search(r"%s\s*=~?" % key, label_str)
                    want = labels[key]
                    if op and "=~" in op.group(0):
                        rx = re.compile(want)
                        found = any(rx.search(s.get(key, "")) for s in series[name])
                    else:
                        found = any(s.get(key) == want for s in series[name])
                    if not found:
                        problems.append(
                            (panel["title"], "%s{%s=%r}" % (name, key, want), "无匹配序列"))
                if not any(set(labels) <= set(s) for s in series[name]):
                    problems.append((panel["title"], name, "标签集不存在"))
            for name in BARE_RE.findall(expr):
                checked += 1
                if name not in series:
                    problems.append((panel["title"], name, "指标不存在（懒注册或未埋点）"))

    print("checked selectors: %d" % checked)
    if not problems:
        print("OK：面板用到的指标名与标签都能在实测抓取里找到")
        return 0
    for title, what, why in problems:
        print("  [%s] %s -> %s" % (title, what, why))
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
