#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""校验 surefire 报告：测试发现范围与用例数是否与清单一致，失败是否为 0。

用法:
    python3 tools/check-test-reports.py --profile unit
    python3 tools/check-test-reports.py --profile integration [--reports target/surefire-reports]

规则:
  * 报告目录必须来自本次运行（跑之前 `mvn clean`，旧报告会被算进来）；
  * 发现的测试类集合必须与 tools/test-manifest.json 里该 profile 的类集合完全一致：
      - 少一个类：用例没被发现（provider 漂移、命名不匹配、profile 用错）；
      - 多一个类：别的档位的用例泄漏进了这一档（例如集成用例跑到 PR 门禁里）；
  * failures + errors 必须为 0；
  * unit 档不允许出现 skipped：单元用例被 skip 通常是凭据/环境判断写错，会伪装成绿灯；
  * 实际执行总数不得少于清单里的 min_tests。

退出码: 0 通过 / 1 不符合 / 2 用法或文件问题
"""

import argparse
import glob
import json
import os
import sys
import xml.etree.ElementTree as ET

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def fail(messages):
    for line in messages:
        sys.stderr.write("GATE FAIL: %s\n" % line)
    return 1


def read_reports(reports_dir):
    pattern = os.path.join(reports_dir, "TEST-*.xml")
    files = sorted(glob.glob(pattern))
    if not files:
        return None, ["%s 下没有找到 surefire XML 报告（测试根本没跑，不能记为通过）" % reports_dir]
    found = {}
    for path in files:
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError as e:
            return None, ["报告无法解析: %s (%s)" % (path, e)]
        name = root.get("name")
        if not name:
            return None, ["报告缺少 name 属性: %s" % path]

        def num(key):
            return int(root.get(key) or 0)

        entry = found.setdefault(name, dict(tests=0, failures=0, errors=0, skipped=0, files=[]))
        entry["tests"] += num("tests")
        entry["failures"] += num("failures")
        entry["errors"] += num("errors")
        entry["skipped"] += num("skipped")
        entry["files"].append(os.path.basename(path))
    return found, []


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, choices=["unit", "integration"])
    parser.add_argument("--reports", default=os.path.join(REPO_ROOT, "target", "surefire-reports"))
    parser.add_argument("--manifest", default=os.path.join(REPO_ROOT, "tools", "test-manifest.json"))
    args = parser.parse_args()

    try:
        with open(args.manifest, encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (IOError, OSError, ValueError) as e:
        sys.stderr.write("无法读取清单 %s: %s\n" % (args.manifest, e))
        return 2
    try:
        expected = manifest["profiles"][args.profile]
    except KeyError:
        sys.stderr.write("清单里没有 profile=%s\n" % args.profile)
        return 2

    expected_classes = set(expected["classes"])
    found, messages = read_reports(args.reports)
    if messages:
        return fail(messages)

    discovered = set(found)
    problems = []
    missing = sorted(expected_classes - discovered)
    extra = sorted(discovered - expected_classes)
    if missing:
        problems.append("以下用例未被发现: %s" % ", ".join(missing))
    if extra:
        problems.append("出现了不属于 %s 档的测试类: %s" % (args.profile, ", ".join(extra)))
        problems.append("如果是新增用例，请同步更新 tools/test-manifest.json")

    total = sum(entry["tests"] for entry in found.values())
    broken = sorted(name for name, entry in found.items() if entry["failures"] or entry["errors"])
    if broken:
        problems.append("存在失败/错误的测试类: %s" % ", ".join(broken))
    if total < expected["min_tests"]:
        problems.append("执行用例数 %d 少于清单下限 %d" % (total, expected["min_tests"]))
    if args.profile == "unit":
        skipped = sorted(name for name, entry in found.items() if entry["skipped"])
        if skipped:
            problems.append("unit 档出现被跳过的用例: %s（跳过不等于通过）" % ", ".join(skipped))

    print("profile      : %s" % args.profile)
    print("classes      : %d (manifest %d)" % (len(discovered), len(expected_classes)))
    for name in sorted(found):
        entry = found[name]
        print("  %-72s tests=%-3d failures=%d errors=%d skipped=%d"
              % (name, entry["tests"], entry["failures"], entry["errors"], entry["skipped"]))
    print("total tests  : %d (min %d)" % (total, expected["min_tests"]))

    if problems:
        return fail(problems)
    print("GATE OK: %s 档发现范围与用例数符合清单，无失败" % args.profile)
    return 0


if __name__ == "__main__":
    sys.exit(main())
