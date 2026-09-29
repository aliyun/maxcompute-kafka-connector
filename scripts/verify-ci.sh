#!/usr/bin/env bash
# 本地等价跑法：与 .github/workflows/ci.yml（unit）/ integration-tests.yml（integration）一致的步骤。
#
# 用法:
#   scripts/verify-ci.sh              # = unit，PR 门禁的本地等价
#   scripts/verify-ci.sh unit         # 无需云凭据的单元回归 + 报告校验
#   scripts/verify-ci.sh integration  # 真实 Kafka + MaxCompute 集成回归（需要 4 个环境变量）
#   scripts/verify-ci.sh drill        # 自检：故意失败/凭据缺失/用例泄漏是否都能阻断
#
# 说明: 每档都先 clean，避免上一次运行的 surefire 报告被算进本次发现范围。
set -uo pipefail

cd "$(dirname "$0")/.."
MODE="${1:-unit}"
MVN_ARGS=(-B -ntp)

unit() {
  echo "== [unit] mvn clean test（IntegrationTest 分类被排除，不读任何云凭据）"
  env -u ALIBABA_CLOUD_ACCESS_KEY_ID -u ALIBABA_CLOUD_ACCESS_KEY_SECRET \
      -u odps_endpoint -u MAXCOMPUTE_PROJECT \
      mvn "${MVN_ARGS[@]}" clean test || return 1
  echo "== [unit] 校验发现范围与用例数"
  python3 tools/check-test-reports.py --profile unit
}

integration() {
  echo "== [integration] mvn clean test -Pintegration-tests（需要真实云凭据）"
  mvn "${MVN_ARGS[@]}" clean test -Pintegration-tests || return 1
  echo "== [integration] 校验发现范围"
  python3 tools/check-test-reports.py --profile integration
}

drill_case() {
  local desc="$1"; shift
  local expect="$1"; shift   # fail = 必须非 0 退出
  echo "-- drill: ${desc}"
  local case_log
  case_log="$(mktemp)"
  local rc=0
  "$@" >"$case_log" 2>&1 || rc=1
  if { [ "$expect" = "fail" ] && [ "$rc" = "0" ]; } || { [ "$expect" = "pass" ] && [ "$rc" = "1" ]; }; then
    echo "   结果不符合预期（期望 ${expect}，实际退出码 $rc），最后 15 行："
    tail -15 "$case_log" | sed 's/^/     /'
    rm -f "$case_log"
    return 1
  fi
  rm -f "$case_log"
  echo "   OK（按预期 ${expect}）"
  return 0
}

drill() {
  local drill_src="src/test/java/com/aliyun/odps/kafka/connect/GateDrillFailingTest.java"
  local rc=0
  if [ -e "$drill_src" ]; then
    echo "发现上次演练残留的文件，请先删除: $drill_src"
    return 1
  fi
  cat > "$drill_src" <<'JAVA'
package com.aliyun.odps.kafka.connect;

import org.junit.Assert;
import org.junit.Test;

/** verify-ci.sh drill 注入的临时用例，运行结束后会被删除。 */
public class GateDrillFailingTest {

  @Test
  public void deliberatelyFails() {
    Assert.fail("门禁自检：这条用例必须让构建失败");
  }
}
JAVA
  trap 'rm -f "'"$drill_src"'"' EXIT

  drill_case "故意失败的单元用例会阻断 mvn test" fail \
    mvn "${MVN_ARGS[@]}" clean test -Dtest=GateDrillFailingTest || rc=1
  drill_case "即使 maven 吞掉失败，报告校验器仍会阻断" fail \
    sh -c "mvn ${MVN_ARGS[*]} clean test -Dtest=GateDrillFailingTest -Dmaven.test.failure.ignore=true \
           && python3 tools/check-test-reports.py --profile unit" || rc=1
  drill_case "集成用例无法在 unit 档里被发现/执行" fail \
    mvn "${MVN_ARGS[@]}" test -Dtest=TestCsvRecord || rc=1
  drill_case "缺少云凭据时集成档必须失败，而不是记为通过" fail \
    env -u ALIBABA_CLOUD_ACCESS_KEY_ID -u ALIBABA_CLOUD_ACCESS_KEY_SECRET \
        -u odps_endpoint -u MAXCOMPUTE_PROJECT \
        mvn "${MVN_ARGS[@]}" test -Pintegration-tests \
        -Dtest=TestMaxComputeSinkConnectorIntegration || rc=1
  # 演练文件必须在最后一步前删掉：它存在时 unit 档就该是红的（前 4 条已经证明过）
  rm -f "$drill_src"
  drill_case "删除演练文件后 unit 档恢复绿色（且清单校验通过）" pass \
    sh -c "mvn ${MVN_ARGS[*]} clean test && python3 tools/check-test-reports.py --profile unit" || rc=1

  trap - EXIT
  if [ "$rc" = "0" ]; then
    echo "== drill: 全部自检符合预期（门禁能阻断，不会假绿）"
  else
    echo "== drill: 有自检未达预期，门禁不可信，请先修门禁"
  fi
  return "$rc"
}

case "$MODE" in
  unit) unit ;;
  integration) integration ;;
  drill) drill ;;
  *) echo "unknown mode: $MODE (unit|integration|drill)" >&2; exit 2 ;;
esac
