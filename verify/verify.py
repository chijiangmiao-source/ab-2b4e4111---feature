"""One-shot acceptance service: build checks + unit tests + API/HTTP smoke.

Runs the build check (byte-compile) and the unit test suite, then exercises
the live HTTP API through the required scenarios:

  1. export keeps following the frozen rules snapshot after rules change
  2. crash recovery (converge a complete staged artifact; clean up a partial one)
  3. business-equivalent retransmission (first receipt, no second artifact)
     and conflict handling (different records or rules snapshot)
  4. receiver confirmations: independent UNCONFIRMED/CONFIRMED state, exactly
     one persistent confirmation per export, replay/conflict semantics,
     concurrent confirmations, and the same result after an app restart

Exits 0 when everything passes, 1 otherwise.
"""
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

API = os.environ.get("API_BASE", "http://localhost:8080").rstrip("/")
DATA_DIR = os.environ.get("DATA_DIR", "./data")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = uuid.uuid4().hex[:6]

FAILURES = []


def check(name, condition, detail=""):
    mark = "PASS" if condition else "FAIL"
    line = "[%s] %s" % (mark, name)
    if not condition and detail:
        line += " -- " + detail
    print(line, flush=True)
    if not condition:
        FAILURES.append(name)


def step(title):
    print("\n=== %s ===" % title, flush=True)


# ------------------------------------------------------------------ helpers

def req(method, path, body=None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(API + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers or {})


def as_json(raw):
    return json.loads(raw.decode("utf-8"))


def wait_for_stage(export_id, stage, timeout):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        status, raw, _ = req("GET", "/api/exports/" + export_id)
        if status == 200:
            last = as_json(raw)
            if last["stage"] == stage:
                return last
        time.sleep(1)
    return last


def published_files(export_id):
    directory = os.path.join(DATA_DIR, "artifacts", "published")
    if not os.path.isdir(directory):
        return []
    return [n for n in os.listdir(directory) if n == export_id + ".json"]


def tmp_files(export_id):
    directory = os.path.join(DATA_DIR, "artifacts", "tmp")
    if not os.path.isdir(directory):
        return []
    return [n for n in os.listdir(directory) if n.startswith(export_id + ".")]


def download(export_id):
    return req("GET", "/api/exports/%s/artifact" % export_id)


def confirm(export_id, receipt, digest):
    return req("POST", "/api/exports/%s/confirmations" % export_id,
               {"ack_receipt_id": receipt, "ack_digest": digest})


def parallel_calls(fn, n):
    """Run fn(i) for i in range(n) behind one barrier; collect (i, status, body, headers)."""
    results = []
    barrier = threading.Barrier(n)

    def work(i):
        barrier.wait()
        try:
            status, raw, headers = fn(i)
            results.append((i, status, as_json(raw), headers))
        except Exception as exc:  # a connection failure is a real failure signal
            results.append((i, 0, {"_exception": repr(exc)}, {}))

    threads = [threading.Thread(target=work, args=(i,)) for i in range(n)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return sorted(results, key=lambda item: item[0])


def get_export(export_id):
    status, raw, _ = req("GET", "/api/exports/" + export_id)
    return as_json(raw) if status == 200 else None


def restart_app_and_wait():
    status, _, _ = req("POST", "/api/test/restart")
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            code, raw, _ = req("GET", "/healthz")
            if code == 200 and as_json(raw).get("ok"):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


# ------------------------------------------------------------------ phases

def build_checks():
    step("构建检查：python -m compileall")
    proc = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "app", "verify", "tests"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if proc.stdout:
        print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    check("compileall app/verify/tests", proc.returncode == 0, proc.stderr.strip()[:400])


def unit_tests():
    step("代码测试：unittest discover")
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", ".", "-v"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    output = (proc.stdout + proc.stderr).strip()
    print("\n".join(output.splitlines()[-25:]))
    check("unit test suite", proc.returncode == 0, output[-400:])


def wait_for_api():
    step("等待 API 健康")
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            status, raw, _ = req("GET", "/healthz")
            if status == 200 and as_json(raw).get("ok"):
                check("health response", True)
                return
        except Exception:
            pass
        time.sleep(1)
    check("health response", False, "API did not become healthy in time")
    raise SystemExit(1)


def smoke():
    e1, e2, e3, e4, e5 = ("VFY%d-%s" % (i, RUN) for i in range(1, 6))
    recs = [
        {"ts": "2026-10-06T01:00:00Z", "lat": 31.230416, "lon": 121.473701, "depth_m": 42.51, "vessel_id": "HAICE-01"},
        {"ts": "2026-10-06T01:05:00Z", "lat": 31.231102, "lon": 121.480233, "depth_m": 43.04, "vessel_id": "HAICE-01"},
    ]
    rules_r1 = {"rules": [
        {"field": "depth_m", "action": "redact"},
        {"field": "vessel_id", "action": "hash", "length": 10},
    ]}
    rules_r2 = {"rules": [{"field": "lat", "action": "redact"}]}

    step("页面通过真实 API 轮询（页面可加载且引用 API）")
    status, raw, headers = req("GET", "/")
    check("operator page served", status == 200 and "text/html" in headers.get("Content-Type", ""))
    check("page polls real API", b"/api/exports" in raw and b"fetch(" in raw)

    step("设置规则 R1（遮蔽 depth_m / 散列 vessel_id）")
    status, raw, _ = req("PUT", "/api/rules", rules_r1)
    check("put rules R1", status == 200, "HTTP %s %s" % (status, raw[:200]))
    d1 = as_json(raw)["digest"]

    step("提交导出 E1 → 201 冻结裁决")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": recs})
    check("submit E1 -> 201", status == 201, "HTTP %s %s" % (status, raw[:300]))
    receipt1 = as_json(raw)
    check("E1 froze rules R1", receipt1.get("rules_digest") == d1)

    step("业务等价重传 E1（键序/空白不同）→ 200 首次回执")
    reordered = [dict(reversed(list(r.items()))) for r in recs]
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": reordered})
    replay = as_json(raw)
    check("replay E1 -> 200", status == 200, "HTTP %s %s" % (status, raw[:300]))
    check("replay returns first receipt",
          replay.get("receipt_id") == receipt1["receipt_id"]
          and replay.get("received_at") == receipt1["received_at"]
          and replay.get("replay") is True)

    step("等待 E1 发布并校验工件")
    detail = wait_for_stage(e1, "PUBLISHED", 90)
    check("E1 published", detail is not None and detail["stage"] == "PUBLISHED",
          "last=%s" % (detail and detail.get("stage")))
    a1 = detail["artifact_digest"] if detail else None
    status, raw, _ = download(e1)
    check("download E1 -> 200", status == 200, "HTTP %s" % status)
    if status == 200:
        check("E1 digest matches download", hashlib.sha256(raw).hexdigest() == a1)
        doc = as_json(raw)
        check("E1 masked per R1 (depth redacted, vessel hashed, lat kept)",
              doc["records"][0]["depth_m"] == "***"
              and doc["records"][0]["vessel_id"] != "HAICE-01"
              and doc["records"][0]["lat"] == 31.230416)
        check("E1 artifact carries frozen digests",
              doc["rules_digest"] == d1 and doc["input_digest"] == receipt1["input_digest"])
    check("exactly one published artifact file for E1", len(published_files(e1)) == 1,
          str(published_files(e1)))

    step("值班员改规则 → R2（遮蔽 lat）")
    status, raw, _ = req("PUT", "/api/rules", rules_r2)
    check("put rules R2", status == 200)
    d2 = as_json(raw)["digest"]
    check("R2 digest differs", d2 != d1)

    step("规则改动后重传 E1（同记录）→ 409 冲突（规则快照不同）")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": recs})
    conflict = as_json(raw)
    check("replay under changed rules -> 409", status == 409, "HTTP %s" % status)
    check("conflict preserves original evidence",
          conflict.get("existing", {}).get("rules_digest") == d1
          and conflict.get("existing", {}).get("receipt_id") == receipt1["receipt_id"])

    step("不同记录重传 E1 → 409 冲突（记录不同）")
    changed = [dict(r, depth_m=99.9) for r in recs]
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": changed})
    check("different records -> 409", status == 409, "HTTP %s" % status)

    detail = wait_for_stage(e1, "PUBLISHED", 5)
    check("E1 evidence untouched after conflicts",
          detail is not None and detail["artifact_digest"] == a1 and detail["stage"] == "PUBLISHED")

    step("规则改动后，E1 仍按冻结快照导出；新导出 E2 使用 R2")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e2, "records": recs})
    check("submit E2 -> 201", status == 201)
    check("E2 froze rules R2", as_json(raw).get("rules_digest") == d2)
    detail2 = wait_for_stage(e2, "PUBLISHED", 90)
    check("E2 published", detail2 is not None and detail2["stage"] == "PUBLISHED")
    status, raw, _ = download(e2)
    if status == 200:
        doc = as_json(raw)
        check("E2 masked per R2 (lat redacted, depth kept)",
              doc["records"][0]["lat"] == "***" and doc["records"][0]["depth_m"] == 42.51)
    else:
        check("download E2 -> 200", False, "HTTP %s" % status)
    status, raw, _ = download(e1)
    check("E1 re-download unchanged after rule change",
          status == 200 and hashlib.sha256(raw).hexdigest() == a1)
    if status == 200:
        doc = as_json(raw)
        check("E1 still follows frozen R1 (depth redacted, lat kept)",
              doc["records"][0]["depth_m"] == "***" and doc["records"][0]["lat"] == 31.230416)

    step("崩溃恢复 A：暂存完整工件后进程退出 → 收敛到同一完整工件")
    status, raw, _ = req("POST", "/api/test/fault", {"export_id": e3, "mode": "crash_after_staged"})
    check("arm fault crash_after_staged", status == 202, "HTTP %s %s" % (status, raw[:200]))
    status, raw, _ = req("POST", "/api/exports", {"export_id": e3, "records": recs})
    check("submit E3 -> 201", status == 201)
    detail3 = wait_for_stage(e3, "PUBLISHED", 150)
    check("E3 published after crash+recovery", detail3 is not None and detail3["stage"] == "PUBLISHED",
          "last=%s" % (detail3 and detail3.get("stage")))
    if detail3:
        events = detail3.get("events", [])
        recovered = any("recovery" in (e.get("detail") or "") or "recover" in e.get("event", "")
                        for e in events)
        check("E3 journal shows recovery convergence", recovered,
              "events=%s" % [e["event"] for e in events])
        status, raw, _ = download(e3)
        check("E3 download verified", status == 200
              and hashlib.sha256(raw).hexdigest() == detail3["artifact_digest"])
        check("exactly one published artifact file for E3", len(published_files(e3)) == 1)
        check("no temp leftovers for E3", tmp_files(e3) == [], str(tmp_files(e3)))

    step("崩溃恢复 B：临时工件写一半进程退出 → 清理残缺工件并重处理")
    status, raw, _ = req("POST", "/api/test/fault", {"export_id": e4, "mode": "crash_partial_write"})
    check("arm fault crash_partial_write", status == 202, "HTTP %s" % status)
    status, raw, _ = req("POST", "/api/exports", {"export_id": e4, "records": recs})
    check("submit E4 -> 201", status == 201)
    detail4 = wait_for_stage(e4, "PUBLISHED", 150)
    check("E4 published after cleanup+requeue", detail4 is not None and detail4["stage"] == "PUBLISHED",
          "last=%s" % (detail4 and detail4.get("stage")))
    if detail4:
        events = [e["event"] for e in detail4.get("events", [])]
        check("E4 journal shows cleanup/requeue",
              "recovery_cleanup" in events or "requeued" in events, "events=%s" % events)
        check("E4 needed a retry", detail4["attempts"] >= 1)
        check("no temp leftovers for E4", tmp_files(e4) == [], str(tmp_files(e4)))
        status, raw, _ = download(e4)
        check("E4 download verified", status == 200
              and hashlib.sha256(raw).hexdigest() == detail4["artifact_digest"])

    step("下载接口不暴露未核验内容（崩溃窗口内只能 409，不能 200）")
    req("POST", "/api/test/fault", {"export_id": e5, "mode": "crash_partial_write"})
    status, raw, _ = req("POST", "/api/exports", {"export_id": e5, "records": recs})
    check("submit E5 -> 201", status == 201)
    saw_409 = False
    exposed = False
    deadline = time.time() + 150
    while time.time() < deadline:
        code, body, _ = download(e5)
        d = wait_for_stage(e5, "PUBLISHED", 1)  # stage probe after the download
        if code == 200:
            # 200 is legitimate only when the export is PUBLISHED (stage never
            # regresses, so a PUBLISHED probe after the 200 is conclusive).
            if d and d["stage"] == "PUBLISHED":
                break
            exposed = True
            break
        if code == 409:
            saw_409 = True
        time.sleep(0.4)
    check("unpublished content never served", not exposed)
    check("download refused while unverified (409 observed)", saw_409)

    step("发布后的业务等价重传：仍返回首次回执且不产生第二个工件")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e2, "records": reordered})
    replay2 = as_json(raw)
    check("replay E2 -> 200 first receipt", status == 200 and replay2.get("replay") is True)
    detail2b = wait_for_stage(e2, "PUBLISHED", 5)
    check("E2 artifact digest unchanged by replay",
          detail2b is not None and detail2b["artifact_digest"] == detail2["artifact_digest"])
    check("exactly one published artifact file for E2", len(published_files(e2)) == 1)

    step("终态检查：无残缺临时工件残留")
    leftovers = []
    for eid in (e1, e2, e3, e4, e5):
        leftovers.extend(tmp_files(eid))
    check("no temp artifacts left behind", leftovers == [], str(leftovers))


def confirmations_smoke():
    c1, c2, c3 = ("CF%d-%s" % (i, RUN) for i in (1, 2, 3))
    recs = [
        {"ts": "2026-10-07T02:00:00Z", "lat": 31.23, "lon": 121.47, "depth_m": 12.3},
        {"ts": "2026-10-07T02:05:00Z", "lat": 31.24, "lon": 121.48, "depth_m": 12.9},
    ]

    step("提交 C1/C2/C3 并等待发布（确认流程前置）")
    digests = {}
    for eid in (c1, c2, c3):
        status, raw, _ = req("POST", "/api/exports", {"export_id": eid, "records": recs})
        check("submit %s -> 201" % eid, status == 201, "HTTP %s %s" % (status, raw[:200]))
        detail = wait_for_stage(eid, "PUBLISHED", 90)
        check("%s published" % eid, detail is not None and detail["stage"] == "PUBLISHED")
        if detail:
            digests[eid] = detail["artifact_digest"]

    step("确认状态独立展示：列表与详情均为 UNCONFIRMED，且回执/摘要/时间为空")
    status, raw, _ = req("GET", "/api/exports")
    listing = {e["export_id"]: e for e in as_json(raw)["exports"]}
    for eid in (c1, c2, c3):
        row = listing.get(eid, {})
        check("%s lists independent UNCONFIRMED state" % eid,
              row.get("confirmation_status") == "UNCONFIRMED"
              and row.get("ack_receipt_id") is None and row.get("ack_digest") is None
              and row.get("confirmed_at") is None
              and row.get("stage") == "PUBLISHED",
              str(row))
    detail = get_export(c1)
    check("C1 detail carries UNCONFIRMED fields",
          detail and detail["confirmation_status"] == "UNCONFIRMED"
          and detail["ack_receipt_id"] is None)

    step("未发布导出不能确认 → 409（故障注入使其在发布窗口外保持未发布）")
    early = c1 + "-X"
    req("POST", "/api/test/fault", {"export_id": early, "mode": "crash_partial_write"})
    status, raw, _ = req("POST", "/api/exports", {"export_id": early, "records": recs})
    check("submit %s -> 201" % early, status == 201)
    status, raw, _ = confirm(early, "rcpt-early", "0" * 64)
    check("confirm unpublished -> 409", status == 409, "HTTP %s %s" % (status, raw[:200]))
    check("unpublished rejection preserves no confirmation",
          as_json(raw).get("error") == "confirmation_not_published"
          and get_export(early)["confirmation_status"] == "UNCONFIRMED")

    step("摘要不符不能确认 → 409，且不写入确认")
    wrong = ("1" if digests[c1][0] != "1" else "2") * 64
    status, raw, _ = confirm(c1, "rcpt-C1", wrong)
    body = as_json(raw)
    check("confirm C1 with wrong digest -> 409", status == 409, "HTTP %s" % status)
    check("digest mismatch error + verified digest echoed",
          body.get("error") == "confirmation_digest_mismatch"
          and body.get("verified", {}).get("artifact_digest") == digests[c1])
    check("C1 still UNCONFIRMED after digest mismatch",
          get_export(c1)["confirmation_status"] == "UNCONFIRMED")

    step("C1 首次确认（摘要与已核验工件一致）→ 201，只写一次")
    status, raw, headers = confirm(c1, "rcpt-C1", digests[c1])
    first = as_json(raw)
    check("confirm C1 -> 201", status == 201, "HTTP %s %s" % (status, raw[:300]))
    check("first confirmation echoed + replay header false",
          status == 201 and first.get("replay") is False
          and first.get("confirmation_status") == "CONFIRMED"
          and first.get("ack_receipt_id") == "rcpt-C1"
          and first.get("ack_digest") == digests[c1]
          and headers.get("X-Confirmation-Replay") == "false")
    confirmed_at = first.get("confirmed_at")
    check("confirmation time recorded", isinstance(confirmed_at, str) and len(confirmed_at) >= 20)

    step("确认不改变下载内容、发布阶段与冻结证据")
    detail_before = get_export(c1)
    status, raw, _ = download(c1)
    check("C1 download after confirmation",
          status == 200 and hashlib.sha256(raw).hexdigest() == digests[c1])
    check("C1 stage/evidence unchanged",
          detail_before["stage"] == "PUBLISHED"
          and detail_before["artifact_digest"] == digests[c1]
          and detail_before["published_at"] is not None)

    step("相同导出 + 相同回执 + 相同摘要重传 → 200 返回首次确认结果")
    status, raw, headers = confirm(c1, "rcpt-C1", digests[c1])
    replay = as_json(raw)
    check("retransmit C1 -> 200 replay", status == 200 and replay.get("replay") is True
          and headers.get("X-Confirmation-Replay") == "true",
          "HTTP %s %s" % (status, raw[:200]))
    check("replay returns first confirmation",
          replay.get("ack_receipt_id") == "rcpt-C1"
          and replay.get("confirmed_at") == confirmed_at
          and replay.get("ack_digest") == digests[c1])

    step("已确认后不同回执标识 → 409，原确认保留")
    status, raw, _ = confirm(c1, "rcpt-OTHER", digests[c1])
    body = as_json(raw)
    check("different receipt -> 409 confirmation_conflict",
          status == 409 and body.get("error") == "confirmation_conflict",
          "HTTP %s %s" % (status, raw[:200]))
    check("original confirmation preserved after receipt conflict",
          body.get("existing", {}).get("ack_receipt_id") == "rcpt-C1"
          and body.get("existing", {}).get("confirmed_at") == confirmed_at
          and get_export(c1)["ack_receipt_id"] == "rcpt-C1")

    step("已确认后不同摘要 → 409，原确认保留")
    status, raw, _ = confirm(c1, "rcpt-C1", wrong)
    check("different digest after confirmation -> 409", status == 409)
    after = get_export(c1)
    check("C1 first confirmation intact",
          after["ack_receipt_id"] == "rcpt-C1" and after["ack_digest"] == digests[c1]
          and after["confirmed_at"] == confirmed_at)

    step("两个并发请求（相同回执+摘要）确认同一导出 → 仅一条确认（201 一次，其余 200）")
    results = parallel_calls(lambda i: confirm(c2, "rcpt-C2", digests[c2]), 6)
    statuses = sorted(r[1] for r in results)
    check("C2 race: one 201 and five 200", statuses == [200] * 5 + [201], str(statuses))
    winners = {r[2].get("confirmed_at") for r in results if r[1] == 201}
    check("C2 race: single confirmation identity", len(winners) == 1 and next(iter(winners)))
    detail2 = get_export(c2)
    check("C2 exactly one confirmation visible",
          detail2["ack_receipt_id"] == "rcpt-C2"
          and detail2["ack_digest"] == digests[c2]
          and detail2["confirmed_at"] == next(iter(winners)))

    step("并发但回执标识互异 → 只能形成一条确认，其余明确冲突")
    results = parallel_calls(lambda i: confirm(c3, "rcpt-C3-%d" % i, digests[c3]), 6)
    created = [r for r in results if r[1] == 201]
    conflicts = [r for r in results if r[1] != 201]
    check("C3 race: exactly one winner", len(created) == 1,
          str([(r[1], r[2].get("error")) for r in results]))
    check("C3 race: losers got explicit 409 conflict",
          len(conflicts) == 5
          and all(r[1] == 409 and r[2].get("error") == "confirmation_conflict" for r in conflicts))
    detail3 = get_export(c3)
    check("C3 winner visible and preserved",
          detail3["ack_receipt_id"] == created[0][2]["ack_receipt_id"]
          and detail3["confirmed_at"] == created[0][2]["confirmed_at"])

    step("服务重启后页面读取同一确认结果（持久化裁决，非内存状态）")
    check("restart hook responded", restart_app_and_wait() or False)
    detail1 = get_export(c1)
    detail2b = get_export(c2)
    detail3b = get_export(c3)
    check("C1 confirmation survives restart",
          detail1 and detail1["confirmation_status"] == "CONFIRMED"
          and detail1["ack_receipt_id"] == "rcpt-C1"
          and detail1["ack_digest"] == digests[c1]
          and detail1["confirmed_at"] == confirmed_at,
          str(detail1 and {k: detail1.get(k) for k in
                           ("confirmation_status", "ack_receipt_id", "confirmed_at")}))
    check("C2/C3 confirmations survive restart",
          detail2b["ack_digest"] == digests[c2] and detail2b["ack_receipt_id"] == "rcpt-C2"
          and detail3b["ack_digest"] == digests[c3]
          and detail3b["ack_receipt_id"].startswith("rcpt-C3-"))
    status, raw, _ = req("GET", "/api/exports")
    listing = {e["export_id"]: e for e in as_json(raw)["exports"]}
    check("list after restart shows CONFIRMED + full fields",
          all(listing[e]["confirmation_status"] == "CONFIRMED"
              and listing[e]["ack_receipt_id"] and listing[e]["ack_digest"]
              and listing[e]["confirmed_at"] for e in (c1, c2, c3)))
    status, raw, _ = confirm(c1, "rcpt-C1", digests[c1])
    check("post-restart retransmit still replays first result",
          status == 200 and as_json(raw).get("confirmed_at") == confirmed_at)

    step("原有下载回归：重启后全部已发布工件仍可下载且摘要核验通过")
    for eid, digest in digests.items():
        status, raw, _ = download(eid)
        check("download regression %s" % eid,
              status == 200 and hashlib.sha256(raw).hexdigest() == digest,
              "HTTP %s" % status)


def main():
    print("verify: one-shot acceptance run %s against %s" % (RUN, API), flush=True)
    build_checks()
    unit_tests()
    wait_for_api()
    smoke()
    confirmations_smoke()
    print("\n==============================================")
    if FAILURES:
        print("verify: FAILED (%d): %s" % (len(FAILURES), ", ".join(FAILURES)), flush=True)
        return 1
    print("verify: ALL CHECKS PASSED", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
