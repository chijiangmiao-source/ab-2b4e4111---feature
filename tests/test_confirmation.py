import hashlib
import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from app import artifacts, config, store, worker
from app.server import Handler


CONFIRM_SQL = "SELECT receipt_id, artifact_digest, confirmed_at FROM confirmations WHERE export_id = ?"


class ConfirmationStoreBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        os.environ["LEASE_TTL_SECONDS"] = "5"
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        for var in ("DATA_DIR", "LEASE_TTL_SECONDS"):
            os.environ.pop(var, None)

    def submit(self, export_id="E-1", records=None):
        records = records if records is not None else [{"ts": "t0", "lat": 31.2, "depth_m": 10}]
        return store.submit_export(self.conn, export_id, records)

    def publish(self, export_id="E-1"):
        """Drive the real pipeline so the verified published digest exists."""
        fencing = store.acquire_lease(self.conn, worker.lease_resource(export_id), "w-test", 5)
        self.assertEqual("published", worker.process_export(self.conn, export_id, "w-test", fencing))
        return store.get_export(self.conn, export_id)["artifact_digest"]

    def confirmation_rows(self, export_id="E-1"):
        return self.conn.execute(CONFIRM_SQL, (export_id,)).fetchall()


class ConfirmationAdjudicationTest(ConfirmationStoreBase):
    def test_unknown_export_is_404(self):
        status, payload = store.confirm_receipt(self.conn, "NOPE", "ack-1", "a" * 64)
        self.assertEqual(404, status)
        self.assertEqual("not_found", payload["error"])
        self.assertEqual([], self.conn.execute("SELECT * FROM confirmations").fetchall())

    def test_unpublished_export_is_rejected(self):
        self.submit()
        digest = hashlib.sha256(b"x").hexdigest()
        status, payload = store.confirm_receipt(self.conn, "E-1", "ack-1", digest)
        self.assertEqual(409, status)
        self.assertEqual("not_published", payload["error"])
        self.assertIsNone(store.get_confirmation(self.conn, "E-1"))

    def test_wrong_digest_on_published_export_is_conflict(self):
        self.submit()
        real_digest = self.publish()
        wrong_digest = ("0" if real_digest[0] != "0" else "1") + real_digest[1:]

        status, payload = store.confirm_receipt(self.conn, "E-1", "ack-1", wrong_digest)
        self.assertEqual(409, status)
        self.assertEqual("digest_mismatch", payload["error"])
        self.assertEqual(real_digest, payload["expected"]["artifact_digest"])
        self.assertIsNone(store.get_confirmation(self.conn, "E-1"))
        events = [e["event"] for e in store.export_events(self.conn, "E-1")]
        self.assertIn("confirm_rejected", events)

    def test_first_confirmation_with_exact_digest_is_written(self):
        self.submit()
        digest = self.publish()
        status, payload = store.confirm_receipt(self.conn, "E-1", "ack-stable-1", digest)
        self.assertEqual(201, status)
        self.assertFalse(payload["replay"])
        self.assertEqual("CONFIRMED", payload["status"])
        self.assertEqual(digest, payload["artifact_digest"])
        self.assertTrue(payload["confirmed_at"])

        saved = store.get_confirmation(self.conn, "E-1")
        self.assertEqual("ack-stable-1", saved["receipt_id"])
        self.assertEqual(digest, saved["artifact_digest"])
        self.assertEqual(payload["confirmed_at"], saved["confirmed_at"])
        events = [e["event"] for e in store.export_events(self.conn, "E-1")]
        self.assertIn("confirmed", events)

    def test_identical_retransmission_returns_first_confirmation(self):
        self.submit()
        digest = self.publish()
        _, first = store.confirm_receipt(self.conn, "E-1", "ack-1", digest)
        status, second = store.confirm_receipt(self.conn, "E-1", "ack-1", digest)
        self.assertEqual(200, status)
        self.assertTrue(second["replay"])
        self.assertEqual(first["receipt_id"], second["receipt_id"])
        self.assertEqual(first["confirmed_at"], second["confirmed_at"])
        self.assertEqual(first["artifact_digest"], second["artifact_digest"])
        rows = self.confirmation_rows()
        self.assertEqual(1, len(rows))

    def test_different_receipt_after_confirmation_conflicts_and_keeps_original(self):
        self.submit()
        digest = self.publish()
        store.confirm_receipt(self.conn, "E-1", "ack-1", digest)
        status, payload = store.confirm_receipt(self.conn, "E-1", "ack-2", digest)
        self.assertEqual(409, status)
        self.assertEqual("conflict", payload["error"])
        self.assertEqual("ack-1", payload["existing"]["receipt_id"])

        saved = store.get_confirmation(self.conn, "E-1")
        self.assertEqual("ack-1", saved["receipt_id"])
        self.assertEqual(digest, saved["artifact_digest"])
        self.assertEqual(1, len(self.confirmation_rows()))
        events = [e["event"] for e in store.export_events(self.conn, "E-1")]
        self.assertIn("confirm_conflict", events)

    def test_different_digest_after_confirmation_conflicts_and_keeps_original(self):
        self.submit()
        digest = self.publish()
        store.confirm_receipt(self.conn, "E-1", "ack-1", digest)
        other = ("0" if digest[0] != "0" else "1") + digest[1:]
        status, payload = store.confirm_receipt(self.conn, "E-1", "ack-1", other)
        self.assertEqual(409, status)
        self.assertEqual("conflict", payload["error"])
        saved = store.get_confirmation(self.conn, "E-1")
        self.assertEqual(digest, saved["artifact_digest"])
        self.assertEqual(1, len(self.confirmation_rows()))

    def test_confirmation_never_rewrites_publish_evidence(self):
        self.submit()
        digest = self.publish()
        before = store.get_export(self.conn, "E-1")
        store.confirm_receipt(self.conn, "E-1", "ack-1", digest)
        # rejected attempts must not touch the evidence either
        store.confirm_receipt(self.conn, "E-1", "ack-2", digest)
        store.confirm_receipt(self.conn, "E-1", "ack-1", "0" * 64)
        after = store.get_export(self.conn, "E-1")
        for field in ("stage", "artifact_digest", "artifact_path", "published_at",
                      "receipt_id", "decision_hash", "rules_snapshot", "records"):
            self.assertEqual(before[field], after[field], field)
        self.assertEqual("PUBLISHED", after["stage"])
        # the downloadable bytes stay byte-identical
        self.assertEqual(digest, hashlib.sha256(artifacts.load_verified(after)).hexdigest())

    def test_confirmation_persists_across_connections(self):
        self.submit()
        digest = self.publish()
        _, payload = store.confirm_receipt(self.conn, "E-1", "ack-1", digest)
        self.conn.close()
        reopened = store.connect()
        try:
            saved = store.get_confirmation(reopened, "E-1")
            self.assertEqual("ack-1", saved["receipt_id"])
            self.assertEqual(digest, saved["artifact_digest"])
            self.assertEqual(payload["confirmed_at"], saved["confirmed_at"])
            self.assertEqual(1, len(store.list_confirmations(reopened)))
        finally:
            reopened.close()
        # reopen the original connection for tearDown
        self.conn = store.connect()

    def test_concurrent_divergent_confirmations_create_one_row(self):
        self.submit()
        digest = self.publish()
        results = []

        def attempt(receipt_id):
            conn = store.connect()
            try:
                results.append(store.confirm_receipt(conn, "E-1", receipt_id, digest))
            finally:
                conn.close()

        threads = [threading.Thread(target=attempt, args=(rid,))
                   for rid in ("ack-A", "ack-B", "ack-C")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        statuses = sorted(status for status, _ in results)
        self.assertEqual([201, 409, 409], statuses)
        rows = self.confirmation_rows()
        self.assertEqual(1, len(rows))
        self.assertIn(rows[0]["receipt_id"], ("ack-A", "ack-B", "ack-C"))

    def test_concurrent_identical_confirmations_share_the_first_result(self):
        self.submit()
        digest = self.publish()
        results = []

        def attempt():
            conn = store.connect()
            try:
                results.append(store.confirm_receipt(conn, "E-1", "ack-1", digest))
            finally:
                conn.close()

        threads = [threading.Thread(target=attempt) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        statuses = sorted(status for status, _ in results)
        self.assertEqual([200, 200, 200, 200, 201], statuses)
        confirmed_at = {payload["confirmed_at"] for _, payload in results}
        self.assertEqual(1, len(confirmed_at))
        rows = self.confirmation_rows()
        self.assertEqual(1, len(rows))
        self.assertEqual(1, len(store.list_confirmations(self.conn)))


class ConfirmationHttpTest(ConfirmationStoreBase):
    def setUp(self):
        super().setUp()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base = "http://127.0.0.1:%d" % self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        super().tearDown()

    def req(self, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Content-Type": "application/json"} if data is not None else {}
        request = urllib.request.Request(self.base + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_confirmation_flow_over_http(self):
        recs = [{"ts": "t0", "lat": 31.2, "depth_m": 10}]
        status, _ = self.req("POST", "/api/exports", {"export_id": "E-1", "records": recs})
        self.assertEqual(201, status)

        status, body = self.req("POST", "/api/exports/E-1/confirmation",
                                {"receipt_id": "ack-1", "artifact_digest": "0" * 64})
        self.assertEqual(409, status)
        self.assertEqual("not_published", body["error"])

        status, body = self.req("POST", "/api/exports/E-1/confirmation",
                                {"receipt_id": "bad receipt!", "artifact_digest": "0" * 64})
        self.assertEqual(422, status)
        status, body = self.req("POST", "/api/exports/E-1/confirmation",
                                {"receipt_id": "ack-1", "artifact_digest": "nope"})
        self.assertEqual(422, status)
        status, body = self.req("POST", "/api/exports/NOPE/confirmation",
                                {"receipt_id": "ack-1", "artifact_digest": "0" * 64})
        self.assertEqual(404, status)

        digest = self.publish("E-1")

        status, body = self.req("POST", "/api/exports/E-1/confirmation",
                                {"receipt_id": "ack-1", "artifact_digest": "f" * 64})
        self.assertEqual(409, status)
        self.assertEqual("digest_mismatch", body["error"])

        status, body = self.req("POST", "/api/exports/E-1/confirmation",
                                {"receipt_id": "ack-1", "artifact_digest": digest})
        self.assertEqual(201, status)
        confirmed_at = body["confirmed_at"]

        status, body = self.req("POST", "/api/exports/E-1/confirmation",
                                {"receipt_id": "ack-1", "artifact_digest": digest})
        self.assertEqual(200, status)
        self.assertTrue(body["replay"])

        status, detail = self.req("GET", "/api/exports/E-1")
        self.assertEqual(200, status)
        self.assertEqual("CONFIRMED", detail["confirmation"]["status"])
        self.assertEqual("ack-1", detail["confirmation"]["receipt_id"])
        self.assertEqual(digest, detail["confirmation"]["artifact_digest"])
        self.assertEqual(confirmed_at, detail["confirmation"]["confirmed_at"])
        self.assertEqual("PUBLISHED", detail["stage"])

        status, listing = self.req("GET", "/api/exports")
        self.assertEqual(200, status)
        row = next(e for e in listing["exports"] if e["export_id"] == "E-1")
        self.assertEqual("CONFIRMED", row["confirmation"]["status"])

        status, body = self.req("POST", "/api/exports/E-1/confirmation",
                                {"receipt_id": "ack-2", "artifact_digest": digest})
        self.assertEqual(409, status)
        self.assertEqual("ack-1", body["existing"]["receipt_id"])

    def test_unconfirmed_view_over_http(self):
        self.submit("E-2")
        status, detail = self.req("GET", "/api/exports/E-2")
        self.assertEqual(200, status)
        self.assertEqual("UNCONFIRMED", detail["confirmation"]["status"])
        self.assertIsNone(detail["confirmation"]["receipt_id"])
        self.assertIsNone(detail["confirmation"]["confirmed_at"])


if __name__ == "__main__":
    unittest.main()
