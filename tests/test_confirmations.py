import hashlib
import os
import tempfile
import threading
import unittest

from app import server, store
from app.render import render_artifact_bytes


class ConfirmationTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        self.conn = store.connect()
        store.init_db(self.conn)
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        os.environ.pop("DATA_DIR", None)

    def publish(self, export_id="E-1"):
        """Drive an export to PUBLISHED with the verified artifact digest."""
        row = store.get_export(self.conn, export_id)
        digest = hashlib.sha256(render_artifact_bytes(row)).hexdigest()
        self.assertTrue(
            store.cas_stage(self.conn, export_id, "PROCESSING", ("RECEIVED",)))
        self.assertTrue(
            store.mark_published(self.conn, export_id, digest, "/p/%s.json" % export_id,
                                 "test", "unit"))
        return digest

    def confirm(self, ack_receipt_id="rcpt-A", digest=None, export_id="E-1"):
        digest = digest if digest is not None else self.publish(export_id)
        return store.confirm_export(self.conn, export_id, ack_receipt_id, digest) + (digest,)


class ConfirmationAdjudicationTest(ConfirmationTestBase):
    def test_first_confirmation_is_written_once(self):
        status, outcome, payload = self.confirm()[:3]
        self.assertEqual(201, status)
        self.assertEqual("created", outcome)
        self.assertEqual("CONFIRMED", payload["confirmation_status"])
        self.assertFalse(payload["replay"])
        self.assertEqual("rcpt-A", payload["ack_receipt_id"])
        row = store.get_confirmation(self.conn, "E-1")
        self.assertIsNotNone(row)
        self.assertEqual(payload["confirmed_at"], row["confirmed_at"])
        events = [e["event"] for e in store.export_events(self.conn, "E-1")]
        self.assertIn("confirmed", events)

    def test_requires_published_export(self):
        status, outcome, payload = store.confirm_export(
            self.conn, "E-1", "rcpt-A", "a" * 64)
        self.assertEqual(409, status)
        self.assertEqual("not_published", outcome)
        self.assertEqual("RECEIVED", payload["stage"])
        self.assertIsNone(store.get_confirmation(self.conn, "E-1"))
        # frozen evidence is untouched
        self.assertEqual("RECEIVED", store.get_export(self.conn, "E-1")["stage"])

    def test_unknown_export_is_404(self):
        status, outcome, _ = store.confirm_export(self.conn, "NOPE", "r", "a" * 64)
        self.assertEqual(404, status)
        self.assertEqual("not_found", outcome)

    def test_digest_mismatch_is_rejected(self):
        digest = self.publish()
        status, outcome, payload = store.confirm_export(
            self.conn, "E-1", "rcpt-A", "b" * 64)
        self.assertEqual(409, status)
        self.assertEqual("digest_mismatch", outcome)
        self.assertEqual(digest, payload["verified"]["artifact_digest"])
        self.assertIsNone(store.get_confirmation(self.conn, "E-1"))
        events = [e["event"] for e in store.export_events(self.conn, "E-1")]
        self.assertIn("confirmation_rejected", events)

    def test_same_receipt_and_digest_retransmit_returns_first_result(self):
        _, _, first = self.confirm()[:3]
        status, outcome, replay = store.confirm_export(
            self.conn, "E-1", "rcpt-A", first["ack_digest"])
        self.assertEqual(200, status)
        self.assertEqual("replay", outcome)
        self.assertTrue(replay["replay"])
        self.assertEqual(first["ack_receipt_id"], replay["ack_receipt_id"])
        self.assertEqual(first["confirmed_at"], replay["confirmed_at"])
        # still exactly one confirmation row
        self.assertEqual(["E-1"], list(store.confirmations_map(self.conn, ["E-1"])))

    def test_different_receipt_after_confirmation_conflicts_and_preserves(self):
        _, _, first = self.confirm()[:3]
        status, outcome, payload = store.confirm_export(
            self.conn, "E-1", "rcpt-B", first["ack_digest"])
        self.assertEqual(409, status)
        self.assertEqual("already_confirmed", outcome)
        self.assertEqual("confirmation_conflict", payload["error"])
        self.assertEqual("rcpt-A", payload["existing"]["ack_receipt_id"])
        row = store.get_confirmation(self.conn, "E-1")
        self.assertEqual("rcpt-A", row["ack_receipt_id"])
        self.assertEqual(first["confirmed_at"], row["confirmed_at"])

    def test_different_digest_after_confirmation_conflicts(self):
        _, _, first = self.confirm()[:3]
        status, _, payload = store.confirm_export(
            self.conn, "E-1", "rcpt-A", "c" * 64)
        self.assertEqual(409, status)
        self.assertEqual(first["ack_digest"], payload["existing"]["ack_digest"])
        self.assertEqual(
            first["ack_digest"], store.get_confirmation(self.conn, "E-1")["ack_digest"])

    def test_confirmation_never_rewrites_published_evidence(self):
        digest = self.publish()
        before = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", before["stage"])
        store.confirm_export(self.conn, "E-1", "rcpt-A", digest)
        # conflicting attempts must not change anything either
        store.confirm_export(self.conn, "E-1", "rcpt-B", digest)
        store.confirm_export(self.conn, "E-1", "rcpt-A", "d" * 64)
        after = store.get_export(self.conn, "E-1")
        for field in ("stage", "artifact_digest", "artifact_path", "published_at",
                      "receipt_id", "decision_hash"):
            self.assertEqual(before[field], after[field], field)


class ConfirmationConcurrencyTest(ConfirmationTestBase):
    def _race(self, receipt_for):
        """n threads adjudicate concurrently against one fresh published export."""
        n = 6
        store.submit_export(self.conn, "E-R", [{"ts": "t0", "lat": 1}])
        digest = self.publish("E-R")
        results = []
        barrier = threading.Barrier(n)

        def worker(i):
            conn = store.connect()
            try:
                barrier.wait()
                results.append(store.confirm_export(conn, "E-R", receipt_for(i), digest))
            finally:
                conn.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    def test_parallel_identical_requests_create_one_confirmation(self):
        results = self._race(lambda i: "rcpt-X")
        statuses = sorted(status for status, _, _ in results)
        self.assertEqual([200] * 5 + [201], statuses)
        confirmed_at = {p["confirmed_at"] for _, _, p in results}
        self.assertEqual(1, len(confirmed_at))
        self.assertEqual(1, len(store.confirmations_map(self.conn, ["E-R"])))

    def test_parallel_different_receipts_leave_one_winner(self):
        results = self._race(lambda i: "rcpt-%d" % i)
        created = [p for status, _, p in results if status == 201]
        rejected = [status for status, _, _ in results if status != 201]
        self.assertEqual(1, len(created))
        self.assertEqual([409] * 5, sorted(rejected))
        row = store.get_confirmation(self.conn, "E-R")
        self.assertEqual(created[0]["ack_receipt_id"], row["ack_receipt_id"])
        self.assertEqual(created[0]["confirmed_at"], row["confirmed_at"])


class ConfirmationPersistenceTest(ConfirmationTestBase):
    def test_fresh_connection_reads_the_same_confirmation(self):
        _, _, first = self.confirm()[:3]
        self.conn.close()
        reopened = store.connect()
        try:
            row = store.get_confirmation(reopened, "E-1")
        finally:
            reopened.close()
        self.assertEqual("rcpt-A", row["ack_receipt_id"])
        self.assertEqual(first["confirmed_at"], row["confirmed_at"])
        self.assertEqual(first["ack_digest"], row["ack_digest"])


class ConfirmationValidationTest(unittest.TestCase):
    def test_body_must_be_object(self):
        with self.assertRaises(server.ApiError) as ctx:
            server._validate_confirmation(["nope"])
        self.assertEqual(422, ctx.exception.status)

    def test_receipt_format(self):
        with self.assertRaises(server.ApiError):
            server._validate_confirmation({"ack_receipt_id": "", "ack_digest": "a" * 64})
        with self.assertRaises(server.ApiError):
            server._validate_confirmation({"ack_receipt_id": "bad id!", "ack_digest": "a" * 64})

    def test_digest_must_be_sha256_hex(self):
        with self.assertRaises(server.ApiError):
            server._validate_confirmation({"ack_receipt_id": "r", "ack_digest": "ABC"})
        with self.assertRaises(server.ApiError):
            server._validate_confirmation(
                {"ack_receipt_id": "r", "ack_digest": "A" * 64})  # uppercase rejected

    def test_valid_payload(self):
        receipt, digest = server._validate_confirmation(
            {"ack_receipt_id": "partner.rcpt-1:2", "ack_digest": "a" * 64})
        self.assertEqual("partner.rcpt-1:2", receipt)
        self.assertEqual("a" * 64, digest)


if __name__ == "__main__":
    unittest.main()
