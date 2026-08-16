import json
import unittest

from rivora import EventStream


class EventStreamTests(unittest.TestCase):
    def test_publish_assigns_monotonic_sequence(self):
        stream = EventStream()
        a = stream.publish("orders", {"id": 1})
        b = stream.publish("orders", {"id": 2})
        self.assertEqual((a.sequence, b.sequence), (1, 2))

    def test_duplicate_publish_is_idempotent(self):
        stream = EventStream()
        a = stream.publish("orders", {"id": 1}, client_key="x")
        b = stream.publish("orders", {"id": 1}, client_key="x")
        self.assertEqual(a, b)
        self.assertEqual(stream.stats()["events"], 1)

    def test_cursor_replay(self):
        stream = EventStream()
        stream.publish("a", {"n": 1})
        cursor = stream.cursor()
        stream.publish("a", {"n": 2})
        replay = stream.replay(after=cursor)
        self.assertEqual([e.payload["n"] for e in replay], [2])

    def test_topic_filter(self):
        stream = EventStream()
        stream.publish("orders", {"id": 1})
        stream.publish("inventory", {"id": 2})
        events = stream.replay(topics=["inventory"])
        self.assertEqual([e.topic for e in events], ["inventory"])

    def test_retention_is_bounded(self):
        stream = EventStream(retention=2)
        stream.publish("x", {"n": 1})
        stream.publish("x", {"n": 2})
        stream.publish("x", {"n": 3})
        self.assertEqual([e.payload["n"] for e in stream.replay()], [2, 3])

    def test_snapshot_is_deterministic_json(self):
        stream = EventStream()
        stream.publish("x", {"b": 2, "a": 1})
        a = stream.snapshot()
        b = stream.snapshot()
        self.assertEqual(a, b)
        self.assertIsInstance(json.loads(a), list)

    def test_invalid_arguments_rejected(self):
        with self.assertRaises(ValueError):
            EventStream(retention=0)
        stream = EventStream()
        with self.assertRaises(ValueError):
            stream.publish("", {})
        with self.assertRaises(ValueError):
            stream.replay(after=-1)
        with self.assertRaises(ValueError):
            stream.replay(limit=0)


if __name__ == "__main__":
    unittest.main()
