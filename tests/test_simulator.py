import unittest
from pathlib import Path

from simulator import Machine, parse_input


BENCHMARK_1 = """\
MEMORY: X=5
PROCESSORS: P, Q
P: Read(X)
Q: Read(X)
Q: Write(X, 10)
Q: Read(X)
Q: Write(X, 15)
P: Write(X, 20)
Q: Read(X)
"""


class ProtocolBenchmarkTests(unittest.TestCase):
    def simulate(self, protocol):
        machine = Machine(parse_input(f"PROTOCOL: {protocol}\n{BENCHMARK_1}"))
        machine.execute()
        return machine

    def test_write_through_benchmark1(self):
        machine = self.simulate("WRITE_INVALIDATE_WRITE_THROUGH")
        self.assertEqual(machine.memory["X"], 20)
        self.assertEqual(machine.caches["P"]["X"].state, "VALID")
        self.assertEqual(machine.caches["Q"]["X"].state, "VALID")
        self.assertEqual(machine.stats["invalidations"], 3)
        self.assertEqual(machine.stats["writebacks"], 0)

    def test_write_back_benchmark1(self):
        machine = self.simulate("WRITE_INVALIDATE_WRITE_BACK")
        self.assertEqual(machine.memory["X"], 20)
        self.assertEqual(machine.caches["P"]["X"].state, "RO")
        self.assertEqual(machine.caches["Q"]["X"].state, "RO")
        self.assertEqual(machine.caches["P"]["X"].value, 20)
        self.assertEqual(machine.stats["invalidations"], 2)
        self.assertEqual(machine.stats["writebacks"], 1)

    def test_write_once_benchmark1(self):
        machine = self.simulate("WRITE_ONCE")
        self.assertEqual(machine.memory["X"], 20)
        self.assertEqual(machine.caches["P"]["X"].state, "RES")
        self.assertEqual(machine.caches["Q"]["X"].state, "VALID")
        self.assertEqual(machine.stats["invalidations"], 2)
        self.assertEqual(machine.stats["writebacks"], 1)

    def test_write_back_replacement_writes_dirty_line(self):
        config = parse_input("""\
PROTOCOL: WRITE_INVALIDATE_WRITE_BACK
MEMORY: X=1
PROCESSORS: P1
P1: Write(X, 9)
P1: Replace(X)
""")
        machine = Machine(config)
        machine.execute()
        self.assertEqual(machine.memory["X"], 9)
        self.assertNotIn("X", machine.caches["P1"])
        self.assertEqual(machine.stats["writebacks"], 1)


class InputAndTraceTests(unittest.TestCase):
    def test_invalid_operation_reference_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "undeclared variable"):
            parse_input("""\
PROTOCOL: WRITE_ONCE
MEMORY: X=1
PROCESSORS: P1
P1: Read(Y)
""")

    def test_trace_contains_each_state_snapshot(self):
        config = parse_input("""\
PROTOCOL: WRITE_INVALIDATE_WRITE_THROUGH
MEMORY: X=5
PROCESSORS: P1
P1: Read(X)
""")
        machine = Machine(config)
        machine.execute()
        rendered = machine.render()
        self.assertIn("Read-Miss", rendered)
        self.assertIn("P1 Cache", rendered)
        self.assertIn("VALID", rendered)
        self.assertEqual(len(machine.snapshots), 2)

    def test_all_protocol_examples_parse(self):
        examples = Path(__file__).parents[1] / "examples"
        for path in examples.glob("*.txt"):
            with self.subTest(example=path.name):
                config = parse_input(path.read_text(encoding="utf-8"))
                self.assertTrue(config.operations)


if __name__ == "__main__":
    unittest.main()
