

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


PROTOCOLS = {
    "WRITE_INVALIDATE_WRITE_THROUGH": "write-through",
    "WRITE_INVALIDATE_WRITE_BACK": "write-back",
    "WRITE_ONCE": "write-once",
}


@dataclass
class Line:
    """One variable's cached value and coherence state."""

    value: int
    state: str


@dataclass
class Operation:
    """A single processor read, write, or explicit cache replacement."""

    processor: str
    kind: str
    variable: str
    value: Optional[int] = None


@dataclass
class Config:
    """Validated workload and initial state parsed from the input file."""

    protocol: str
    memory: dict[str, int]
    processors: list[str]
    operations: list[Operation]


@dataclass
class Machine:
    """Serialized shared-bus model with private per-processor caches.

    The three protocols use these states:
    * write-through: VALID / INV
    * write-back ownership: RO / RW / INV
    * Write-Once: VALID / RES / DIRTY / INV

    Each public operation is applied atomically in input order. Snapshots keep
    historical rows in the rendered trace from changing as later events run.
    """

    config: Config
    memory: dict[str, int] = field(init=False)
    caches: dict[str, dict[str, Line]] = field(init=False)
    stats: Counter = field(default_factory=Counter)
    trace: list[tuple[str, str]] = field(default_factory=list)
    snapshots: list[tuple[dict[str, int], dict[str, dict[str, Optional[tuple[int, str]]]]]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.memory = dict(self.config.memory)
        self.caches = {p: {} for p in self.config.processors}

    @property
    def protocol(self) -> str:
        return PROTOCOLS[self.config.protocol]

    def _valid(self, processor: str, variable: str) -> Optional[Line]:
        line = self.caches[processor].get(variable)
        return line if line and line.state != "INV" else None

    def _owner(self, variable: str, states: tuple[str, ...]) -> Optional[str]:
        for p in self.config.processors:
            line = self._valid(p, variable)
            if line and line.state in states:
                return p
        return None

    def _invalidate_others(self, requester: str, variable: str) -> None:
        for p in self.config.processors:
            if p != requester and variable in self.caches[p]:
                self.caches[p][variable].state = "INV"

    def _read(self, p: str, x: str) -> str:
        """Perform a local read or service its miss through the bus/memory."""
        local = self._valid(p, x)
        if local:
            self.stats["read_hits"] += 1
            return f"{p} reads {x} (Read-Hit)"

        self.stats["read_misses"] += 1
        value = self.memory[x]
        if self.protocol == "write-back":
            owner = self._owner(x, ("RW",))
            if owner:
                value = self.caches[owner][x].value
                self.memory[x] = value
                self.stats["writebacks"] += 1
                self.caches[owner][x].state = "RO"
                state = "RO"
            else:
                state = "RO"
            self.caches[p][x] = Line(value, state)
        elif self.protocol == "write-once":
            owner = self._owner(x, ("DIRTY",))
            if owner:
                value = self.caches[owner][x].value
                self.memory[x] = value
                self.stats["writebacks"] += 1
                self.caches[owner][x].state = "VALID"
            self.caches[p][x] = Line(value, "VALID")
        else:
            self.caches[p][x] = Line(value, "VALID")
        return f"{p} reads {x} (Read-Miss)"

    def _write(self, p: str, x: str, value: int) -> str:
        """Apply a store and protocol-specific invalidation/ownership actions."""
        local = self._valid(p, x)
        hit = local is not None
        self.stats["write_hits" if hit else "write_misses"] += 1
        label = "Write-Hit" if hit else "Write-Miss"

        if self.protocol == "write-through":
            self.stats["invalidations"] += 1
            self._invalidate_others(p, x)
            self.memory[x] = value
            self.caches[p][x] = Line(value, "VALID")
        elif self.protocol == "write-back":
            if hit and local.state == "RW":
                local.value = value
                # An RW hit is private and requires no bus transaction.
            else:
                # A write miss may take the latest value from an RW owner.
                owner = self._owner(x, ("RW",))
                if not hit:
                    base = self.caches[owner][x].value if owner else self.memory[x]
                    self.caches[p][x] = Line(base, "INV")
                self.stats["invalidations"] += 1
                self._invalidate_others(p, x)
                self.caches[p][x].value = value
                self.caches[p][x].state = "RW"
        else:  # Goodman's Write-Once
            if hit and local.state in ("RES", "DIRTY"):
                local.value = value
                local.state = "DIRTY"
            else:
                # First write is write-through, whether it followed a read or
                # fetched the block on the write itself.
                owner = self._owner(x, ("DIRTY",))
                if not hit:
                    base = self.caches[owner][x].value if owner else self.memory[x]
                    self.caches[p][x] = Line(base, "INV")
                    if owner:
                        self.memory[x] = base
                        self.stats["writebacks"] += 1
                self.stats["invalidations"] += 1
                self._invalidate_others(p, x)
                self.memory[x] = value
                self.caches[p][x].value = value
                self.caches[p][x].state = "RES"
        return f"{p} writes {x}={value} ({label})"

    def _replace(self, p: str, x: str) -> str:
        """Evict one named variable, writing back a dirty RW owner if needed."""
        line = self.caches[p].get(x)
        if line and line.state != "INV" and self.protocol == "write-back" and line.state == "RW":
            self.memory[x] = line.value
            self.stats["writebacks"] += 1
        self.caches[p].pop(x, None)
        return f"{p} replaces {x} (Replacement)"

    def execute(self) -> None:
        """Run configured operations and retain one state snapshot per row."""
        self.trace.clear()
        self.snapshots.clear()
        self.trace.append(("Initial State", ""))
        self._save_snapshot()
        for op in self.config.operations:
            if op.kind == "Read":
                event = self._read(op.processor, op.variable)
            elif op.kind == "Write":
                assert op.value is not None
                event = self._write(op.processor, op.variable, op.value)
            else:
                event = self._replace(op.processor, op.variable)
            self.trace.append((event, ""))
            self._save_snapshot()

    def _save_snapshot(self) -> None:
        self.snapshots.append((dict(self.memory), {
            p: {variable: (line.value, line.state) for variable, line in cache.items()}
            for p, cache in self.caches.items()
        }))

    def render(self) -> str:
        """Format the operation trace and final counters as a plain-text report."""
        variables = list(self.config.memory)
        processors = self.config.processors
        lines: list[str] = []
        lines.append("BUS-BASED SNOOPING CACHE COHERENCE SIMULATOR")
        lines.append(f"Protocol: {self.config.protocol}")
        lines.append("")
        event_width = 34
        value_width = 5
        state_width = 9
        pair_width = value_width + 1 + state_width
        memory_width = max(len("Memory"), len(variables) * value_width + len(variables) - 1)
        processor_widths = [max(len(p + " Cache"), len(variables) * pair_width + 3 * (len(variables) - 1)) for p in processors]
        header = f"{'#':>3} | {'Event':<{event_width}} | {'Memory':^{memory_width}} | "
        header += " | ".join(f"{p + ' Cache':^{w}}" for p, w in zip(processors, processor_widths))
        lines.append(header)
        sub = "     | " + " " * event_width + " | " + " ".join(f"{v:>{value_width}}" for v in variables) + " " * max(0, memory_width - (len(variables) * value_width + len(variables) - 1)) + " | "
        processor_headers = []
        for width in processor_widths:
            pairs = " | ".join(f"{'Val(' + v + ')':>{value_width}} {'State(' + v + ')':<{state_width}}" for v in variables)
            processor_headers.append(f"{pairs:<{width}}")
        sub += " | ".join(processor_headers)
        lines.append(sub)
        lines.append("-" * max(90, len(header), len(sub)))
        for i, (event, _) in enumerate(self.trace):
            mem = " ".join(f"{self.memory[v]:>{value_width}}" for v in variables)
            row = f"{i:>3} | {event:<{event_width}} | {mem:<{memory_width}} | "
            # Render each saved snapshot (captured just below in execute state).
            snap = self.snapshots[i] if i < len(self.snapshots) else None
            if snap:
                memory_snap, cache_snap = snap
                mem = " ".join(f"{memory_snap[v]:>{value_width}}" for v in variables)
                row = f"{i:>3} | {event:<{event_width}} | {mem:<{memory_width}} | "
                cells = []
                for p, width in zip(processors, processor_widths):
                    pairs = []
                    for v in variables:
                        line = cache_snap[p].get(v)
                        value, state = line if line else ("-", "-")
                        pairs.append(f"{str(value):>{value_width}} {state:<{state_width}}")
                    cells.append(f" | ".join(pairs).ljust(width))
            else:
                cells = []
                for p, width in zip(processors, processor_widths):
                    pairs = []
                    for v in variables:
                        line = self.caches[p].get(v)
                        value, state = (line.value, line.state) if line else ("-", "-")
                        pairs.append(f"{str(value):>{value_width}} {state:<{state_width}}")
                    cells.append(f" | ".join(pairs).ljust(width))
            row += " | ".join(cells)
            lines.append(row)

        lines.extend(["", "FINAL SYSTEM SUMMARY", "Global memory: " + ", ".join(f"{v}={self.memory[v]}" for v in variables)])
        for p in processors:
            contents = [f"{v}={line.value}({line.state})" for v, line in self.caches[p].items()]
            lines.append(f"{p} cache: " + (", ".join(contents) if contents else "empty"))
        lines.extend([
            f"Read hits: {self.stats['read_hits']}    Read misses: {self.stats['read_misses']}",
            f"Write hits: {self.stats['write_hits']}   Write misses: {self.stats['write_misses']}",
            f"Bus invalidation commands: {self.stats['invalidations']}",
            f"Memory write-back operations: {self.stats['writebacks']}",
        ])
        return "\n".join(lines)


def parse_input(text: str) -> Config:
    """Parse and validate the documented PROTOCOL/MEMORY/PROCESSORS format."""
    protocol = None
    memory = None
    processors = None
    operations: list[Operation] = []
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if ":" not in line:
            raise ValueError(f"line {number}: expected KEY: value or PROCESSOR: operation")
        key, value = (part.strip() for part in line.split(":", 1))
        if key.upper() == "PROTOCOL":
            protocol = value.upper()
        elif key.upper() == "MEMORY":
            memory = {}
            for item in value.split(","):
                match = re.fullmatch(r"\s*([A-Za-z][A-Za-z0-9_]*)\s*=\s*(-?\d+)\s*", item)
                if not match:
                    raise ValueError(f"line {number}: invalid memory item {item!r}")
                name, val = match.groups()
                if name in memory:
                    raise ValueError(f"line {number}: duplicate variable {name}")
                memory[name] = int(val)
        elif key.upper() == "PROCESSORS":
            processors = [p.strip() for p in value.split(",") if p.strip()]
        else:
            match = re.fullmatch(r"(Read|Write|Replace)\s*\((.*)\)", value, re.IGNORECASE)
            if not match:
                raise ValueError(f"line {number}: expected Read(X), Write(X, value), or Replace(X)")
            kind, args = match.groups()
            parts = [part.strip() for part in args.split(",")]
            kind = kind.title()
            if kind == "Write":
                if len(parts) != 2 or not re.fullmatch(r"-?\d+", parts[1]):
                    raise ValueError(f"line {number}: Write requires a variable and integer value")
                operations.append(Operation(key, kind, parts[0], int(parts[1])))
            else:
                if len(parts) != 1:
                    raise ValueError(f"line {number}: {kind} requires exactly one variable")
                operations.append(Operation(key, kind, parts[0]))
    if protocol not in PROTOCOLS:
        raise ValueError(f"PROTOCOL must be one of: {', '.join(PROTOCOLS)}")
    if memory is None or not 1 <= len(memory) <= 3:
        raise ValueError("MEMORY must define between 1 and 3 variables")
    if processors is None or not 1 <= len(processors) <= 3 or len(set(processors)) != len(processors):
        raise ValueError("PROCESSORS must list 1 to 3 unique processor IDs")
    for op in operations:
        if op.processor not in processors:
            raise ValueError(f"operation uses undeclared processor {op.processor!r}")
        if op.variable not in memory:
            raise ValueError(f"operation uses undeclared variable {op.variable!r}")
    return Config(protocol, memory, processors, operations)


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point; return 0 on success and 2 for input/file errors."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", help="input file; defaults to stdin")
    args = parser.parse_args(argv)
    try:
        text = Path(args.input).read_text(encoding="utf-8") if args.input else sys.stdin.read()
        config = parse_input(text)
        machine = Machine(config)
        machine.execute()
        print(machine.render())
        return 0
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
