# Bus-Based Snooping Cache Coherence Simulator

An event-driven simulator for a serialized shared bus, private processor caches, and shared main memory. One variable is one cache block. It supports 1-3 processors, 1-3 integer variables, arbitrary operation streams, and the three required protocols:

- `WRITE_INVALIDATE_WRITE_THROUGH` (`VALID` / `INV`)
- `WRITE_INVALIDATE_WRITE_BACK` (`RO` / `RW` / `INV`)
- `WRITE_ONCE` (`VALID` / `RES` / `DIRTY` / `INV`)

## Source code and run instructions

The implementation is in `simulator.py`. It uses Python 3.10 or later and only the Python standard library, so no compiler or package installation is needed. Run it from this project directory:

```sh
# Run a workload from a text file
python simulator.py example.txt

# Run with one of the assignment benchmark streams
python simulator.py examples/benchmark1_write_once.txt

# Omit the input filename to read a workload from standard input
python simulator.py
# Or redirect a file into standard input
python simulator.py < example.txt

# Show command-line usage
python simulator.py --help

# Run the unit tests
python -m unittest discover -s tests -v
```

### CLI options

```text
usage: simulator.py [-h] [input]
```

- `input` (optional): path to a workload text file. If omitted, the simulator reads standard input.
- `-h`, `--help`: display usage and exit.

The protocol, memory, processors, and operation stream are specified inside the workload file; there are no command-line flags for overriding those values. The program prints the trace and summary to standard output. File or input-format errors are reported to standard error and return exit status `2`; a successful run returns `0`.

The output contains an operation-by-operation trace of global memory and every cache, followed by the final cache and memory contents and hit, miss, invalidation, and write-back counts. Invalidated lines remain visible with state `INV` until replaced. A replaced line is removed from the named processor's cache; a dirty `RW` line is written back first for the write-back protocol.

## Benchmark trace inputs

`examples/` contains input streams for both benchmark cases in the assignment, with a separate file for each protocol. Run each file with `python simulator.py examples/<filename>.txt` to produce its trace for the verification report. The six files are `benchmark1_write_through.txt`, `benchmark1_write_back.txt`, `benchmark1_write_once.txt`, and the corresponding `benchmark2_...` files.

## Workload input format

```text
PROTOCOL: WRITE_INVALIDATE_WRITE_BACK
MEMORY: X=5, Y=10, Z=20
PROCESSORS: P1, P2
P1: Read(X)
P2: Write(X, 10)
P1: Replace(X)
```

Blank lines and `#` comments are ignored. Operations are processed in input order. `Read(variable)`, `Write(variable, integer)`, and `Replace(variable)` are supported. Processor and variable names in operations must have been declared.

## Protocol behavior

The write-through protocol writes every store to memory and invalidates other cached copies. The write-back ownership protocol lets a private `RW` cache write without bus traffic; a read of an `RW` block flushes its current value to memory and downgrades sharers to `RO`. Evicting `RW` writes it back. Write-Once writes the first store through to memory and invalidates peers (`RES`); later stores in `RES`/`DIRTY` stay private and become `DIRTY`. A read miss snooping a `DIRTY` copy flushes it and returns all copies to `VALID`.

Invalidation statistics count broadcast invalidation commands, including commands sent when no other cache currently has a copy. Write-back statistics count dirty owner flushes and dirty write-back evictions. Read/write hit and miss counts classify the requesting processor's local line at the start of the operation.
