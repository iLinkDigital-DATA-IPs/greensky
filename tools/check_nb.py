"""Check a Fabric notebook-content.py against the structure 02a uses."""
import ast
import re
import sys
import pathlib

# PARAMETERS CELL is a code cell toggled as the parameter cell: Fabric injects pipeline base
# parameters in a cell beneath it, and by no other route a notebook can read.
MARK = re.compile(r"^# (CELL|PARAMETERS CELL|MARKDOWN|METADATA) \*{20,}$")
CODE = ("CELL", "PARAMETERS CELL")


def cells(path):
    lines = pathlib.Path(path).read_text(encoding="utf-8").splitlines()
    out, kind, buf = [], None, []
    for ln in lines:
        m = MARK.match(ln)
        if m:
            if kind:
                out.append((kind, buf))
            kind, buf = m.group(1), []
        else:
            buf.append(ln)
    if kind:
        out.append((kind, buf))
    return out


def check(path):
    name = pathlib.Path(path).parent.name
    cs = cells(path)
    errs = []
    kinds = [k for k, _ in cs]
    if kinds[0] != "METADATA":
        errs.append("file does not open with the header METADATA block")
    # every CELL must be followed by a METADATA block; MARKDOWN must not be
    for i, (k, body) in enumerate(cs):
        nxt = kinds[i + 1] if i + 1 < len(kinds) else None
        if k in CODE and nxt != "METADATA":
            errs.append(f"cell {i}: {k} not followed by METADATA (got {nxt})")
        if k == "MARKDOWN":
            if nxt == "METADATA":
                errs.append(f"cell {i}: MARKDOWN followed by a METADATA block")
            bad = [l for l in body if l.strip() and not l.startswith("#")]
            if bad:
                errs.append(f"cell {i}: MARKDOWN has non-comment line: {bad[0][:60]!r}")
        if k == "METADATA":
            meta = [l for l in body if l.strip()]
            if not all(l.startswith("# META") for l in meta):
                errs.append(f"cell {i}: METADATA block has a non-# META line")
    # at most one parameter cell, and first, so the injected cell lands ahead of %run 00_config
    params = [i for i, k in enumerate(kinds) if k == "PARAMETERS CELL"]
    if len(params) > 1:
        errs.append(f"{len(params)} PARAMETERS CELL markers, expected at most one")
    if params and params[0] != 1:
        errs.append(f"cell {params[0]}: PARAMETERS CELL is not the first cell after the header")
    # code cells must parse together, in order
    code = []
    for k, body in cs:
        if k in CODE:
            code += [("pass  # " + l) if l.startswith("%") else l for l in body]
    try:
        ast.parse("\n".join(code))
    except SyntaxError as e:
        errs.append(f"python syntax: line {e.lineno}: {e.msg}: {e.text!r}")

    n_cell = sum(kinds.count(k) for k in CODE)
    n_md = kinds.count("MARKDOWN")
    print(f"{name}: {n_cell} code cells ({len(params)} parameter), {n_md} markdown cells, "
          f"{len(code)} code lines")
    for e in errs:
        print(f"  FAIL  {e}")
    if not errs:
        print("  OK    structure and syntax")
    return not errs


ok = all(check(p) for p in sys.argv[1:])
sys.exit(0 if ok else 1)
