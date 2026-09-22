"""Check a Fabric notebook-content.py against the structure 02a uses."""
import ast
import re
import sys
import pathlib

MARK = re.compile(r"^# (CELL|MARKDOWN|METADATA) \*{20,}$")


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
        if k == "CELL" and nxt != "METADATA":
            errs.append(f"cell {i}: CELL not followed by METADATA (got {nxt})")
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
    # code cells must parse together, in order
    code = []
    for k, body in cs:
        if k == "CELL":
            code += [("pass  # " + l) if l.startswith("%") else l for l in body]
    try:
        ast.parse("\n".join(code))
    except SyntaxError as e:
        errs.append(f"python syntax: line {e.lineno}: {e.msg}: {e.text!r}")

    n_cell = kinds.count("CELL")
    n_md = kinds.count("MARKDOWN")
    print(f"{name}: {n_cell} code cells, {n_md} markdown cells, {len(code)} code lines")
    for e in errs:
        print(f"  FAIL  {e}")
    if not errs:
        print("  OK    structure and syntax")
    return not errs


ok = all(check(p) for p in sys.argv[1:])
sys.exit(0 if ok else 1)
