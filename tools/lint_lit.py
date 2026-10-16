"""Static check: integer literals handed to F.lit() that cannot be a Spark long.

Spark builds F.lit(<python int>) as a LongType literal. 2**63 is one past Long.MAX_VALUE, so
it fails at expression-construction time with NumberFormatException. This is detectable from
the source without a Spark session.

SCOPE, stated honestly: this catches ONE class of Spark-side defect -- an out-of-range
numeric literal. It does not execute any Spark expression and cannot verify that a Spark
expression computes what its Python counterpart computes. That gap is what the in-notebook
golden vectors are for, and they only fire on Fabric.
"""
import ast, re, sys, pathlib

LONG_MIN, LONG_MAX = -(2 ** 63), 2 ** 63 - 1
MARK = re.compile(r"^# (CELL|MARKDOWN|METADATA) \*{20,}$")


def code_of(path):
    out, kind = [], None
    for ln in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        m = MARK.match(ln)
        if m:
            kind = m.group(1); out.append(""); continue
        out.append(("pass  # " + ln) if (kind == "CELL" and ln.startswith("%"))
                   else (ln if kind == "CELL" else ""))
    return "\n".join(out)


def check(path):
    tree = ast.parse(code_of(path))
    bad = []
    for n in ast.walk(tree):
        if not isinstance(n, ast.Call):
            continue
        f = n.func
        name = (f.attr if isinstance(f, ast.Attribute) else
                (f.id if isinstance(f, ast.Name) else None))
        if name != "lit":
            continue
        for a in n.args:
            v = None
            if isinstance(a, ast.Constant) and isinstance(a.value, int) \
                    and not isinstance(a.value, bool):
                v = a.value
            elif isinstance(a, ast.UnaryOp) and isinstance(a.op, ast.USub) \
                    and isinstance(a.operand, ast.Constant) \
                    and isinstance(a.operand.value, int):
                v = -a.operand.value
            if v is not None and not (LONG_MIN <= v <= LONG_MAX):
                bad.append((a.lineno, v))
    nm = pathlib.Path(path).parent.name
    for ln, v in bad:
        print(f"  FAIL {nm}:{ln}  F.lit({v}) exceeds Long.MAX_VALUE ({LONG_MAX}); "
              "Spark cannot build it as a long literal")
    if not bad:
        print(f"  OK   {nm}: no out-of-range F.lit integer literal")
    return not bad


sys.exit(0 if all(check(p) for p in sys.argv[1:]) else 1)
