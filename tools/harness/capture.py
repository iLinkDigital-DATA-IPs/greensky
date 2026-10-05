"""Run one harness unchanged and fingerprint every DataFrame, Series and ndarray it produces.

    python capture.py [--no-aggregate SITE_GLOB]... HARNESS.py OUT_DIR [harness argv ...]

The harness's stdout is left untouched (the caller captures it); this script reports only on
stderr and writes OUT_DIR/<name>.fingerprint.json. Baselines live in tools/harness/baseline/.

ENVIRONMENT PIN. Before running, the Python, pandas and numpy versions are compared with those
recorded in tools/harness/baseline/*.fingerprint.json (numpy only where the baseline recorded
it). Any difference is a refusal (exit 3): a library upgrade moves hashes and would read
exactly like a regression. With no baseline present there is nothing to pin, and it says so.

WHAT IS CAPTURED, via sys.monitoring (Python 3.12+):
  * every value RETURNED by a function, top-level or nested one level in a tuple/list/dict;
  * every value held in a function's LOCALS when it returns, excluding objects that were
    passed in as arguments (those are someone else's product);
  * the module globals of each module body when it finishes (incl. the harness itself, and
    its globals as they stood if it raises).
"Function" covers code in tools/harness/*.py (not this file) and code the harnesses exec()
from notebook cells (compiled under non-path filenames such as "03a_model_cell"). Library
code is disabled per code location on first sight, so it costs almost nothing.

CANONICAL FORMS (each hashed with sha256):
  DataFrame  index kept as columns unless it is a default RangeIndex; rows sorted by every
             column (natively, else on repr); to_csv(index=False). Row-order-insensitive,
             row-content-sensitive. Unchanged from the first baseline, so its hashes carry over.
  Series     the same, except the index is ALWAYS kept as a column: a Series' values are
             keyed by its index, and with a RangeIndex the key is the position.
  Index      (incl. DatetimeIndex) hashed as the Series of its values, so position is kept
             as the key: an Index of install dates moves if any one date moves.
  ndarray    (ndim >= 1) dtype.str, shape, then the C-order bytes (object arrays: the repr of
             each element). Order-SENSITIVE on purpose: an array has no key column, so sorting
             it would reduce a boolean slot mask to its count -- the summary-count blindness
             the fingerprint exists to beat.
Series, Index and ndarray sites carry a <Series>/<Index>/<ndarray> tag in their name, so
they never share a site (or a #callN numbering) with a DataFrame or with each other.

AGGREGATION. A site hit more than MAX_CALLS times is one "#all" entry: row_count summed, sha256
over the ordered per-call hashes. To see the individual calls of a site, disable it:
    --no-aggregate 'harness_alarm_rate.py::simulate::*'     (repeatable, fnmatch glob)
    CAPTURE_NO_AGGREGATE='glob1,glob2'                       (comma-separated, same effect)
A disaggregated site lists every call as #callN, each with call_args: the scalar arguments
(str/int/float/bool/Timestamp/...) its function was called with, e.g. the equipment_id.
Disaggregation changes only the entries of the named sites; every other hash is identical.
"""
import datetime as _dt
import fnmatch
import glob
import hashlib
import json
import os
import sys
import traceback

import numpy as np
import pandas as pd

MAX_CALLS = 25
HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
BASELINE_DIR = os.path.join(HARNESS_DIR, "baseline")
SELF = os.path.normcase(os.path.abspath(__file__))
VERSIONS = {"python": sys.version.split()[0], "pandas": pd.__version__, "numpy": np.__version__}


# --- environment pin --------------------------------------------------------------------------
def check_environment():
    seen = {}
    for p in sorted(glob.glob(os.path.join(BASELINE_DIR, "*.fingerprint.json"))):
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        for k in VERSIONS:
            if k in d:
                seen.setdefault(k, {}).setdefault(d[k], []).append(os.path.basename(p))
    if not seen:
        print(f"[capture] no baseline in {BASELINE_DIR}: nothing to pin versions against",
              file=sys.stderr)
        return
    problems = []
    for k, vals in seen.items():
        if len(vals) > 1:
            problems.append(f"  the baseline itself is mixed on {k}: "
                            + ", ".join(f"{v} ({len(fs)} files)" for v, fs in vals.items()))
        elif VERSIONS[k] not in vals:
            problems.append(f"  {k}: baseline {next(iter(vals))}, this interpreter {VERSIONS[k]}")
    if problems:
        sys.stderr.write(
            "[capture] REFUSING TO RUN: the environment differs from the baseline's.\n"
            + "\n".join(problems) + "\n"
            f"  interpreter: {sys.executable}\n"
            "  A different Python/pandas/numpy can change sort order, CSV float formatting or RNG\n"
            "  streams, which moves hashes and looks exactly like a regression. Run under the\n"
            f"  baseline's versions, or regenerate the whole baseline in {BASELINE_DIR}\n"
            "  deliberately under the new ones (move the old files aside first).\n")
        sys.exit(3)


# --- canonical hashes -------------------------------------------------------------------------
def canonical_sha(df, keep_index=False):
    d = df
    if keep_index or not (isinstance(d.index, pd.RangeIndex) and d.index.start == 0
                          and d.index.step == 1):
        d = d.reset_index()
        d.columns = [f"__index__{c}" if i < df.index.nlevels else c
                     for i, c in enumerate(d.columns)]
    d = d.copy()
    d.columns = [str(c) for c in d.columns]
    # positional names so duplicate labels cannot break sorting
    names = list(d.columns)
    d.columns = [f"c{i}" for i in range(len(names))]
    try:
        d = d.sort_values(list(d.columns), kind="mergesort", na_position="last")
    except Exception:
        d = d.map(lambda v: repr(v)).sort_values(list(d.columns), kind="mergesort")
    d.columns = names
    try:
        csv = d.to_csv(index=False, lineterminator="\n")
    except Exception:
        csv = d.map(repr).to_csv(index=False, lineterminator="\n")
    return hashlib.sha256(csv.encode("utf-8")).hexdigest()


def series_sha(s):
    return canonical_sha(s.to_frame(name="__value__"), keep_index=True)


def ndarray_sha(a):
    h = hashlib.sha256(f"{a.dtype.str}|{a.shape}|".encode())
    if a.dtype.hasobject:
        for v in a.ravel(order="C"):
            h.update(repr(v).encode("utf-8"))
            h.update(b"\x1f")
    else:
        h.update(np.ascontiguousarray(a).tobytes(order="C"))
    return h.hexdigest()


def index_sha(ix):
    return series_sha(pd.Series(ix, name="__index_values__"))


def tag_of(v):
    if isinstance(v, pd.DataFrame):
        return ""
    if isinstance(v, pd.Series):
        return "<Series>"
    if isinstance(v, pd.Index):
        return "<Index>"
    if isinstance(v, np.ndarray) and v.ndim >= 1:
        return "<ndarray>"
    return None


SCALARS = (str, int, float, bool, np.integer, np.floating, np.bool_,
           pd.Timestamp, pd.Timedelta, _dt.date, _dt.datetime, _dt.timedelta, type(None))


# --- recorder ---------------------------------------------------------------------------------
class Recorder:
    def __init__(self, no_agg):
        self.sites = {}          # site -> list of per-call records, insertion-ordered
        self.calls = {}          # id(frame) -> (ids of capturable arguments, scalar call args)
        self.busy = False
        self.no_agg = no_agg
        self.no_agg_code = {"::".join(p.split("::")[:2]) for p in no_agg}

    def disaggregated(self, site):
        return any(fnmatch.fnmatchcase(site, p) for p in self.no_agg)

    def add(self, site, v, call_args):
        try:
            if isinstance(v, pd.DataFrame):
                rec = {"row_count": int(len(v)), "column_list": [str(c) for c in v.columns],
                       "dtypes": [str(t) for t in v.dtypes], "sha256": canonical_sha(v)}
            elif isinstance(v, pd.Series):
                rec = {"kind": "Series", "row_count": int(len(v)), "name": str(v.name),
                       "dtype": str(v.dtype), "index_dtype": str(v.index.dtype),
                       "sha256": series_sha(v)}
            elif isinstance(v, pd.Index):
                rec = {"kind": "Index", "row_count": int(len(v)), "index_type": type(v).__name__,
                       "dtype": str(v.dtype), "sha256": index_sha(v)}
            else:
                rec = {"kind": "ndarray", "shape": list(v.shape), "dtype": str(v.dtype),
                       "sha256": ndarray_sha(v)}
        except Exception as e:                     # never let fingerprinting break the run
            rec = {"sha256": None, "error": f"{type(e).__name__}: {e}"}
        if call_args is not None and self.disaggregated(site):
            rec["call_args"] = call_args
        self.sites.setdefault(site, []).append(rec)

    def add_value(self, site, v, seen, call_args=None, nest=True):
        t = tag_of(v)
        if t is not None:
            if id(v) not in seen:
                seen.add(id(v))
                self.add(site + t, v, call_args)
        elif nest and isinstance(v, (tuple, list)) and len(v) <= 64:
            for i, x in enumerate(v):
                self.add_value(f"{site}[{i}]", x, seen, call_args, nest=False)
        elif nest and isinstance(v, dict) and len(v) <= 64:
            for k, x in v.items():
                self.add_value(f"{site}[{k!r}]", x, seen, call_args, nest=False)

    def entries(self):
        out = []
        for site, calls in self.sites.items():
            if len(calls) <= MAX_CALLS or self.disaggregated(site):
                for n, r in enumerate(calls):
                    out.append({"frame_name": f"{site}#call{n}" if len(calls) > 1 else site, **r})
            else:
                h = hashlib.sha256()
                for r in calls:
                    h.update((r["sha256"] or "ERR").encode())
                first = calls[0]
                agg = {"frame_name": f"{site}#all", "n_calls": len(calls)}
                if "kind" in first:
                    agg["kind"] = first["kind"]
                if "column_list" in first or "kind" not in first:     # DataFrame site
                    cols = first.get("column_list")
                    agg.update({"row_count": sum(r.get("row_count") or 0 for r in calls),
                                "column_list": cols,
                                "columns_vary": any(r.get("column_list") != cols for r in calls),
                                "dtypes": first.get("dtypes")})
                elif first.get("kind") in ("Series", "Index"):
                    agg.update({"row_count": sum(r.get("row_count") or 0 for r in calls),
                                "dtype": first.get("dtype")})
                else:
                    agg.update({"shape_first": first.get("shape"), "dtype": first.get("dtype"),
                                "shapes_vary": any(r.get("shape") != first.get("shape")
                                                   for r in calls)})
                agg["sha256"] = h.hexdigest()
                out.append(agg)
        return out


R = None


def interesting(code):
    f = code.co_filename
    if f.startswith("<"):
        return False
    if not os.path.isabs(f):
        return True                                 # exec'd notebook cell
    f = os.path.normcase(os.path.abspath(f))
    return f != SELF and os.path.dirname(f) == os.path.normcase(HARNESS_DIR)


def site_of(code):
    f = code.co_filename
    if os.path.isabs(f):
        f = os.path.basename(f)
    return f"{f}::{code.co_qualname}"


def scalar_args(code, loc):
    n = code.co_argcount + code.co_kwonlyargcount
    out = {}
    for k in code.co_varnames[:n]:
        v = loc.get(k)
        if isinstance(v, SCALARS):
            out[k] = v.isoformat() if hasattr(v, "isoformat") else (
                v.item() if isinstance(v, np.generic) else v)
    return out


mon = sys.monitoring
TOOL = mon.PROFILER_ID


def on_start(code, offset):
    if not interesting(code):
        return mon.DISABLE
    if R.busy or code.co_name == "<module>":
        return
    R.busy = True
    try:
        fr = sys._getframe(1)
        loc = fr.f_locals
        ids = {id(v) for v in loc.values() if tag_of(v) is not None}
        args = scalar_args(code, loc) if (R.no_agg and any(
            fnmatch.fnmatchcase(site_of(code), p) for p in R.no_agg_code)) else None
        R.calls[id(fr)] = (ids, args)
    finally:
        R.busy = False


def on_return(code, offset, retval):
    if not interesting(code):
        return mon.DISABLE
    if R.busy:
        return
    R.busy = True
    try:
        fr = sys._getframe(1)
        site = site_of(code)
        argids, call_args = R.calls.pop(id(fr), (set(), None))
        seen = set(argids)
        R.add_value(f"{site}::return", retval, seen, call_args)
        if code.co_name == "<module>":
            items = [(k, v) for k, v in fr.f_globals.items()
                     if isinstance(k, str) and tag_of(v) is not None]
            for k, v in sorted(items, key=lambda kv: kv[0]):
                R.add_value(f"{site}::global:{k}", v, seen)
        else:
            for k, v in list(fr.f_locals.items()):
                if tag_of(v) is not None:
                    R.add_value(f"{site}::local:{k}", v, seen, call_args)
    finally:
        R.busy = False


def parse_argv(argv):
    no_agg = [p for p in os.environ.get("CAPTURE_NO_AGGREGATE", "").split(",") if p.strip()]
    rest = []
    i = 0
    while i < len(argv):
        if argv[i] == "--no-aggregate" and not rest:
            if i + 1 >= len(argv):
                sys.exit("capture.py: --no-aggregate needs a site glob")
            no_agg.append(argv[i + 1])
            i += 2
        else:
            rest.append(argv[i])
            i += 1
    if len(rest) < 2:
        sys.exit(__doc__.split("\n\n")[1])
    return [p.strip() for p in no_agg], rest


def main():
    global R
    no_agg, rest = parse_argv(sys.argv[1:])
    check_environment()
    path = os.path.abspath(rest[0])
    out_dir = os.path.abspath(rest[1])
    name = os.path.splitext(os.path.basename(path))[0]
    R = Recorder(no_agg)
    sys.argv = [path] + rest[2:]
    sys.path.insert(0, os.path.dirname(path))
    os.chdir(os.path.dirname(path))

    mon.use_tool_id(TOOL, "baseline-capture")
    mon.register_callback(TOOL, mon.events.PY_START, on_start)
    mon.register_callback(TOOL, mon.events.PY_RETURN, on_return)
    mon.set_events(TOOL, mon.events.PY_START | mon.events.PY_RETURN)

    g = {"__name__": "__main__", "__file__": path, "__builtins__": __builtins__}
    status, err = "ok", None
    try:
        with open(path, encoding="utf-8") as f:
            code = compile(f.read(), path, "exec")
        exec(code, g)
    except SystemExit as e:
        if e.code not in (None, 0):
            status, err = "failed", f"SystemExit({e.code!r})"
    except BaseException as e:
        status, err = "failed", "".join(traceback.format_exception(e))
    finally:
        sys.stdout.flush()
        mon.set_events(TOOL, 0)

    if status != "ok":
        # the module body never returned: fingerprint whatever globals it reached
        R.busy = True
        seen = set()
        items = [(k, v) for k, v in g.items() if isinstance(k, str) and tag_of(v) is not None]
        for k, v in sorted(items, key=lambda kv: kv[0]):
            R.add_value(f"{os.path.basename(path)}::<module>::global-at-failure:{k}", v, seen)

    doc = {"harness": name, "status": status, "error": err, **VERSIONS,
           "no_aggregate": no_agg, "frames": R.entries()}
    with open(os.path.join(out_dir, f"{name}.fingerprint.json"), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1)
    print(f"[capture] {name}: {status}, {len(doc['frames'])} entries", file=sys.stderr)
    if err:
        print(err, file=sys.stderr)
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
