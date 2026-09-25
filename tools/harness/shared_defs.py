"""Top-level definitions shared verbatim between 02b and 02e, and a way to pull them out.

Fabric notebooks cannot import from one another, so 02e carries a character-for-character
copy of 02b's hash, spectral, slot-grid, interval and state-join helpers. harness_ch4.py uses
extract() on both notebooks and fails if any of these differ, so the copy cannot drift into a
second formulation.
"""
import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
NB = REPO / "Methane Emissions/Accelerator/Planetary Computer/Varon et al Approach/notebooks"
NB_02B = NB / "02_scada/02b_gen_scada_telemetry.Notebook/notebook-content.py"
NB_02E = NB / "02_scada/02e_gen_ch4_telemetry.Notebook/notebook-content.py"

# In dependency order: this is also the order they appear in 02e's shared cell.
SHARED = ["HASH_CHUNKS", "HASH_DIVISOR", "TELEMETRY_HASH_GOLDEN", "row_hash_col", "hash_uniform",
          "TELEMETRY_HARMONICS", "TELEMETRY_AMP_JITTER", "HARMONIC_OMEGA", "series_params",
          "spectral_expr", "series_frame", "TELEMETRY_EPOCH", "_EPOCH_TS", "slot_ceil",
          "window_slots", "_slot_range", "merge_intervals", "assert_disjoint", "HOUR_S",
          "_ramp_phi", "state_buckets", "attach_state"]


def extract(path, names=SHARED):
    """{name: exact source text} of the FIRST top-level def or assignment of each name."""
    src = Path(path).read_text(encoding="utf-8").replace("\r\n", "\n")
    code = "\n".join(("pass  # " + ln) if ln.startswith("%") else ln for ln in src.split("\n"))
    found = {}
    for node in ast.parse(code).body:
        if isinstance(node, ast.FunctionDef):
            ids = [node.name]
        elif isinstance(node, ast.Assign):
            ids = [t.id for t in node.targets if isinstance(t, ast.Name)]
        else:
            continue
        for nm in ids:
            if nm in names and nm not in found:
                found[nm] = ast.get_source_segment(code, node)
    missing = [n for n in names if n not in found]
    assert not missing, f"{Path(path).parent.name}: no top-level definition of {missing}"
    return found


def shared_block():
    """The text of 02e's shared cell body, built from 02b."""
    d = extract(NB_02B)
    out = []
    for n in SHARED:
        is_def = d[n].startswith("def ")
        out.append(("\n\n" if is_def else "") + d[n] + ("\n\n" if is_def else ""))
    return "\n".join(out).replace("\n\n\n\n", "\n\n\n").strip("\n")
