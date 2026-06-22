# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2d020c6b-6189-4bf7-9180-18c5717317e5",
# META       "default_lakehouse_name": "Operations_LH",
# META       "default_lakehouse_workspace_id": "f455d12f-81e4-45ae-9bb7-b195846025fe",
# META       "known_lakehouses": [
# META         {
# META           "id": "2d020c6b-6189-4bf7-9180-18c5717317e5"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "cf70e84c-e5f3-9589-4218-88cc1ae7b47d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# =====================================================================
# 52_gen_work_orders  ->  gold.fact_work_order  (+ gold.workorder_event)
# Work orders are opened from compliance violations and from emission episodes
# (leak repairs). SLA by priority; resolution_time_hours sampled, with a share
# left Open (null resolution). is_breached = resolution_time > sla. State
# transitions are emitted as rows into the gold.workorder_event audit shell.
# =====================================================================


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run config_and_seeds

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RUN_MODE = "backfill"
try:
    RUN_MODE = getArgument("run_mode", "backfill")
except Exception:
    pass
RUN_MODE = (str(RUN_MODE) or "backfill").lower()

try:
    _start_override = getArgument("start_date", "")
    _end_override   = getArgument("end_date",   "")
except Exception:
    _start_override = ""
    _end_override   = ""

INCREMENTAL_LOOKBACK_DAYS = 1
if RUN_MODE == "incremental":
    WINDOW_END   = pd.Timestamp.utcnow().floor("D")
    WINDOW_START = WINDOW_END - pd.Timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
    WRITE_MODE   = "append"
else:
    WINDOW_START = pd.Timestamp(HISTORY_START)
    WINDOW_END   = pd.Timestamp(REAL_PLUME_END)
    WRITE_MODE   = "overwrite"

if _start_override:
    WINDOW_START = pd.Timestamp(_start_override)
if _end_override:
    WINDOW_END   = pd.Timestamp(_end_override)

def _add_date_sk(sdf, ts_col):
    return sdf.withColumn("date_sk", F.date_format(F.col(ts_col), "yyyyMMdd").cast("long"))

def write_new_fact(sdf, table):
    w = sdf.write.mode(WRITE_MODE).format("delta")
    if WRITE_MODE == "overwrite":
        w = w.option("overwriteSchema", "true")
    w.saveAsTable(table)
    print(f"{table}: {sdf.count()} rows ({WRITE_MODE})")

print(f"RUN_MODE={RUN_MODE}  window={WINDOW_START.date()}..{WINDOW_END.date()}  write={WRITE_MODE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SLA = {"P1": 24, "P2": 72, "P3": 168, "P4": 336}
DISC_TEAM = None  # team chosen at random across the 8 crews

# Source 1: compliance violations
comp = (spark.table("gold.fact_compliance_event")
          .where(F.col("is_violation"))
          .select("facility_sk","equipment_sk","event_ts","severity")
          .where((F.col("event_ts") >= F.lit(str(WINDOW_START))) &
                 (F.col("event_ts") <= F.lit(str(WINDOW_END))))
          .toPandas())
# Source 2: emission episodes (repairs)
ep = pd.DataFrame()  # episode-sourced work orders removed — real data only

r = get_rng("workorders")
wo_rows = []; ev_rows = []; n = 0

def make_wo(facility_sk, equipment_sk, created, severity, source):
    global n
    n += 1
    tid = f"WO-{n:06d}"
    prio = {"Critical": "P1", "Major": "P2", "Minor": "P3"}.get(severity, "P4")
    sla = SLA[prio]
    team = int(r.integers(1, 9))
    created = pd.Timestamp(created)
    # ~85% are resolved; the rest remain open
    resolved = r.random() < 0.85
    if resolved:
        res_h = float(r.gamma(2.0, sla / 3.0))
        closed = created + pd.Timedelta(hours=res_h)
        status = "Closed"
    else:
        res_h = None; closed = None; status = str(r.choice(["Open","In Progress"]))
    wo_rows.append({
        "work_order_sk": n, "ticket_id": tid,
        "facility_sk": int(facility_sk),
        "equipment_sk": (int(equipment_sk) if equipment_sk is not None and not pd.isna(equipment_sk) else None),
        "team_sk": team, "created_ts": created.to_pydatetime(),
        "closed_ts": (closed.to_pydatetime() if closed is not None else None),
        "priority": prio, "source": source, "status": status,
        "sla_hours": float(sla),
        "resolution_time_hours": (round(res_h, 2) if res_h is not None else None),
        "is_breached": bool(res_h is not None and res_h > sla),
        "downtime_hours": round(float(r.uniform(1, 24)), 2),
        "cost_usd": round(float(r.gamma(2.0, 1200.0)), 2),
    })
    # audit trail
    actor = f"crew_{team}"
    ev_rows.append({"ticket_id": tid, "facility_sk": int(facility_sk),
                    "equipment_sk": (int(equipment_sk) if equipment_sk is not None and not pd.isna(equipment_sk) else None),
                    "event_ts": created.to_pydatetime(), "from_status": None, "to_status": "Open",
                    "actor": "system", "note": f"opened from {source}"})
    if status in ("In Progress", "Closed"):
        ip_ts = created + pd.Timedelta(hours=float(r.uniform(0.5, 6)))
        ev_rows.append({"ticket_id": tid, "facility_sk": int(facility_sk),
                        "equipment_sk": (int(equipment_sk) if equipment_sk is not None and not pd.isna(equipment_sk) else None),
                        "event_ts": ip_ts.to_pydatetime(), "from_status": "Open", "to_status": "In Progress",
                        "actor": actor, "note": "dispatched"})
    if status == "Closed":
        ev_rows.append({"ticket_id": tid, "facility_sk": int(facility_sk),
                        "equipment_sk": (int(equipment_sk) if equipment_sk is not None and not pd.isna(equipment_sk) else None),
                        "event_ts": closed.to_pydatetime(), "from_status": "In Progress", "to_status": "Closed",
                        "actor": actor, "note": "repair verified"})

for _, c in comp.iterrows():
    make_wo(c.facility_sk, c.equipment_sk, c.event_ts, c.severity, "Compliance")
for e in ep.itertuples():
    sev = "Critical" if float(e.peak_rate_kg_s) > 0.5 else ("Major" if float(e.peak_rate_kg_s) > 0.1 else "Minor")
    created = pd.Timestamp(e.end_ts) + pd.Timedelta(hours=float(r.uniform(1, 48)))
    if WINDOW_START <= created <= WINDOW_END:
        make_wo(e.facility_sk, e.equipment_sk, created, sev, "Episode")

wp = pd.DataFrame(wo_rows); ev = pd.DataFrame(ev_rows)
print(f"work orders: {len(wp)}  open={int((wp.status!='Closed').sum()) if len(wp) else 0}  "
      f"breached={int(wp.is_breached.sum()) if len(wp) else 0}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sdf = (spark.createDataFrame(wp)
         .withColumn("created_ts", F.col("created_ts").cast("timestamp"))
         .withColumn("closed_ts",  F.col("closed_ts").cast("timestamp"))
         .withColumn("equipment_sk", F.col("equipment_sk").cast("long"))
         .withColumn("resolution_time_hours", F.col("resolution_time_hours").cast("double")))
sdf = _add_date_sk(sdf, "created_ts")
write_new_fact(sdf, "gold.fact_work_order")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── audit events -> existing empty shell gold.workorder_event ─────────
# Match the shell schema exactly: ticket_id, facility_sk(bigint), equipment_sk(bigint),
# event_ts, from_status, to_status, actor, note.
ev_cols = ["ticket_id","facility_sk","equipment_sk","event_ts","from_status","to_status","actor","note"]
esdf = (spark.createDataFrame(ev)
          .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
          .withColumn("facility_sk", F.col("facility_sk").cast("long"))
          .withColumn("equipment_sk", F.col("equipment_sk").cast("long"))
          .select(*ev_cols))
esdf.write.mode(WRITE_MODE).format("delta").saveAsTable("gold.workorder_event")
print(f"gold.workorder_event: {esdf.count()} audit rows ({WRITE_MODE})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
