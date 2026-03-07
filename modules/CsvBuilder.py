#!/usr/bin/env python3
"""
Build a punctuality CSV (ONLY requested fields) from:
  - tripinfo.xml
  - stopinfo.xml
  - summary.xml (optional, only used if it contains a date-like attribute)

Output columns (only these):
- date
- train_number
- train_id
- relation
- scheduled_departure_time
- scheduled_arrival_time
- realtime_departure_time
- realtime_arrival_time
- relation_direction
- scheduled_departure_date
- scheduled_arrival_date
- realtime_departure_date
- realtime_arrival_date
- station

Notes:
- SUMO typically reports times as seconds from simulation start.
- If you want actual calendar dates, pass --start-date (YYYY-MM-DD) and optional --start-time (HH:MM:SS).
- If calendar info exists in XML attributes, we’ll use it, otherwise we derive from --start-date/--start-time.
"""

from __future__ import annotations

import argparse
import math
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, date, time
from pathlib import Path
from typing import Any, Dict, Optional, List

import pandas as pd
import random

# ---------- helpers ----------
def to_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    x = x.strip()
    if x == "":
        return None
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except ValueError:
        return None


def pick_first(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return str(d[k])
    return None


def sec_to_dt(
    secs: Optional[float],
    base_dt: Optional[datetime],
) -> Optional[datetime]:
    if secs is None or base_dt is None:
        return None
    return base_dt + timedelta(seconds=float(secs))


def fmt_date(dt: Optional[datetime]) -> Optional[str]:
    return dt.date().isoformat() if dt else None


def fmt_time(dt: Optional[datetime]) -> Optional[str]:
    return dt.time().strftime("%H:%M:%S") if dt else None


def parse_iso_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def parse_iso_time(s: str) -> Optional[time]:
    # allow HH:MM or HH:MM:SS
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except Exception:
            continue
    return None


def safe_attr_dict(el: ET.Element) -> Dict[str, str]:
    return dict(el.attrib or {})


# ---------- parsing ----------
@dataclass
class TripTimes:
    trip_id: str
    # real-time seconds
    depart_rt: Optional[float]
    arrive_rt: Optional[float]
    # scheduled seconds (if derivable)
    depart_sched: Optional[float]
    arrive_sched: Optional[float]
    # relation hints (best-effort)
    relation: Optional[str]
    direction: Optional[str]
    train_number: Optional[str]
    delay : float


def parse_tripinfo(tripinfo_path: Path) -> Dict[str, TripTimes]:
    """
    Build a mapping tripId -> TripTimes.
    scheduled = real - delay where possible.
    """
    tree = ET.parse(tripinfo_path)
    root = tree.getroot()

    out: Dict[str, TripTimes] = {}

    for ti in root.iterfind(".//tripinfo"):
        a = safe_attr_dict(ti)
        trip_id = a.get("id") or a.get("tripId") or a.get("name")
        if not trip_id:
            continue

        depart_rt = to_float(a.get("depart"))
        arrive_rt = to_float(a.get("arrival"))

        departDelay = to_float(a.get("departDelay"))

        depart_sched = (depart_rt - departDelay) if (depart_rt is not None and departDelay is not None) else None
        arrive_sched = (arrive_rt - departDelay) if (arrive_rt is not None and departDelay is not None) else None

        # Best-effort train_number (depends on how you configured SUMO)
        train_number = pick_first(
            a,
            keys=["line", "trainNumber", "number", "name", "id"],
        )

        # Best-effort relation: from->to if present, else departEdge->arrivalEdge if present, else None
        relation = None
        fr = pick_first(a, ["from", "fromEdge", "fromTaz"])
        to = pick_first(a, ["to", "toEdge", "toTaz"])
        if fr and to:
            relation = f"{fr}->{to}"
        else:
            dep_lane = a.get("departLane")
            arr_lane = a.get("arrivalLane")
            # lane format often like "edge_0" so edge is before last underscore
            def lane_to_edge(l: Optional[str]) -> Optional[str]:
                if not l:
                    return None
                m = re.match(r"(.+)_\d+$", l)
                return m.group(1) if m else l

            de = lane_to_edge(dep_lane)
            ae = lane_to_edge(arr_lane)
            if de and ae:
                relation = f"{de}->{ae}"

        # Best-effort direction: based on relation arrow if we have it
        direction = None
        if relation and "->" in relation:
            direction = relation.split("->", 1)[1]  # "towards <to>"-ish, but keep minimal

        out[trip_id] = TripTimes(
            trip_id=trip_id,
            depart_rt=depart_rt,
            arrive_rt=arrive_rt,
            depart_sched=depart_sched,
            arrive_sched=arrive_sched,
            relation=relation,
            direction=direction,
            train_number=train_number,
            delay=departDelay
        )

    return out


def parse_summary_for_base_date(summary_path: Path) -> Optional[str]:
    """
    Some summaries (depending on setup) may contain date-like attributes.
    We'll look for anything like YYYY-MM-DD in root or last step attributes.
    Returns the first ISO date string if found.
    """
    tree = ET.parse(summary_path)
    root = tree.getroot()

    # root attributes
    for v in (root.attrib or {}).values():
        if isinstance(v, str):
            m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", v)
            if m:
                return m.group(1)

    steps = root.findall(".//step")
    if steps:
        last = steps[-1]
        for v in (last.attrib or {}).values():
            if isinstance(v, str):
                m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", v)
                if m:
                    return m.group(1)

    return None


def parse_stopinfo_rows(stopinfo_path: Path, trips : dict[TripTimes]) -> List[Dict[str, Any]]:
    random.seed(0)
    tree = ET.parse(stopinfo_path)
    root = tree.getroot()

    rows: List[Dict[str, Any]] = []
    for si in root.iterfind(".//stopinfo"):
        a = safe_attr_dict(si)
        trip_id = a.get("id") or a.get("tripId") or a.get("name")
        departure_delay = None
        if not trip_id:
            continue
        elif trip_id in trips: 
            departure_delay = trips[trip_id].delay


        station = pick_first(a, ["stopID", "busStop", "trainStop", "station", "stop"]).split("_")[0]  # varies by config

        started = to_float(a.get("started"))
        ended = to_float(a.get("ended"))

        # Delays at stop-level are not always present; try common names
        started_delay = departure_delay if departure_delay is not None else 0.0
        ended_delay = abs(((started + 30.) - ended) + departure_delay) if departure_delay is not None else abs(((started + 30.) - ended)) 

        started_sched = (started - started_delay) if (started is not None and started_delay is not None) else None
        ended_sched = (ended - ended_delay) if (ended is not None and ended_delay is not None) else None

        rows.append(
            dict(
                train_id=trip_id,
                station=station,
                realtime_arrival_sec=started,
                realtime_departure_sec=ended,
                scheduled_arrival_sec=started_sched,
                scheduled_departure_sec=ended_sched,
                delay_arrival=started_delay,
                delay_departure=ended_delay
            )
        )

    return rows


# ---------- main transform ----------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tripinfo", required=True, type=Path)
    ap.add_argument("--stopinfo", required=True, type=Path)
    ap.add_argument("--summary", required=False, type=Path)

    ap.add_argument("--out", required=True, type=Path)

    # If you want calendar dates in output:
    ap.add_argument("--start-date", required=False, help="YYYY-MM-DD (used to convert seconds to dates)")
    ap.add_argument("--start-time", required=False, default="00:00:00", help="HH:MM[:SS], default 00:00:00")

    args = ap.parse_args()

    trip_map = parse_tripinfo(args.tripinfo)
    stop_rows = parse_stopinfo_rows(args.stopinfo, trip_map)

    # Determine base datetime:
    base_dt: Optional[datetime] = None
    start_date_str = args.start_date

    if not start_date_str and args.summary and args.summary.exists():
        start_date_str = parse_summary_for_base_date(args.summary)

    if start_date_str:
        d = parse_iso_date(start_date_str)
        t = parse_iso_time(args.start_time) if args.start_time else time(0, 0, 0)
        if d and t:
            base_dt = datetime.combine(d, t)

    # Build output rows (one per station stop)
    out_rows: List[Dict[str, Any]] = []

    for r in stop_rows:
        trip_id = r["train_id"]
        tt = trip_map.get(trip_id)

        # Trip-level scheduled/rt (fallbacks if stop-level scheduled missing)
        # NOTE: Note that stop-level "scheduled" may not exist; we keep None if not derivable.
        # Also, stopinfo gives station-level times; tripinfo gives overall trip endpoint times.
        sched_dep_sec = r.get("scheduled_departure_sec")
        sched_arr_sec = r.get("scheduled_arrival_sec")
        rt_dep_sec = r.get("realtime_departure_sec")
        rt_arr_sec = r.get("realtime_arrival_sec")

        # Convert to datetime if base_dt exists
        sched_dep_dt = sec_to_dt(sched_dep_sec, base_dt)
        sched_arr_dt = sec_to_dt(sched_arr_sec, base_dt)
        rt_dep_dt = sec_to_dt(rt_dep_sec, base_dt)
        rt_arr_dt = sec_to_dt(rt_arr_sec, base_dt)

        # "date" column: if we can, use the real-time arrival date (station event date)
        # otherwise None.
        date_val = fmt_date(rt_arr_dt) or fmt_date(rt_dep_dt) or fmt_date(sched_arr_dt) or fmt_date(sched_dep_dt)

        train_number = tt.train_number if tt else None
        relation = tt.relation if tt else None
        direction = tt.direction if tt else None

        delay_depature = r["delay_departure"]
        delay_arrival = r["delay_arrival"]

        out_rows.append(
            dict(
                date=date_val,
                train_number=train_number,
                train_id=trip_id,
                relation=relation,

                scheduled_departure_time=fmt_time(sched_dep_dt),
                scheduled_arrival_time=fmt_time(sched_arr_dt),
                realtime_departure_time=fmt_time(rt_dep_dt),
                realtime_arrival_time=fmt_time(rt_arr_dt),

                relation_direction=direction,

                scheduled_departure_date=fmt_date(sched_dep_dt),
                scheduled_arrival_date=fmt_date(sched_arr_dt),
                realtime_departure_date=fmt_date(rt_dep_dt),
                realtime_arrival_date=fmt_date(rt_arr_dt),

                delay_arrival=delay_depature,
                delay_departure=delay_arrival,

                station=r.get("station"),
            )
        )

    df = pd.DataFrame(out_rows)

    # Ensure ONLY requested columns (in your order)
    cols = [
        "date",
        "train_number",
        "train_id",
        "relation",
        "scheduled_departure_time",
        "scheduled_arrival_time",
        "realtime_departure_time",
        "realtime_arrival_time",
        "relation_direction",
        "scheduled_departure_date",
        "scheduled_arrival_date",
        "realtime_departure_date",
        "realtime_arrival_date",
        "delay_departure",
        "delay_arrival",
        "realtime_arrival_date",
        "station",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Wrote {len(df)} rows to {args.out}")


if __name__ == "__main__":
    main()
