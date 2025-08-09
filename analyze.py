#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, Any, Optional, Callable, List

import pandas as pd


def maybe_float(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def maybe_int(x: str) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def parse_summary(path: Path) -> Dict[str, Any]:
    s = path.read_text(encoding="utf-8", errors="ignore")
    g: Dict[str, Any] = {}

    def grab(rx: str, key: str, cast_fn: Optional[Callable[[str], Any]] = None) -> bool:
        m = re.search(rx, s, flags=re.IGNORECASE)
        if not m:
            return False
        val = m.group(1)
        if cast_fn is not None:
            try:
                g[key] = cast_fn(val)
            except Exception:
                g[key] = val
        else:
            g[key] = val
        return True

    grab(r"Nodes:\s+(\d+)", "nodes", lambda x: int(x))
    grab(r"Sync interval:\s*([^\n]+)", "sync_interval")
    grab(r"Write interval:\s*([^\n]+)", "write_interval")
    grab(r"HB:\s*([^\n]+)", "heartbeat")
    grab(r"Suspect:\s*([^\n]+)", "suspect_timeout")
    grab(r"Delta max:\s*(\d+)", "delta_chunk_bytes", lambda x: int(x))

    m = re.search(
        r"Chaos:\s+loss=([0-9.]+)\s+dup=([0-9.]+)\s+reorder=([0-9.]+)\s+delay=([a-z0-9]+)\s+jitter=([a-z0-9]+)",
        s,
        re.IGNORECASE,
    )
    if m:
        g.update(
            {
                "loss": maybe_float(m.group(1)),
                "dup": maybe_float(m.group(2)),
                "reorder": maybe_float(m.group(3)),
                "delay": m.group(4),
                "jitter": m.group(5),
            }
        )

    m2 = re.search(
        r"SyncLatency\(s\):\s+mean=([0-9.]+)\s+stdev=([0-9.]+)\s+samples=(\d+)",
        s,
        re.IGNORECASE,
    )
    if m2:
        g.update(
            {
                "mean_latency": maybe_float(m2.group(1)),
                "stdev_latency": maybe_float(m2.group(2)),
                "samples": maybe_int(m2.group(3)),
            }
        )

    m3 = re.search(r"Convergence\(s\):\s+([0-9.]+|n/a)", s, re.IGNORECASE)
    if m3:
        val = m3.group(1)
        try:
            g["convergence_s"] = None if val.lower() == "n/a" else float(val)
        except Exception:
            g["convergence_s"] = None

    return g


def percentiles_from_rounds(path: Path) -> Dict[str, Any]:
    try:
        df = pd.read_csv(path)
    except Exception:
        return {
            "p50": float("nan"),
            "p95": float("nan"),
            "p99": float("nan"),
            "samples_csv": 0,
        }

    col = "latency_seconds"
    if col not in df.columns or df.empty:
        return {
            "p50": float("nan"),
            "p95": float("nan"),
            "p99": float("nan"),
            "samples_csv": 0,
        }

    qs = df[col].quantile([0.5, 0.95, 0.99])

    def _pick(q: float) -> float:
        try:
            v = qs.loc[q]
            return float(v) if pd.notna(v) else float("nan")
        except Exception:
            return float("nan")

    return {
        "p50": _pick(0.5),
        "p95": _pick(0.95),
        "p99": _pick(0.99),
        "samples_csv": int(df.shape[0]),
    }


def scan_runs(root: Path) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for summary in root.rglob("summary.txt"):
        run_dir = summary.parent
        sync_rounds = run_dir / "sync_rounds.csv"
        row: Dict[str, Any] = {"run_dir": str(run_dir.relative_to(root))}
        row.update(parse_summary(summary))
        if sync_rounds.exists():
            row.update(percentiles_from_rounds(sync_rounds))
        rows.append(row)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?", default="out")
    ap.add_argument("--out", default=None, help="write combined CSV here")
    args = ap.parse_args()

    root = Path(args.root)
    df = scan_runs(root)

    # Derive a compact label if possible
    if not df.empty:
        cols = [
            "nodes",
            "loss",
            "reorder",
            "delay",
            "jitter",
            "sync_interval",
            "write_interval",
            "heartbeat",
            "suspect_timeout",
            "delta_chunk_bytes",
        ]
        have = [c for c in cols if c in df.columns]
        if have:
            vals = df[have].astype(str).fillna("")
            # Build a plain list[str] of labels
            labels = vals.apply(lambda r: "_".join(r.values.tolist()), axis=1).tolist()
            df["label"] = labels
        else:
            df["label"] = ""

    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(outp, index=False)
        print(f"Wrote {outp}")
    else:
        print(df.to_csv(index=False))

    # Optional: quick pivot by nodes + loss + reorder
    try:
        if not df.empty and all(
            c in df.columns for c in ["nodes", "loss", "reorder", "p95"]
        ):
            piv = df.pivot_table(
                values="p95", index=["nodes", "loss", "reorder"], aggfunc="mean"
            ).reset_index()
            print("\nMean p95 latency by nodes×loss×reorder:")
            print(piv.to_string(index=False))
    except Exception:
        pass


if __name__ == "__main__":
    main()
