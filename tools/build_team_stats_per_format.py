#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import os
import shutil
import sqlite3
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

from build_team_stats import ensure_schema


def detect_formatids(parquet_path: Path) -> list[str]:
    pf = pq.ParquetFile(parquet_path)
    formats: set[str] = set()
    for batch in pf.iter_batches(columns=["formatid"], batch_size=65536):
        for value in batch.column(0).to_pylist():
            fmt = (value or "unknown").strip().lower()
            if fmt:
                formats.add(fmt)
    return sorted(formats) or ["unknown"]


def merge_format_db(master_conn: sqlite3.Connection, format_db: Path) -> None:
    master_conn.execute("ATTACH DATABASE ? AS src", (str(format_db),))
    try:
        master_conn.execute("BEGIN")
        master_conn.execute(
            """
            INSERT INTO combo_bucket(formatid, elo_bucket, combo_size, combo_key, games, wins, sum_elo)
            SELECT formatid, elo_bucket, combo_size, combo_key, games, wins, sum_elo
            FROM src.combo_bucket
            WHERE formatid <> 'all'
            ON CONFLICT(formatid, elo_bucket, combo_size, combo_key) DO UPDATE SET
              games=combo_bucket.games+excluded.games,
              wins=combo_bucket.wins+excluded.wins,
              sum_elo=combo_bucket.sum_elo+excluded.sum_elo
            """
        )
        master_conn.execute(
            """
            INSERT INTO matches_bucket(formatid, elo_bucket, matches)
            SELECT formatid, elo_bucket, matches
            FROM src.matches_bucket
            WHERE formatid <> 'all'
            ON CONFLICT(formatid, elo_bucket) DO UPDATE SET
              matches=matches_bucket.matches+excluded.matches
            """
        )
        master_conn.execute("COMMIT")
    except Exception:
        master_conn.execute("ROLLBACK")
        raise
    finally:
        master_conn.execute("DETACH DATABASE src")


def is_built_format_db(db_path: Path) -> bool:
    if not db_path.exists() or db_path.stat().st_size <= 4096:
        return False
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        quick = cur.execute("PRAGMA quick_check").fetchone()
        if not quick or quick[0] != "ok":
            conn.close()
            return False
        matches = cur.execute("SELECT COALESCE(SUM(matches),0) FROM matches_bucket WHERE formatid <> 'all'").fetchone()[0]
        combos = cur.execute("SELECT COUNT(*) FROM combo_bucket WHERE formatid <> 'all'").fetchone()[0]
        conn.close()
        return bool(matches) and bool(combos)
    except Exception:
        return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", default="pokemon-showdown-replays")
    ap.add_argument("--glob", default="*.parquet")
    ap.add_argument("--out_dir", default="teams-by-format")
    ap.add_argument("--work_root", default="/root/dev/team-format-build")
    ap.add_argument("--chunk_files", type=int, default=1)
    ap.add_argument("--batch_size", type=int, default=256)
    ap.add_argument("--flush", type=int, default=100)
    ap.add_argument("--flush_rows", type=int, default=2000)
    ap.add_argument("--elo_step", type=int, default=100)
    ap.add_argument("--min_size", type=int, default=2)
    ap.add_argument("--max_size", type=int, default=6)
    ap.add_argument("--min_games", type=int, default=100)
    ap.add_argument("--format", action="append", dest="formats", help="Restrict build to one or more formatids")
    ap.add_argument("--formats_file", default="", help="Path to a newline-delimited format list")
    ap.add_argument("--resume", action="store_true", help="Skip formats whose sqlite already exists")
    ap.add_argument("--keep_links", action="store_true")
    ap.add_argument("--fast_sqlite", action="store_true", default=False)
    ap.add_argument("--no_fast_sqlite", action="store_false", dest="fast_sqlite")
    ap.add_argument("--merge_into", default="", help="Merge each completed format DB into this master SQLite")
    ap.add_argument("--delete_after_merge", action="store_true", help="Delete per-format SQLite after a successful merge")
    args = ap.parse_args()

    files = [Path(p) for p in sorted(glob.glob(os.path.join(args.data_dir, args.glob)))]
    if not files:
        raise SystemExit("No parquet files found.")

    groups: dict[str, list[Path]] = defaultdict(list)
    for path in files:
        for fmt in detect_formatids(path):
            groups[fmt].append(path)

    selected_formats = sorted(groups)
    requested_formats = []
    if args.formats_file:
        fmt_file = Path(args.formats_file)
        if fmt_file.exists():
            requested_formats.extend(line.strip().lower() for line in fmt_file.read_text(encoding="utf-8").splitlines() if line.strip())
    if args.formats:
        requested_formats.extend(raw.strip().lower() for raw in args.formats if raw and raw.strip())

    if requested_formats:
        wanted_in_order = []
        for fmt in requested_formats:
            if fmt and fmt not in wanted_in_order:
                wanted_in_order.append(fmt)
        selected_formats = [fmt for fmt in wanted_in_order if fmt in groups]

    if not selected_formats:
        raise SystemExit("No matching formats to build.")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    done_dir = out_dir / ".done"
    done_dir.mkdir(parents=True, exist_ok=True)

    work_root = Path(args.work_root)
    work_root.mkdir(parents=True, exist_ok=True)

    chunked_script = Path(__file__).with_name("build_team_stats_chunked.py")
    master_conn: sqlite3.Connection | None = None
    master_path = Path(args.merge_into) if args.merge_into else None
    if master_path:
        master_conn = sqlite3.connect(str(master_path))
        master_conn.row_factory = sqlite3.Row
        ensure_schema(master_conn, fast_sqlite=False)

    print(f"Formats to build: {len(selected_formats)}")
    for idx, fmt in enumerate(selected_formats, start=1):
        out_path = out_dir / f"{fmt}.sqlite"
        done_marker = done_dir / f"{fmt}.done"
        built_ok = is_built_format_db(out_path)
        if args.resume and master_conn is not None and done_marker.exists():
            print(f"[{idx}/{len(selected_formats)}] skip {fmt} (done marker)")
            continue

        if master_conn is not None and built_ok:
            print(f"[{idx}/{len(selected_formats)}] merge existing {fmt} -> {master_path}")
            merge_format_db(master_conn, out_path)
            done_marker.write_text("merged\n", encoding="utf-8")
            if args.delete_after_merge:
                out_path.unlink(missing_ok=True)
            continue

        if args.resume and built_ok:
            print(f"[{idx}/{len(selected_formats)}] skip {fmt} (exists: {out_path})")
            continue

        if out_path.exists() and not built_ok:
            print(f"[{idx}/{len(selected_formats)}] remove incomplete {fmt} -> {out_path}")
            out_path.unlink(missing_ok=True)

        link_dir = work_root / "links" / fmt
        chunk_dir = work_root / "chunks" / fmt
        if link_dir.exists():
            shutil.rmtree(link_dir)
        if chunk_dir.exists():
            shutil.rmtree(chunk_dir)
        link_dir.mkdir(parents=True, exist_ok=True)
        chunk_dir.mkdir(parents=True, exist_ok=True)

        for path in groups[fmt]:
            target = link_dir / path.name
            if target.exists() or target.is_symlink():
                target.unlink()
            target.symlink_to(path)

        print(f"[{idx}/{len(selected_formats)}] build {fmt} ({len(groups[fmt])} files) -> {out_path}")
        cmd = [
            sys.executable,
            str(chunked_script),
            "--data_dir",
            str(link_dir),
            "--glob",
            "*.parquet",
            "--out",
            str(out_path),
            "--work_dir",
            str(chunk_dir),
            "--chunk_files",
            str(args.chunk_files),
            "--batch_size",
            str(args.batch_size),
            "--flush",
            str(args.flush),
            "--flush_rows",
            str(args.flush_rows),
            "--elo_step",
            str(args.elo_step),
            "--min_size",
            str(args.min_size),
            "--max_size",
            str(args.max_size),
            "--min_games",
            str(args.min_games),
            "--format_filter",
            fmt,
        ]
        if args.fast_sqlite:
            cmd.append("--fast_sqlite")
        else:
            cmd.append("--no_fast_sqlite")

        subprocess.run(cmd, check=True)

        if master_conn is not None:
            print(f"[{idx}/{len(selected_formats)}] merge {fmt} -> {master_path}")
            merge_format_db(master_conn, out_path)
            done_marker.write_text("merged\n", encoding="utf-8")
            if args.delete_after_merge:
                out_path.unlink(missing_ok=True)

        if not args.keep_links and link_dir.exists():
            shutil.rmtree(link_dir)
        if chunk_dir.exists():
            shutil.rmtree(chunk_dir)

    if master_conn is not None:
        master_conn.close()

    print(f"OK -> {out_dir}")


if __name__ == "__main__":
    main()
