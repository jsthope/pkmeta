#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import math
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

from build_team_stats import create_indexes, ensure_schema, prune_small_rows, rollup_all


def merge_chunk(conn: sqlite3.Connection, chunk_db: Path) -> None:
    conn.execute("ATTACH DATABASE ? AS src", (str(chunk_db),))
    try:
        conn.execute("BEGIN")
        conn.execute(
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
        conn.execute(
            """
            INSERT INTO matches_bucket(formatid, elo_bucket, matches)
            SELECT formatid, elo_bucket, matches
            FROM src.matches_bucket
            WHERE formatid <> 'all'
            ON CONFLICT(formatid, elo_bucket) DO UPDATE SET
              matches=matches_bucket.matches+excluded.matches
            """
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("DETACH DATABASE src")


def build_chunk(args: argparse.Namespace, skip_files: int, max_files: int, chunk_db: Path) -> None:
    cmd = [
        sys.executable,
        str(Path(__file__).with_name("build_team_stats.py")),
        "--data_dir",
        args.data_dir,
        "--glob",
        args.glob,
        "--out",
        str(chunk_db),
        "--elo_step",
        str(args.elo_step),
        "--min_size",
        str(args.min_size),
        "--max_size",
        str(args.max_size),
        "--batch_size",
        str(args.batch_size),
        "--flush",
        str(args.flush),
        "--flush_rows",
        str(args.flush_rows),
        "--skip_files",
        str(skip_files),
        "--max_files",
        str(max_files),
        "--min_games",
        "1",
    ]
    if args.fast_sqlite:
        cmd.append("--fast_sqlite")
    for fmt in args.format_filters or []:
        cmd.extend(["--format_filter", fmt])
    subprocess.run(cmd, check=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", default="pokemon-showdown-replays")
    ap.add_argument("--glob", default="*.parquet")
    ap.add_argument("--out", default="teams.sqlite")
    ap.add_argument("--work_dir", default="/root/dev/team-chunks")
    ap.add_argument("--chunk_files", type=int, default=4)
    ap.add_argument("--elo_step", type=int, default=100)
    ap.add_argument("--min_size", type=int, default=2)
    ap.add_argument("--max_size", type=int, default=6)
    ap.add_argument("--batch_size", type=int, default=512)
    ap.add_argument("--flush", type=int, default=500)
    ap.add_argument("--flush_rows", type=int, default=5000)
    ap.add_argument("--fast_sqlite", action="store_true", default=True)
    ap.add_argument("--no_fast_sqlite", action="store_false", dest="fast_sqlite")
    ap.add_argument("--max_files", type=int, default=0)
    ap.add_argument("--skip_files", type=int, default=0)
    ap.add_argument("--min_games", type=int, default=100)
    ap.add_argument("--keep_chunks", action="store_true")
    ap.add_argument("--append", action="store_true", help="Append remaining chunks into an existing output DB")
    ap.add_argument("--format_filter", action="append", dest="format_filters")
    args = ap.parse_args()

    files_all = sorted(glob.glob(os.path.join(args.data_dir, args.glob)))
    if not files_all:
        raise SystemExit("No parquet files found.")

    skip = max(0, args.skip_files)
    files = files_all[skip:]
    if args.max_files and args.max_files > 0:
        files = files[: args.max_files]
    if not files:
        raise SystemExit("No files left to process after --skip_files/--max_files.")

    work_dir = Path(args.work_dir)
    if work_dir.exists() and not args.keep_chunks:
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    out_path = Path(args.out)
    if out_path.exists() and not args.append:
        out_path.unlink()

    conn = sqlite3.connect(str(out_path))
    conn.row_factory = sqlite3.Row
    ensure_schema(conn, fast_sqlite=args.fast_sqlite)

    chunk_files = max(1, int(args.chunk_files))
    total_chunks = int(math.ceil(len(files) / chunk_files))
    print(
        f"Chunked team build: total_files={len(files)} chunk_files={chunk_files} chunks={total_chunks} "
        f"batch={args.batch_size} flush={args.flush} flush_rows={args.flush_rows}"
    )

    for chunk_idx in range(total_chunks):
        chunk_skip = skip + (chunk_idx * chunk_files)
        chunk_count = min(chunk_files, len(files) - (chunk_idx * chunk_files))
        chunk_db = work_dir / f"teams_chunk_{chunk_idx:03d}.sqlite"
        if chunk_db.exists():
            chunk_db.unlink()

        print(f"[chunk {chunk_idx + 1}/{total_chunks}] building files skip={chunk_skip} count={chunk_count} -> {chunk_db}")
        build_chunk(args, chunk_skip, chunk_count, chunk_db)
        print(f"[chunk {chunk_idx + 1}/{total_chunks}] merging {chunk_db}")
        merge_chunk(conn, chunk_db)
        if not args.keep_chunks:
            chunk_db.unlink(missing_ok=True)

    print("Finalizing merged team database")
    rollup_all(conn)
    prune_small_rows(conn, max(1, int(args.min_games)))
    create_indexes(conn)
    conn.close()

    if work_dir.exists() and not args.keep_chunks:
        try:
            work_dir.rmdir()
        except OSError:
            pass

    print(f"OK -> {out_path}")


if __name__ == "__main__":
    main()
