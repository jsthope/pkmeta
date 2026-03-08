#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import glob
import os
import sqlite3
from collections import defaultdict
from itertools import combinations
from typing import DefaultDict, Tuple

import pyarrow.parquet as pq
from tqdm import tqdm

from build_stats import canonicalize_species, elo_bucketize, parse_log_one_pass


def ensure_schema(conn: sqlite3.Connection, fast_sqlite: bool) -> None:
    if fast_sqlite:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=MEMORY;
            PRAGMA cache_size=-200000;
            PRAGMA mmap_size=30000000000;
            """
        )
    else:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA temp_store=MEMORY;
            """
        )

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS combo_bucket (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          combo_size INTEGER NOT NULL,
          combo_key TEXT NOT NULL,
          games INTEGER NOT NULL,
          wins INTEGER NOT NULL,
          sum_elo INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket, combo_size, combo_key)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS matches_bucket (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          matches INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS combo_query_cache (
          formatid TEXT NOT NULL,
          elo_min INTEGER NOT NULL,
          elo_max INTEGER NOT NULL,
          combo_size INTEGER NOT NULL,
          combo_key TEXT NOT NULL,
          combo_names TEXT NOT NULL,
          combo_types TEXT NOT NULL,
          games INTEGER NOT NULL,
          wins INTEGER NOT NULL,
          sum_elo INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_min, elo_max, combo_size, combo_key)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS combo_query_cache_meta (
          formatid TEXT NOT NULL,
          elo_min INTEGER NOT NULL,
          elo_max INTEGER NOT NULL,
          combo_size INTEGER NOT NULL,
          matches INTEGER NOT NULL,
          cached_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (formatid, elo_min, elo_max, combo_size)
        ) WITHOUT ROWID;
        """
    )
    conn.commit()


def create_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_combo_bucket_fmt_elo_size ON combo_bucket(formatid, elo_bucket, combo_size);
        CREATE INDEX IF NOT EXISTS idx_combo_bucket_fmt_size_games ON combo_bucket(formatid, combo_size, games);
        CREATE INDEX IF NOT EXISTS idx_combo_query_cache_window_games ON combo_query_cache(formatid, elo_min, elo_max, combo_size, games);
        CREATE INDEX IF NOT EXISTS idx_combo_query_cache_window_names ON combo_query_cache(formatid, elo_min, elo_max, combo_size, combo_names);
        """
    )
    conn.commit()


def prune_small_rows(conn: sqlite3.Connection, min_games: int) -> None:
    if min_games <= 1:
        return
    conn.execute("BEGIN")
    conn.execute(
        """
        DELETE FROM combo_bucket
        WHERE (formatid, combo_size, combo_key) IN (
          SELECT formatid, combo_size, combo_key
          FROM combo_bucket
          GROUP BY formatid, combo_size, combo_key
          HAVING SUM(games) < ?
        )
        """,
        (min_games,),
    )
    conn.execute("DELETE FROM combo_query_cache WHERE games < ?", (min_games,))
    conn.execute("DELETE FROM combo_query_cache_meta")
    conn.execute("COMMIT")


def rollup_all(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN")

    conn.execute("DELETE FROM matches_bucket WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO matches_bucket(formatid, elo_bucket, matches)
        SELECT 'all', elo_bucket, SUM(matches)
        FROM matches_bucket
        WHERE formatid <> 'all'
        GROUP BY elo_bucket
        """
    )

    conn.execute("DELETE FROM combo_bucket WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO combo_bucket(formatid, elo_bucket, combo_size, combo_key, games, wins, sum_elo)
        SELECT 'all', elo_bucket, combo_size, combo_key, SUM(games), SUM(wins), SUM(sum_elo)
        FROM combo_bucket
        WHERE formatid <> 'all'
        GROUP BY elo_bucket, combo_size, combo_key
        """
    )

    conn.execute("COMMIT")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", default="data")
    ap.add_argument("--glob", default="train-*.parquet")
    ap.add_argument("--out", default="teams.sqlite")
    ap.add_argument("--elo_step", type=int, default=100)
    ap.add_argument("--min_size", type=int, default=2)
    ap.add_argument("--max_size", type=int, default=6)
    ap.add_argument("--batch_size", type=int, default=8192)
    ap.add_argument("--flush", type=int, default=100000)
    ap.add_argument("--fast_sqlite", action="store_true", help="Use faster SQLite pragmas (synchronous=OFF)")
    ap.add_argument("--max_files", type=int, default=0, help="Process only N parquet files (0 = all)")
    ap.add_argument("--skip_files", type=int, default=0, help="Skip K parquet files before processing")
    ap.add_argument("--min_games", type=int, default=100, help="Drop combo rows below this game count")
    args = ap.parse_args()

    min_size = max(2, min(6, int(args.min_size)))
    max_size = max(min_size, min(6, int(args.max_size)))

    files_all = sorted(glob.glob(os.path.join(args.data_dir, args.glob)))
    if not files_all:
        raise SystemExit("No parquet files found.")

    skip = max(0, args.skip_files)
    files = files_all[skip:]
    if args.max_files and args.max_files > 0:
        files = files[: args.max_files]
    if not files:
        raise SystemExit("No files left to process after --skip_files/--max_files.")

    print(
        f"Parquet files found: {len(files_all)} | skip={skip} | max_files={args.max_files} | "
        f"selected={len(files)} | sizes={min_size}-{max_size}"
    )

    if os.path.exists(args.out):
        os.remove(args.out)

    conn = sqlite3.connect(args.out)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn, fast_sqlite=args.fast_sqlite)

    combo_cache: DefaultDict[Tuple[str, int, int, str], list[int]] = defaultdict(lambda: [0, 0, 0])
    matches_cache: DefaultDict[Tuple[str, int], int] = defaultdict(int)

    def flush() -> None:
        if not (combo_cache or matches_cache):
            return

        conn.execute("BEGIN")

        conn.executemany(
            """
            INSERT INTO combo_bucket(formatid, elo_bucket, combo_size, combo_key, games, wins, sum_elo)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(formatid, elo_bucket, combo_size, combo_key) DO UPDATE SET
              games=games+excluded.games,
              wins=wins+excluded.wins,
              sum_elo=sum_elo+excluded.sum_elo
            """,
            [
                (fmt, bucket, combo_size_key, combo_key, vals[0], vals[1], vals[2])
                for (fmt, bucket, combo_size_key, combo_key), vals in combo_cache.items()
            ],
        )

        conn.executemany(
            """
            INSERT INTO matches_bucket(formatid, elo_bucket, matches)
            VALUES (?,?,?)
            ON CONFLICT(formatid, elo_bucket) DO UPDATE SET
              matches=matches+excluded.matches
            """,
            [(fmt, bucket, matches) for (fmt, bucket), matches in matches_cache.items()],
        )

        conn.execute("COMMIT")
        combo_cache.clear()
        matches_cache.clear()

    seen = 0

    for fp in tqdm(files, desc="Parquet files"):
        pf = pq.ParquetFile(fp)
        for batch in pf.iter_batches(columns=["log", "rating", "formatid"], batch_size=args.batch_size):
            logs = batch.column(0).to_pylist()
            ratings = batch.column(1).to_pylist()
            formats = batch.column(2).to_pylist()

            for log, rating, formatid in zip(logs, ratings, formats):
                if not isinstance(log, str) or not log:
                    continue

                fmt = (formatid or "unknown").strip().lower() or "unknown"
                try:
                    elo = int(rating) if rating is not None else 0
                except Exception:
                    elo = 0
                bucket = elo_bucketize(elo, args.elo_step)

                pm = parse_log_one_pass(log)
                if pm is None:
                    continue

                matches_cache[(fmt, bucket)] += 1

                for side in ("p1", "p2"):
                    won = 1 if side == pm.winner_side else 0
                    team_keys = sorted({
                        key
                        for species in pm.teams_species[side]
                        for key in [canonicalize_species(species)[0]]
                        if key
                    })
                    if len(team_keys) < min_size:
                        continue

                    upper = min(max_size, len(team_keys))
                    for combo_size_key in range(min_size, upper + 1):
                        for combo in combinations(team_keys, combo_size_key):
                            combo_key = "|".join(combo)
                            vals = combo_cache[(fmt, bucket, combo_size_key, combo_key)]
                            vals[0] += 1
                            vals[1] += won
                            vals[2] += elo

                seen += 1
                if seen % args.flush == 0:
                    flush()

    flush()
    rollup_all(conn)
    prune_small_rows(conn, max(1, int(args.min_games)))
    create_indexes(conn)
    conn.close()

    print(
        f"OK -> {args.out} (processed={len(files)}, elo_step={args.elo_step}, batch={args.batch_size}, "
        f"flush={args.flush}, sizes={min_size}-{max_size})"
    )


if __name__ == "__main__":
    main()
