#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, DefaultDict, Dict, Optional, Tuple
from urllib.request import urlopen

import pyarrow.parquet as pq
from tqdm import tqdm


_TOID_RE = re.compile(r"[^a-z0-9]+")
MOVES_JSON_URL = "https://play.pokemonshowdown.com/data/moves.json"


def to_id(name: str) -> str:
    return _TOID_RE.sub("", (name or "").lower())


def side_from_ident(ident: str) -> Optional[str]:
    if not ident:
        return None
    p = ident.split(":", 1)[0].strip()[:2]
    return p if p in ("p1", "p2") else None


def elo_bucketize(elo: int, step: int) -> int:
    if elo < 0:
        elo = 0
    return (elo // step) * step


def load_move_types(local_json_path: Optional[str] = None) -> Dict[str, Tuple[str, str]]:
    if local_json_path:
        with open(local_json_path, "r", encoding="utf-8") as f:
            raw = f.read()
    else:
        req = urlopen(MOVES_JSON_URL, timeout=30)
        raw = req.read().decode("utf-8")
    data = json.loads(raw)
    out: Dict[str, Tuple[str, str]] = {}
    for k, v in data.items():
        if not isinstance(v, dict):
            continue
        mid = str(k)
        name = str(v.get("name") or mid)
        mtype = str(v.get("type") or "Unknown")
        out[mid] = (name, mtype)
    return out


@dataclass
class ParsedMoveMatch:
    winner_side: str
    move_uses: Dict[str, DefaultDict[str, int]]


def parse_log_moves(log: str) -> Optional[ParsedMoveMatch]:
    players: Dict[str, str] = {}
    winner_name: Optional[str] = None
    move_uses = {"p1": defaultdict(int), "p2": defaultdict(int)}

    for line in log.split("\n"):
        if not line or line[0] != "|":
            continue
        parts = line.split("|")
        if len(parts) < 2:
            continue
        tag = parts[1]

        if tag == "player" and len(parts) >= 4:
            side = parts[2]
            if side in ("p1", "p2"):
                players[side] = parts[3]

        elif tag == "win" and len(parts) >= 3:
            winner_name = parts[2]

        elif tag == "move" and len(parts) >= 4:
            side = side_from_ident(parts[2])
            if side not in ("p1", "p2"):
                continue
            mv_name = parts[3].strip()
            if not mv_name:
                continue
            move_uses[side][mv_name] += 1

    winner_side: Optional[str] = None
    if winner_name:
        if players.get("p1") == winner_name:
            winner_side = "p1"
        elif players.get("p2") == winner_name:
            winner_side = "p2"

    if winner_side not in ("p1", "p2"):
        return None

    return ParsedMoveMatch(winner_side=winner_side, move_uses=move_uses)


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
        CREATE TABLE IF NOT EXISTS move_bucket (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          move_id TEXT NOT NULL,
          move_name TEXT NOT NULL,
          move_type TEXT NOT NULL,
          games INTEGER NOT NULL,
          wins INTEGER NOT NULL,
          uses INTEGER NOT NULL,
          sum_elo INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket, move_id)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS move_type_bucket (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          move_type TEXT NOT NULL,
          games INTEGER NOT NULL,
          wins INTEGER NOT NULL,
          uses INTEGER NOT NULL,
          sum_elo INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket, move_type)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS matches_bucket (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          matches INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket)
        ) WITHOUT ROWID;
        """
    )
    conn.commit()


def create_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_move_bucket_fmt_elo ON move_bucket(formatid, elo_bucket);
        CREATE INDEX IF NOT EXISTS idx_move_bucket_fmt_type ON move_bucket(formatid, move_type);
        CREATE INDEX IF NOT EXISTS idx_type_bucket_fmt_elo ON move_type_bucket(formatid, elo_bucket);
        """
    )
    conn.commit()


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

    conn.execute("DELETE FROM move_bucket WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO move_bucket(formatid, elo_bucket, move_id, move_name, move_type, games, wins, uses, sum_elo)
        SELECT 'all', elo_bucket, move_id, MIN(move_name), MIN(move_type), SUM(games), SUM(wins), SUM(uses), SUM(sum_elo)
        FROM move_bucket
        WHERE formatid <> 'all'
        GROUP BY elo_bucket, move_id
        """
    )

    conn.execute("DELETE FROM move_type_bucket WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO move_type_bucket(formatid, elo_bucket, move_type, games, wins, uses, sum_elo)
        SELECT 'all', elo_bucket, move_type, SUM(games), SUM(wins), SUM(uses), SUM(sum_elo)
        FROM move_type_bucket
        WHERE formatid <> 'all'
        GROUP BY elo_bucket, move_type
        """
    )

    conn.execute("COMMIT")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", default="data")
    ap.add_argument("--glob", default="train-*.parquet")
    ap.add_argument("--out", default="attacks.sqlite")
    ap.add_argument("--elo_step", type=int, default=100)
    ap.add_argument("--batch_size", type=int, default=8192)
    ap.add_argument("--flush", type=int, default=100000)
    ap.add_argument("--fast_sqlite", action="store_true", help="Use faster SQLite pragmas (synchronous=OFF)")
    ap.add_argument("--max_files", type=int, default=0, help="Process only N parquet files (0 = all)")
    ap.add_argument("--skip_files", type=int, default=0, help="Skip K parquet files before processing")
    ap.add_argument("--moves_json", default="", help="Optional local path to moves.json (Pokemon Showdown format)")
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

    print(f"Parquet files found: {len(files_all)} | skip={skip} | max_files={args.max_files} | selected={len(files)}")
    if args.moves_json:
        print(f"Loading move type map from local file: {args.moves_json}")
    else:
        print("Loading move type map from Pokemon Showdown...")
    move_type_map = load_move_types(args.moves_json or None)
    print(f"Loaded {len(move_type_map)} moves")

    if os.path.exists(args.out):
        os.remove(args.out)

    conn = sqlite3.connect(args.out)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn, fast_sqlite=args.fast_sqlite)

    move_cache: DefaultDict[Tuple[str, int, str], list[int | str]] = defaultdict(lambda: ["", "Unknown", 0, 0, 0, 0])
    type_cache: DefaultDict[Tuple[str, int, str], list[int]] = defaultdict(lambda: [0, 0, 0, 0])
    matches_cache: DefaultDict[Tuple[str, int], int] = defaultdict(int)

    def flush() -> None:
        if not (move_cache or type_cache or matches_cache):
            return

        conn.execute("BEGIN")

        conn.executemany(
            """
            INSERT INTO move_bucket(formatid, elo_bucket, move_id, move_name, move_type, games, wins, uses, sum_elo)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(formatid, elo_bucket, move_id) DO UPDATE SET
              move_name=excluded.move_name,
              move_type=excluded.move_type,
              games=games+excluded.games,
              wins=wins+excluded.wins,
              uses=uses+excluded.uses,
              sum_elo=sum_elo+excluded.sum_elo
            """,
            [
                (fmt, bucket, move_id, v[0], v[1], v[2], v[3], v[4], v[5])
                for (fmt, bucket, move_id), v in move_cache.items()
            ],
        )

        conn.executemany(
            """
            INSERT INTO move_type_bucket(formatid, elo_bucket, move_type, games, wins, uses, sum_elo)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(formatid, elo_bucket, move_type) DO UPDATE SET
              games=games+excluded.games,
              wins=wins+excluded.wins,
              uses=uses+excluded.uses,
              sum_elo=sum_elo+excluded.sum_elo
            """,
            [
                (fmt, bucket, move_type, v[0], v[1], v[2], v[3])
                for (fmt, bucket, move_type), v in type_cache.items()
            ],
        )

        conn.executemany(
            """
            INSERT INTO matches_bucket(formatid, elo_bucket, matches)
            VALUES (?,?,?)
            ON CONFLICT(formatid, elo_bucket) DO UPDATE SET
              matches=matches+excluded.matches
            """,
            [(fmt, bucket, m) for (fmt, bucket), m in matches_cache.items()],
        )

        conn.execute("COMMIT")

        move_cache.clear()
        type_cache.clear()
        matches_cache.clear()

    seen = 0

    for fp in tqdm(files, desc="Parquet files"):
        try:
            pf = pq.ParquetFile(fp)
        except Exception as e:
            print(f"Skipping unreadable parquet: {fp} ({e})")
            continue
        for batch in pf.iter_batches(columns=["log", "rating", "formatid"], batch_size=args.batch_size):
            logs = batch.column(0).to_pylist()
            ratings = batch.column(1).to_pylist()
            fmts = batch.column(2).to_pylist()

            for log, rating, formatid in zip(logs, ratings, fmts):
                if not isinstance(log, str) or not log:
                    continue

                fmt = (formatid or "unknown").strip().lower() or "unknown"
                try:
                    elo = int(rating) if rating is not None else 0
                except Exception:
                    elo = 0
                bucket = elo_bucketize(elo, args.elo_step)

                parsed = parse_log_moves(log)
                if parsed is None:
                    continue

                matches_cache[(fmt, bucket)] += 1

                for side in ("p1", "p2"):
                    won = 1 if side == parsed.winner_side else 0
                    move_uses_side = parsed.move_uses[side]

                    type_seen_side = set()
                    for move_name, uses in move_uses_side.items():
                        move_id = to_id(move_name)
                        if not move_id:
                            continue

                        canonical_name, move_type = move_type_map.get(move_id, (move_name, "Unknown"))
                        rec = move_cache[(fmt, bucket, move_id)]
                        if not rec[0]:
                            rec[0] = canonical_name
                        if rec[1] == "Unknown" and move_type != "Unknown":
                            rec[1] = move_type

                        rec[2] = int(rec[2]) + 1
                        rec[3] = int(rec[3]) + won
                        rec[4] = int(rec[4]) + int(uses)
                        rec[5] = int(rec[5]) + elo

                        type_seen_side.add(str(rec[1]))
                        type_cache[(fmt, bucket, str(rec[1]))][2] += int(uses)

                    for move_type in type_seen_side:
                        type_cache[(fmt, bucket, move_type)][0] += 1
                        type_cache[(fmt, bucket, move_type)][1] += won
                        type_cache[(fmt, bucket, move_type)][3] += elo

                seen += 1
                if seen % args.flush == 0:
                    flush()

    flush()
    rollup_all(conn)
    create_indexes(conn)
    conn.close()
    print(f"OK -> {args.out} (processed={len(files)}, elo_step={args.elo_step}, batch={args.batch_size}, flush={args.flush})")


if __name__ == "__main__":
    main()
