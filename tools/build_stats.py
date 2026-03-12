#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import glob
import os
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, DefaultDict, Dict, List, Optional, Tuple

import pyarrow.parquet as pq
from tqdm import tqdm


# Helpers

_TOID_RE = re.compile(r"[^a-z0-9]+")

_id_cache: Dict[str, str] = {}
_MERGE_TO_BASE = {"minior", "florges", "squawkabilly", "pikachu"}

def canonicalize_species(raw: str) -> tuple[str, str]:
    """Return (key, display_name), merging selected cosmetic forms into base species."""
    sp = clean_species(raw)
    if not sp:
        return ("", "")

    base_name = sp.split("-", 1)[0]
    base_id = to_id_cached(base_name)

    if base_id in _MERGE_TO_BASE:
        return (base_id, base_name)

    return (to_id_cached(sp), sp)

def to_id_cached(species: str) -> str:
    k = _id_cache.get(species)
    if k is not None:
        return k
    kk = _TOID_RE.sub("", species.lower())
    _id_cache[species] = kk
    return kk

def clean_species(raw: str) -> str:
    return raw.split(",", 1)[0].strip()

def parse_day(uploadtime: Any) -> Optional[str]:
    if uploadtime is None:
        return None
    if isinstance(uploadtime, datetime):
        return uploadtime.date().isoformat()
    if isinstance(uploadtime, date):
        return uploadtime.isoformat()
    if isinstance(uploadtime, (int, float)):
        try:
            ts = float(uploadtime)
            if ts > 10_000_000_000:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        except Exception:
            return None
    if isinstance(uploadtime, str):
        s = uploadtime.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.date().isoformat()
        except Exception:
            return s[:10] if len(s) >= 10 else None
    return None


TIMESTAMP_COLUMN_CANDIDATES = (
    "uploadtime",
    "upload_time",
    "uploaded_at",
    "created_at",
    "timestamp",
    "battle_time",
    "date",
)


def pick_timestamp_column(schema_names: list[str]) -> Optional[str]:
    available = {str(name) for name in schema_names}
    for candidate in TIMESTAMP_COLUMN_CANDIDATES:
        if candidate in available:
            return candidate
    return None

def elo_bucketize(elo: int, step: int) -> int:
    if elo < 0:
        elo = 0
    return (elo // step) * step

def side_from_ident(ident: str) -> Optional[str]:
    if not ident:
        return None
    x = ident.split(":", 1)[0].strip()  # p1a
    s = x[:2]
    return s if s in ("p1", "p2") else None

def ident_prefix(ident: str) -> str:
    return ident.split(":", 1)[0].strip()  # p1a


def token_after_prefix(tokens: List[str], prefix: str) -> Optional[str]:
    for t in tokens:
        if t.startswith(prefix):
            return t[len(prefix):].strip()
    return None

def hp_ratio(hp_str: str) -> Optional[float]:
    if not hp_str:
        return None
    s = hp_str.strip().lower()
    if "fnt" in s:
        return 0.0
    if s.endswith("%"):
        try:
            p = float(s[:-1])
            return max(0.0, min(1.0, p / 100.0))
        except Exception:
            return None
    if "/" in s:
        a, b = s.split("/", 1)
        try:
            num = float(re.sub(r"[^0-9.]", "", a))
            den = float(re.sub(r"[^0-9.]", "", b))
            if den <= 0:
                return None
            return max(0.0, min(1.0, num / den))
        except Exception:
            return None
    return None

def canonical_pair(a: str, b: str) -> Tuple[str, str]:
    return (a, b) if a < b else (b, a)


# One-pass log parsing

@dataclass
class ParsedMatch:
    teams_species: Dict[str, List[str]]
    winner_side: Optional[str]

    used: Dict[str, DefaultDict[str, int]]
    lead: Dict[str, DefaultDict[str, int]]
    kills: Dict[str, DefaultDict[str, int]]
    deaths: Dict[str, DefaultDict[str, int]]
    dmg_dealt: Dict[str, DefaultDict[str, int]]
    dmg_taken: Dict[str, DefaultDict[str, int]]
    moves: Dict[str, DefaultDict[Tuple[str, str], int]]
    items: Dict[str, DefaultDict[Tuple[str, str], int]]
    abilities: Dict[str, DefaultDict[Tuple[str, str], int]]


def parse_log_one_pass(log: str) -> Optional[ParsedMatch]:
    players: Dict[str, str] = {}
    teams_species: Dict[str, List[str]] = {"p1": [], "p2": []}
    saw_preview_poke = {"p1": False, "p2": False}
    winner_name: Optional[str] = None

    used = {"p1": defaultdict(int), "p2": defaultdict(int)}
    lead = {"p1": defaultdict(int), "p2": defaultdict(int)}
    kills = {"p1": defaultdict(int), "p2": defaultdict(int)}
    deaths = {"p1": defaultdict(int), "p2": defaultdict(int)}
    dmg_dealt = {"p1": defaultdict(int), "p2": defaultdict(int)}
    dmg_taken = {"p1": defaultdict(int), "p2": defaultdict(int)}
    moves = {"p1": defaultdict(int), "p2": defaultdict(int)}
    items = {"p1": defaultdict(int), "p2": defaultdict(int)}
    abilities = {"p1": defaultdict(int), "p2": defaultdict(int)}

    active_key: Dict[str, str] = {}
    last_hp: Dict[str, float] = {}
    first_active: Dict[str, Optional[str]] = {"p1": None, "p2": None}
    last_move_user: Optional[Tuple[str, str]] = None
    seen_abilities: Dict[str, set[Tuple[str, str]]] = {"p1": set(), "p2": set()}
    seen_moves: Dict[str, set[Tuple[str, str]]] = {"p1": set(), "p2": set()}

    for line in log.split("\n"):
        if not line or line[0] != "|":
            continue
        parts = line.split("|")
        if len(parts) < 2:
            continue
        tag = parts[1]

        if tag == "player" and len(parts) >= 4:
            side = parts[2]
            name = parts[3].strip()
            if side in ("p1", "p2") and name:
                players[side] = name

        elif tag == "poke" and len(parts) >= 4:
            side = parts[2]
            if side in ("p1", "p2"):
                saw_preview_poke[side] = True
                sp = clean_species(parts[3])
                k, nm = canonicalize_species(sp)
                if nm:
                    teams_species[side].append(nm)


        elif tag == "win" and len(parts) >= 3:
            winner_name = parts[2]

        elif tag == "turn":
            last_move_user = None

        elif tag in ("switch", "drag") and len(parts) >= 5:
            ident = parts[2]
            side = side_from_ident(ident)
            if side not in ("p1", "p2"):
                continue
            ip = ident_prefix(ident)    
            sp = clean_species(parts[3])
            k, _nm = canonicalize_species(sp)

            if not k:
                continue

            if not saw_preview_poke[side] and sp and sp not in teams_species[side]:
                teams_species[side].append(sp)

            active_key[ip] = k
            used[side][k] = 1
            if first_active[side] is None:
                first_active[side] = k
                lead[side][k] = 1

            r = hp_ratio(parts[4])
            if r is not None:
                last_hp[ip] = r

        elif tag == "move" and len(parts) >= 4:
            ident = parts[2]
            side = side_from_ident(ident)
            if side not in ("p1", "p2"):
                continue
            ip = ident_prefix(ident)
            k = active_key.get(ip)
            if not k:
                continue
            mv = parts[3].strip()
            if mv:
                kk = (k, mv)
                if kk not in seen_moves[side]:
                    seen_moves[side].add(kk)
                    moves[side][kk] += 1
            last_move_user = (side, k)

        elif tag in ("-item", "-enditem", "item") and len(parts) >= 4:
            ident = parts[2]
            side = side_from_ident(ident)
            if side not in ("p1", "p2"):
                continue
            ip = ident_prefix(ident)
            k = active_key.get(ip)
            if not k:
                continue
            it = parts[3].strip()
            if it:
                items[side][(k, it)] += 1

        elif tag == "-ability" and len(parts) >= 4:
            ident = parts[2]
            side = side_from_ident(ident)
            if side not in ("p1", "p2"):
                continue

            extra = parts[4:]
            from_move = token_after_prefix(extra, "[from] move:")
            from_ability = token_after_prefix(extra, "[from] ability:")
            of_ident = token_after_prefix(extra, "[of] ")

            if from_move:
                # Ability changed by move (e.g. Entrainment), not a native reveal.
                continue

            target_ident = ident
            if from_ability:
                if from_ability == "Trace" and of_ident:
                    # Trace reveals the target's ability; attribute to [of].
                    target_ident = of_ident
                    side = side_from_ident(target_ident)
                    if side not in ("p1", "p2"):
                        continue
                else:
                    # Other ability-sourced changes are usually temporary/replaced states.
                    continue

            ip = ident_prefix(target_ident)
            k = active_key.get(ip)
            if not k and ":" in target_ident:
                sp = clean_species(target_ident.split(":", 1)[1])
                k, _ = canonicalize_species(sp)
            if not k:
                continue

            ab = parts[3].strip()
            if not ab:
                continue

            kk = (k, ab)
            if kk in seen_abilities[side]:
                continue
            seen_abilities[side].add(kk)
            abilities[side][kk] += 1

        elif tag == "-damage" and len(parts) >= 4:
            ident = parts[2]
            side = side_from_ident(ident)
            if side not in ("p1", "p2"):
                continue
            ip = ident_prefix(ident)
            target_k = active_key.get(ip)
            if not target_k:
                continue

            newr = hp_ratio(parts[3])
            if newr is None:
                continue
            oldr = last_hp.get(ip)
            last_hp[ip] = newr
            if oldr is None:
                continue

            delta = newr - oldr
            if delta >= 0:
                continue
            dmg = int(round((-delta) * 10000))

            dmg_taken[side][target_k] += dmg

            if last_move_user is not None:
                atk_side, atk_k = last_move_user
                if atk_side != side:
                    dmg_dealt[atk_side][atk_k] += dmg

        elif tag == "faint" and len(parts) >= 3:
            ident = parts[2]
            side = side_from_ident(ident)
            if side not in ("p1", "p2"):
                continue
            ip = ident_prefix(ident)
            target_k = active_key.get(ip)
            if target_k:
                deaths[side][target_k] += 1

            if last_move_user is not None:
                atk_side, atk_k = last_move_user
                if atk_side != side:
                    kills[atk_side][atk_k] += 1

    winner_side: Optional[str] = None
    if winner_name:
        if players.get("p1") == winner_name:
            winner_side = "p1"
        elif players.get("p2") == winner_name:
            winner_side = "p2"

    if winner_side not in ("p1", "p2"):
        return None
    if not teams_species["p1"] or not teams_species["p2"]:
        return None

    return ParsedMatch(
        teams_species=teams_species,
        winner_side=winner_side,
        used=used, lead=lead, kills=kills, deaths=deaths,
        dmg_dealt=dmg_dealt, dmg_taken=dmg_taken,
        moves=moves, items=items, abilities=abilities,
    )


# SQLite schema (indexes created later)

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
        CREATE TABLE IF NOT EXISTS pokemon_bucket (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          key TEXT NOT NULL,
          name TEXT NOT NULL,
          games INTEGER NOT NULL,
          wins INTEGER NOT NULL,
          sum_elo INTEGER NOT NULL,
          brought INTEGER NOT NULL,
          used INTEGER NOT NULL,
          leads INTEGER NOT NULL,
          kills INTEGER NOT NULL,
          deaths INTEGER NOT NULL,
          dmg_dealt INTEGER NOT NULL,
          dmg_taken INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket, key)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS matches_bucket (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          matches INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS mates_bucket (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          a TEXT NOT NULL,
          b TEXT NOT NULL,
          games INTEGER NOT NULL,
          wins INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket, a, b)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS vs_bucket (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          a TEXT NOT NULL,
          b TEXT NOT NULL,
          games INTEGER NOT NULL,
          wins INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket, a, b)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS pokemon_day (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          day TEXT NOT NULL,
          key TEXT NOT NULL,
          name TEXT NOT NULL,
          games INTEGER NOT NULL,
          wins INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket, day, key)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS day_totals (
          formatid TEXT NOT NULL,
          elo_bucket INTEGER NOT NULL,
          day TEXT NOT NULL,
          matches INTEGER NOT NULL,
          games_sum INTEGER NOT NULL,
          PRIMARY KEY (formatid, elo_bucket, day)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS pokemon_moves (
          formatid TEXT NOT NULL,
          key TEXT NOT NULL,
          move TEXT NOT NULL,
          uses INTEGER NOT NULL,
          PRIMARY KEY (formatid, key, move)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS pokemon_items (
          formatid TEXT NOT NULL,
          key TEXT NOT NULL,
          item TEXT NOT NULL,
          uses INTEGER NOT NULL,
          PRIMARY KEY (formatid, key, item)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS pokemon_abilities (
          formatid TEXT NOT NULL,
          key TEXT NOT NULL,
          ability TEXT NOT NULL,
          uses INTEGER NOT NULL,
          PRIMARY KEY (formatid, key, ability)
        ) WITHOUT ROWID;
        """
    )
    conn.commit()

def create_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_pokemon_bucket_fmt_elo ON pokemon_bucket(formatid, elo_bucket);
        CREATE INDEX IF NOT EXISTS idx_pokemon_bucket_fmt_key ON pokemon_bucket(formatid, key);
        CREATE INDEX IF NOT EXISTS idx_pokemon_day_fmt_elo_key ON pokemon_day(formatid, elo_bucket, key, day);
        CREATE INDEX IF NOT EXISTS idx_day_totals_fmt_elo_day ON day_totals(formatid, elo_bucket, day);
        CREATE INDEX IF NOT EXISTS idx_vs_bucket_fmt_a ON vs_bucket(formatid, a);
        """
    )
    conn.commit()


# SQL rollup for the "all" format bucket

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

    conn.execute("DELETE FROM pokemon_bucket WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO pokemon_bucket(formatid, elo_bucket, key, name, games, wins, sum_elo, brought, used, leads, kills, deaths, dmg_dealt, dmg_taken)
        SELECT 'all', elo_bucket, key, MIN(name),
               SUM(games), SUM(wins), SUM(sum_elo),
               SUM(brought), SUM(used), SUM(leads),
               SUM(kills), SUM(deaths),
               SUM(dmg_dealt), SUM(dmg_taken)
        FROM pokemon_bucket
        WHERE formatid <> 'all'
        GROUP BY elo_bucket, key
        """
    )

    conn.execute("DELETE FROM mates_bucket WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO mates_bucket(formatid, elo_bucket, a, b, games, wins)
        SELECT 'all', elo_bucket, a, b, SUM(games), SUM(wins)
        FROM mates_bucket
        WHERE formatid <> 'all'
        GROUP BY elo_bucket, a, b
        """
    )

    conn.execute("DELETE FROM vs_bucket WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO vs_bucket(formatid, elo_bucket, a, b, games, wins)
        SELECT 'all', elo_bucket, a, b, SUM(games), SUM(wins)
        FROM vs_bucket
        WHERE formatid <> 'all'
        GROUP BY elo_bucket, a, b
        """
    )

    conn.execute("DELETE FROM pokemon_day WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO pokemon_day(formatid, elo_bucket, day, key, name, games, wins)
        SELECT 'all', elo_bucket, day, key, MIN(name), SUM(games), SUM(wins)
        FROM pokemon_day
        WHERE formatid <> 'all'
        GROUP BY elo_bucket, day, key
        """
    )

    conn.execute("DELETE FROM day_totals WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO day_totals(formatid, elo_bucket, day, matches, games_sum)
        SELECT 'all', elo_bucket, day, SUM(matches), SUM(games_sum)
        FROM day_totals
        WHERE formatid <> 'all'
        GROUP BY elo_bucket, day
        """
    )

    conn.execute("DELETE FROM pokemon_moves WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO pokemon_moves(formatid, key, move, uses)
        SELECT 'all', key, move, SUM(uses)
        FROM pokemon_moves
        WHERE formatid <> 'all'
        GROUP BY key, move
        """
    )

    conn.execute("DELETE FROM pokemon_items WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO pokemon_items(formatid, key, item, uses)
        SELECT 'all', key, item, SUM(uses)
        FROM pokemon_items
        WHERE formatid <> 'all'
        GROUP BY key, item
        """
    )

    conn.execute("DELETE FROM pokemon_abilities WHERE formatid='all'")
    conn.execute(
        """
        INSERT INTO pokemon_abilities(formatid, key, ability, uses)
        SELECT 'all', key, ability, SUM(uses)
        FROM pokemon_abilities
        WHERE formatid <> 'all'
        GROUP BY key, ability
        """
    )

    conn.execute("COMMIT")


# Main build

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", default="pokemon-showdown-replays")
    ap.add_argument("--glob", default="*.parquet")
    ap.add_argument("--out", default="stats.sqlite")
    ap.add_argument("--elo_step", type=int, default=100)
    ap.add_argument("--batch_size", type=int, default=8192)
    ap.add_argument("--flush", type=int, default=100000)
    ap.add_argument("--fast_sqlite", action="store_true", default=True, help="Use faster SQLite pragmas (synchronous=OFF)")

    ap.add_argument("--max_files", type=int, default=0, help="Process only N parquet files (0 = all)")
    ap.add_argument("--skip_files", type=int, default=0, help="Skip K parquet files before processing")

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

    if os.path.exists(args.out):
        os.remove(args.out)

    conn = sqlite3.connect(args.out)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn, fast_sqlite=args.fast_sqlite)

    poke_cache: Dict[Tuple[str, int, str], List[int | str]] = {}
    matches_cache: DefaultDict[Tuple[str, int], int] = defaultdict(int)
    mates_cache: DefaultDict[Tuple[str, int, str, str], List[int]] = defaultdict(lambda: [0, 0])
    vs_cache: DefaultDict[Tuple[str, int, str, str], List[int]] = defaultdict(lambda: [0, 0])
    day_cache: DefaultDict[Tuple[str, int, str, str], List[int | str]] = defaultdict(lambda: ["", 0, 0])
    day_tot_cache: DefaultDict[Tuple[str, int, str], List[int]] = defaultdict(lambda: [0, 0])
    moves_cache: DefaultDict[Tuple[str, str, str], int] = defaultdict(int)
    items_cache: DefaultDict[Tuple[str, str, str], int] = defaultdict(int)
    abilities_cache: DefaultDict[Tuple[str, str, str], int] = defaultdict(int)

    def bump_pokemon(fmt: str, bucket: int, key: str, name: str,
                     games: int, wins: int, sum_elo: int,
                     brought: int, used: int, leads: int,
                     kills: int, deaths: int, dmg_dealt: int, dmg_taken: int) -> None:
        kk = (fmt, bucket, key)
        cur = poke_cache.get(kk)
        if cur is None:
            poke_cache[kk] = [name, games, wins, sum_elo, brought, used, leads, kills, deaths, dmg_dealt, dmg_taken]
        else:
            if isinstance(cur[0], str) and len(name) > len(cur[0]):
                cur[0] = name
            cur[1] = int(cur[1]) + games
            cur[2] = int(cur[2]) + wins
            cur[3] = int(cur[3]) + sum_elo
            cur[4] = int(cur[4]) + brought
            cur[5] = int(cur[5]) + used
            cur[6] = int(cur[6]) + leads
            cur[7] = int(cur[7]) + kills
            cur[8] = int(cur[8]) + deaths
            cur[9] = int(cur[9]) + dmg_dealt
            cur[10] = int(cur[10]) + dmg_taken

    def flush() -> None:
        if not (poke_cache or matches_cache or mates_cache or vs_cache or day_cache or day_tot_cache or moves_cache or items_cache or abilities_cache):
            return

        conn.execute("BEGIN")

        conn.executemany(
            """
            INSERT INTO pokemon_bucket(formatid, elo_bucket, key, name, games, wins, sum_elo, brought, used, leads, kills, deaths, dmg_dealt, dmg_taken)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(formatid, elo_bucket, key) DO UPDATE SET
              name=excluded.name,
              games=games+excluded.games,
              wins=wins+excluded.wins,
              sum_elo=sum_elo+excluded.sum_elo,
              brought=brought+excluded.brought,
              used=used+excluded.used,
              leads=leads+excluded.leads,
              kills=kills+excluded.kills,
              deaths=deaths+excluded.deaths,
              dmg_dealt=dmg_dealt+excluded.dmg_dealt,
              dmg_taken=dmg_taken+excluded.dmg_taken
            """,
            [
                (fmt, b, key, v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10])
                for (fmt, b, key), v in poke_cache.items()
            ],
        )

        conn.executemany(
            """
            INSERT INTO matches_bucket(formatid, elo_bucket, matches)
            VALUES (?,?,?)
            ON CONFLICT(formatid, elo_bucket) DO UPDATE SET
              matches=matches+excluded.matches
            """,
            [(fmt, b, m) for (fmt, b), m in matches_cache.items()],
        )

        conn.executemany(
            """
            INSERT INTO mates_bucket(formatid, elo_bucket, a, b, games, wins)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(formatid, elo_bucket, a, b) DO UPDATE SET
              games=games+excluded.games,
              wins=wins+excluded.wins
            """,
            [(fmt, b, a, c, v[0], v[1]) for (fmt, b, a, c), v in mates_cache.items()],
        )

        conn.executemany(
            """
            INSERT INTO vs_bucket(formatid, elo_bucket, a, b, games, wins)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(formatid, elo_bucket, a, b) DO UPDATE SET
              games=games+excluded.games,
              wins=wins+excluded.wins
            """,
            [(fmt, b, a, opp, v[0], v[1]) for (fmt, b, a, opp), v in vs_cache.items()],
        )

        conn.executemany(
            """
            INSERT INTO pokemon_day(formatid, elo_bucket, day, key, name, games, wins)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(formatid, elo_bucket, day, key) DO UPDATE SET
              name=excluded.name,
              games=games+excluded.games,
              wins=wins+excluded.wins
            """,
            [(fmt, bucket, day, key, v[0], v[1], v[2]) for (fmt, bucket, day, key), v in day_cache.items()],
        )

        conn.executemany(
            """
            INSERT INTO day_totals(formatid, elo_bucket, day, matches, games_sum)
            VALUES (?,?,?,?,?)
            ON CONFLICT(formatid, elo_bucket, day) DO UPDATE SET
              matches=matches+excluded.matches,
              games_sum=games_sum+excluded.games_sum
            """,
            [(fmt, bucket, day, v[0], v[1]) for (fmt, bucket, day), v in day_tot_cache.items()],
        )

        conn.executemany(
            """
            INSERT INTO pokemon_moves(formatid, key, move, uses)
            VALUES (?,?,?,?)
            ON CONFLICT(formatid, key, move) DO UPDATE SET
              uses=uses+excluded.uses
            """,
            [(fmt, key, mv, uses) for (fmt, key, mv), uses in moves_cache.items()],
        )

        conn.executemany(
            """
            INSERT INTO pokemon_items(formatid, key, item, uses)
            VALUES (?,?,?,?)
            ON CONFLICT(formatid, key, item) DO UPDATE SET
              uses=uses+excluded.uses
            """,
            [(fmt, key, it, uses) for (fmt, key, it), uses in items_cache.items()],
        )

        conn.executemany(
            """
            INSERT INTO pokemon_abilities(formatid, key, ability, uses)
            VALUES (?,?,?,?)
            ON CONFLICT(formatid, key, ability) DO UPDATE SET
              uses=uses+excluded.uses
            """,
            [(fmt, key, ab, uses) for (fmt, key, ab), uses in abilities_cache.items()],
        )

        conn.execute("COMMIT")

        poke_cache.clear()
        matches_cache.clear()
        mates_cache.clear()
        vs_cache.clear()
        day_cache.clear()
        day_tot_cache.clear()
        moves_cache.clear()
        items_cache.clear()
        abilities_cache.clear()

    seen = 0

    for fp in tqdm(files, desc="Parquet files"):
        pf = pq.ParquetFile(fp)
        timestamp_col = pick_timestamp_column(pf.schema.names)
        columns = ["log", "rating", "formatid"]
        if timestamp_col:
            columns.append(timestamp_col)
        for batch in pf.iter_batches(columns=columns, batch_size=args.batch_size):
            logs = batch.column(0).to_pylist()
            ratings = batch.column(1).to_pylist()
            fmts = batch.column(2).to_pylist()
            upt = batch.column(3).to_pylist() if timestamp_col else [None] * len(logs)

            for log, rating, formatid, uploadtime in zip(logs, ratings, fmts, upt):
                if not isinstance(log, str) or not log:
                    continue

                fmt = (formatid or "unknown").strip().lower() or "unknown"
                day = parse_day(uploadtime)

                try:
                    elo = int(rating) if rating is not None else 0
                except Exception:
                    elo = 0
                bucket = elo_bucketize(elo, args.elo_step)

                pm = parse_log_one_pass(log)
                if pm is None:
                    continue

                matches_cache[(fmt, bucket)] += 1

                if day:
                    day_tot_cache[(fmt, bucket, day)][0] += 1
                    day_tot_cache[(fmt, bucket, day)][1] += (len(pm.teams_species["p1"]) + len(pm.teams_species["p2"]))

                for side in ("p1", "p2"):
                    won = 1 if side == pm.winner_side else 0
                    team_sp = pm.teams_species[side]
                    opp_sp = pm.teams_species["p2" if side == "p1" else "p1"]

                    brought_counts: DefaultDict[str, int] = defaultdict(int)
                    name_by_key: Dict[str, str] = {}
                    for sp in team_sp:
                        k, nm = canonicalize_species(sp)
                        if not k:
                            continue
                        brought_counts[k] += 1
                        name_by_key.setdefault(k, nm or sp)


                    used_side = pm.used[side]
                    lead_side = pm.lead[side]
                    kills_side = pm.kills[side]
                    deaths_side = pm.deaths[side]
                    dd_side = pm.dmg_dealt[side]
                    dt_side = pm.dmg_taken[side]

                    for k, bc in brought_counts.items():
                        name = name_by_key.get(k, k)
                        used = 1 if used_side.get(k, 0) else 0
                        leads = 1 if lead_side.get(k, 0) else 0
                        kkills = int(kills_side.get(k, 0))
                        ddeaths = int(deaths_side.get(k, 0))
                        dmgd = int(dd_side.get(k, 0))
                        dmgt = int(dt_side.get(k, 0))

                        bump_pokemon(
                            fmt=fmt, bucket=bucket, key=k, name=name,
                            games=bc,
                            wins=(bc if won else 0),
                            sum_elo=(bc * elo),
                            brought=bc,
                            used=used, leads=leads,
                            kills=kkills, deaths=ddeaths,
                            dmg_dealt=dmgd, dmg_taken=dmgt,
                        )

                        if day:
                            dk = (fmt, bucket, day, k)
                            if not day_cache[dk][0]:
                                day_cache[dk][0] = name
                            day_cache[dk][1] = int(day_cache[dk][1]) + bc
                            day_cache[dk][2] = int(day_cache[dk][2]) + (bc if won else 0)

                    team_keys = sorted(set(canonicalize_species(s)[0] for s in team_sp if canonicalize_species(s)[0]))
                    opp_keys  = sorted(set(canonicalize_species(s)[0] for s in opp_sp  if canonicalize_species(s)[0]))

                    for i in range(len(team_keys)):
                        for j in range(i + 1, len(team_keys)):
                            a, b = canonical_pair(team_keys[i], team_keys[j])
                            mates_cache[(fmt, bucket, a, b)][0] += 1
                            mates_cache[(fmt, bucket, a, b)][1] += (1 if won else 0)

                    for a in team_keys:
                        for b in opp_keys:
                            vs_cache[(fmt, bucket, a, b)][0] += 1
                            vs_cache[(fmt, bucket, a, b)][1] += (1 if won else 0)

                    for (k, mv), uses in pm.moves[side].items():
                        moves_cache[(fmt, k, mv)] += uses
                    for (k, it), uses in pm.items[side].items():
                        items_cache[(fmt, k, it)] += uses
                    for (k, ab), uses in pm.abilities[side].items():
                        abilities_cache[(fmt, k, ab)] += uses

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
