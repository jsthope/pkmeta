#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import unicodedata
from typing import Any, Dict, List, Set
from urllib.request import Request, urlopen

from flask import Flask, Response, jsonify, redirect, render_template, request
from werkzeug.middleware.proxy_fix import ProxyFix

SPRITE_HOME = "https://play.pokemonshowdown.com/sprites/home/"
SPRITE_GEN5 = "https://play.pokemonshowdown.com/sprites/gen5/"
SPRITE_ANI  = "https://play.pokemonshowdown.com/sprites/ani/"

POKEDEX_URLS = (
    "https://play.pokemonshowdown.com/data/pokedex.json",
    "https://raw.githubusercontent.com/smogon/pokemon-showdown/master/data/pokedex.json",
)
MOVES_URLS = (
    "https://play.pokemonshowdown.com/data/moves.json",
    "https://raw.githubusercontent.com/smogon/pokemon-showdown/master/data/moves.json",
)
_TOID_RE = re.compile(r"[^a-z0-9]+")
_TYPE_ORDER = [
    "Normal", "Fire", "Water", "Electric", "Grass", "Ice", "Fighting", "Poison", "Ground",
    "Flying", "Psychic", "Bug", "Rock", "Ghost", "Dragon", "Dark", "Steel", "Fairy", "Unknown",
]
_TYPE_ORDER_INDEX = {t: i for i, t in enumerate(_TYPE_ORDER)}
_POKEDEX_TYPE_CACHE: Dict[str, List[str]] | None = None
_POKEDEX_ABILITIES_CACHE: Dict[str, List[str]] | None = None
_POKEDEX_BASE_STATS_CACHE: Dict[str, Dict[str, int]] | None = None
_MOVE_TYPE_CACHE: Dict[str, str] | None = None


# Sprite URL helpers

def _dashify(name: str) -> str:
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = s.replace("♀", "-f").replace("♂", "-m")
    s = s.replace("%", "")
    s = s.replace(":", "")
    s = s.replace(".", "")
    s = s.replace("’", "'")
    s = s.replace("'", "")
    s = s.replace(" ", "-")
    s = re.sub(r"[^a-z0-9\-]", "", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s

def _keep_only_first_dash(slug: str) -> str:
    # Keep only the first dash-separated segment break for sprite fallback.
    if slug.count("-") <= 1:
        return slug
    head, rest = slug.split("-", 1)
    return head + "-" + rest.replace("-", "")

def sprite_urls(key: str, name: str) -> List[str]:
    slug = _dashify(name)
    slug2 = _keep_only_first_dash(slug)
    out: List[str] = []

    def add(u: str) -> None:
        if u not in out:
            out.append(u)

    # Animated sprites may use dashed or compact forms.
    add(f"{SPRITE_ANI}{slug}.gif")
    add(f"{SPRITE_ANI}{slug2}.gif")
    add(f"{SPRITE_ANI}{key}.gif")

    # Static sprite fallbacks.
    for base in (SPRITE_HOME, SPRITE_GEN5):
        add(f"{base}{slug}.png")
        add(f"{base}{slug2}.png")
        add(f"{base}{key}.png")

    return out


# Ranking helpers for low-sample stability

def wilson_lower_bound(w: int, n: int, z: float = 1.96) -> float:
    if n <= 0:
        return 0.0
    phat = w / n
    denom = 1 + (z*z)/n
    centre = phat + (z*z)/(2*n)
    margin = z * math.sqrt((phat*(1-phat) + (z*z)/(4*n)) / n)
    return (centre - margin) / denom

def wilson_upper_bound(w: int, n: int, z: float = 1.96) -> float:
    if n <= 0:
        return 1.0
    phat = w / n
    denom = 1 + (z*z)/n
    centre = phat + (z*z)/(2*n)
    margin = z * math.sqrt((phat*(1-phat) + (z*z)/(4*n)) / n)
    return (centre + margin) / denom


# SQLite helpers

def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def clamp_int(x: Any, lo: int, hi: int) -> int:
    try:
        v = int(x)
    except Exception:
        v = lo
    return max(lo, min(hi, v))


def _to_id(s: str) -> str:
    return _TOID_RE.sub("", (s or "").lower())


def _normalize_type(t: str) -> str:
    tt = (t or "").strip()
    if not tt:
        return ""
    return tt[0].upper() + tt[1:].lower()


def _parse_types_param(raw: str) -> Set[str]:
    out: Set[str] = set()
    for x in (raw or "").split(","):
        t = _normalize_type(x)
        if t:
            out.add(t)
    return out


def _read_json_url(url: str) -> Any:
    req = Request(url, headers={"User-Agent": "pkmeta/1.0"})
    with urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def load_pokedex_type_map(local_json_path: str = "") -> Dict[str, List[str]]:
    global _POKEDEX_TYPE_CACHE
    if _POKEDEX_TYPE_CACHE is not None:
        return _POKEDEX_TYPE_CACHE

    data: Any = None

    if local_json_path:
        try:
            with open(local_json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = None

    if data is None:
        for url in POKEDEX_URLS:
            try:
                data = _read_json_url(url)
                break
            except Exception:
                data = None

    out: Dict[str, List[str]] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            types_raw = v.get("types")
            if not isinstance(types_raw, list):
                continue
            types = [_normalize_type(str(t)) for t in types_raw if _normalize_type(str(t))]
            if not types:
                continue

            kid = _to_id(str(k))
            if kid:
                out[kid] = types

            nm = str(v.get("name") or "")
            nid = _to_id(nm)
            if nid and nid not in out:
                out[nid] = types

    _POKEDEX_TYPE_CACHE = out
    return out


def load_pokedex_abilities_map(local_json_path: str = "") -> Dict[str, List[str]]:
    global _POKEDEX_ABILITIES_CACHE
    if _POKEDEX_ABILITIES_CACHE is not None:
        return _POKEDEX_ABILITIES_CACHE

    data: Any = None

    if local_json_path:
        try:
            with open(local_json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = None

    if data is None:
        for url in POKEDEX_URLS:
            try:
                data = _read_json_url(url)
                break
            except Exception:
                data = None

    out: Dict[str, List[str]] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            abs_raw = v.get("abilities")
            if not isinstance(abs_raw, dict):
                continue
            abilities = [str(x).strip() for x in abs_raw.values() if str(x).strip()]
            if not abilities:
                continue

            kid = _to_id(str(k))
            if kid:
                out[kid] = abilities

            nm = str(v.get("name") or "")
            nid = _to_id(nm)
            if nid and nid not in out:
                out[nid] = abilities

    _POKEDEX_ABILITIES_CACHE = out
    return out


def load_pokedex_base_stats_map(local_json_path: str = "") -> Dict[str, Dict[str, int]]:
    global _POKEDEX_BASE_STATS_CACHE
    if _POKEDEX_BASE_STATS_CACHE is not None:
        return _POKEDEX_BASE_STATS_CACHE

    data: Any = None

    if local_json_path:
        try:
            with open(local_json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = None

    if data is None:
        for url in POKEDEX_URLS:
            try:
                data = _read_json_url(url)
                break
            except Exception:
                data = None

    out: Dict[str, Dict[str, int]] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            bs_raw = v.get("baseStats")
            if not isinstance(bs_raw, dict):
                continue

            bs = {
                "hp": int(bs_raw.get("hp", 0) or 0),
                "atk": int(bs_raw.get("atk", 0) or 0),
                "def": int(bs_raw.get("def", 0) or 0),
                "spa": int(bs_raw.get("spa", 0) or 0),
                "spd": int(bs_raw.get("spd", 0) or 0),
                "spe": int(bs_raw.get("spe", 0) or 0),
            }

            kid = _to_id(str(k))
            if kid:
                out[kid] = bs

            nm = str(v.get("name") or "")
            nid = _to_id(nm)
            if nid and nid not in out:
                out[nid] = bs

    _POKEDEX_BASE_STATS_CACHE = out
    return out


def load_move_type_map(local_json_path: str = "") -> Dict[str, str]:
    global _MOVE_TYPE_CACHE
    if _MOVE_TYPE_CACHE is not None:
        return _MOVE_TYPE_CACHE

    data: Any = None

    if local_json_path:
        try:
            with open(local_json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = None

    if data is None:
        for url in MOVES_URLS:
            try:
                data = _read_json_url(url)
                break
            except Exception:
                data = None

    out: Dict[str, str] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            mtype = _normalize_type(str(v.get("type") or ""))
            if not mtype:
                continue

            kid = _to_id(str(k))
            if kid:
                out[kid] = mtype

            nm = str(v.get("name") or "")
            nid = _to_id(nm)
            if nid and nid not in out:
                out[nid] = mtype

    _MOVE_TYPE_CACHE = out
    return out


# Flask app

def make_app(
    db_path: str,
    attacks_db_path: str = "attacks.sqlite",
    min_pair_games_default: int = 3000,
    min_vs_games_default: int = 3000,
) -> Flask:
    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

    @app.before_request
    def force_https_and_canonical_host():
        host = (request.host.split(":", 1)[0] or "").lower()
        if host not in {"pkmeta.net", "www.pkmeta.net"}:
            return None

        if host == "www.pkmeta.net":
            return redirect(f"https://pkmeta.net{request.full_path.rstrip('?')}", code=301)

        if not request.is_secure:
            return redirect(f"https://pkmeta.net{request.full_path.rstrip('?')}", code=301)

        return None

    @app.after_request
    def add_security_headers(resp: Response):
        host = (request.host.split(":", 1)[0] or "").lower()
        if host == "pkmeta.net" and request.is_secure:
            resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
        return resp

    @app.get("/")
    def index() -> str:
        return render_template("index.html")

    @app.get("/robots.txt")
    def robots_txt() -> Response:
        body = "\n".join(
            [
                "User-agent: *",
                "Allow: /",
                "Sitemap: https://pkmeta.net/sitemap.xml",
            ]
        )
        return Response(body + "\n", mimetype="text/plain")

    @app.get("/sitemap.xml")
    def sitemap_xml() -> Response:
        xml = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">
  <url>
    <loc>https://pkmeta.net/</loc>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>
"""
        return Response(xml, mimetype="application/xml")

    @app.get("/api/formats")
    def api_formats():
        conn = get_conn(db_path)
        rows = conn.execute("SELECT DISTINCT formatid FROM matches_bucket ORDER BY formatid").fetchall()
        conn.close()
        formats = [r["formatid"] for r in rows]
        return jsonify({"formats": formats})

    @app.get("/api/elo_bounds")
    def api_elo_bounds():
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        conn = get_conn(db_path)
        row = conn.execute(
            "SELECT MIN(elo_bucket) AS mn, MAX(elo_bucket) AS mx FROM matches_bucket WHERE formatid=?",
            (formatid,),
        ).fetchone()
        conn.close()
        mn = int(row["mn"] if row and row["mn"] is not None else 0)
        mx = int(row["mx"] if row and row["mx"] is not None else 2000)
        return jsonify({"min": mn, "max": mx, "step": 100})

    @app.get("/api/types")
    def api_types():
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        elo_min = clamp_int(request.args.get("elo_min", 0), 0, 10000)
        elo_max = clamp_int(request.args.get("elo_max", 10000), 0, 10000)
        if elo_min > elo_max:
            elo_min, elo_max = elo_max, elo_min

        type_counts: Dict[str, int] = {}
        poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))

        conn = get_conn(db_path)
        rows = conn.execute(
            """
            SELECT key, SUM(games) AS games
            FROM pokemon_bucket
            WHERE formatid=? AND elo_bucket BETWEEN ? AND ?
            GROUP BY key
            """,
            (formatid, elo_min, elo_max),
        ).fetchall()
        conn.close()

        for r in rows:
            games = int(r["games"])
            types = poke_type_map.get(str(r["key"]), [])
            for t in types:
                type_counts[t] = type_counts.get(t, 0) + games

        conn2 = get_conn(attacks_db_path)
        try:
            rows2 = conn2.execute(
                """
                SELECT move_type, SUM(games) AS games
                FROM move_bucket
                WHERE formatid=? AND elo_bucket BETWEEN ? AND ?
                GROUP BY move_type
                """,
                (formatid, elo_min, elo_max),
            ).fetchall()
            for r in rows2:
                t = _normalize_type(str(r["move_type"]))
                if not t:
                    continue
                type_counts[t] = type_counts.get(t, 0) + int(r["games"])
        except sqlite3.OperationalError:
            pass
        finally:
            conn2.close()

        types_sorted = sorted(
            type_counts.items(),
            key=lambda kv: (-kv[1], _TYPE_ORDER_INDEX.get(kv[0], 999), kv[0]),
        )
        return jsonify({"types": [t for t, _ in types_sorted]})

    @app.get("/api/pokemon")
    def api_pokemon():
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        q = (request.args.get("q", "") or "").strip().lower()
        selected_types = _parse_types_param(request.args.get("types", "") or "")
        sort = (request.args.get("sort", "winrate") or "winrate").strip()
        order = (request.args.get("order", "desc") or "desc").strip().lower()
        min_games = clamp_int(request.args.get("min_games", 5000), 0, 10_000_000)
        limit = clamp_int(request.args.get("limit", 200), 10, 2000)

        elo_min = clamp_int(request.args.get("elo_min", 0), 0, 10000)
        elo_max = clamp_int(request.args.get("elo_max", 10000), 0, 10000)
        if elo_min > elo_max:
            elo_min, elo_max = elo_max, elo_min

        conn = get_conn(db_path)

        denom_row = conn.execute(
            "SELECT COALESCE(SUM(games),0) AS s FROM pokemon_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (formatid, elo_min, elo_max),
        ).fetchone()
        total_games_sum = int(denom_row["s"]) if denom_row else 0
        if total_games_sum <= 0:
            total_games_sum = 1

        matches_row = conn.execute(
            "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (formatid, elo_min, elo_max),
        ).fetchone()
        matches = int(matches_row["m"]) if matches_row else 0

        rows = conn.execute(
            """
            SELECT
              key,
              MIN(name) AS name,
              SUM(games) AS games,
              SUM(wins) AS wins,
              SUM(sum_elo) AS sum_elo,
              SUM(used) AS used,
              SUM(leads) AS leads,
              SUM(kills) AS kills,
              SUM(deaths) AS deaths,
              SUM(dmg_dealt) AS dmg_dealt,
              SUM(dmg_taken) AS dmg_taken
            FROM pokemon_bucket
            WHERE formatid=? AND elo_bucket BETWEEN ? AND ?
            GROUP BY key
            HAVING SUM(games) >= ?
            """,
            (formatid, elo_min, elo_max, min_games),
        ).fetchall()

        conn.close()
        poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))

        items: List[Dict[str, Any]] = []
        for r in rows:
            games = int(r["games"])
            wins = int(r["wins"])
            used = int(r["used"])
            leads = int(r["leads"])
            sum_elo = int(r["sum_elo"])

            winrate = wins / games if games else 0.0
            denom = max(1, 2 * matches)
            popularity = games / denom

            avg_elo = (sum_elo / games) if games else 0.0
            lead_rate = (leads / used) if used else 0.0

            kills = int(r["kills"])
            deaths = int(r["deaths"])
            kd = (kills / max(1, deaths))

            dmg_dealt = int(r["dmg_dealt"]) / 100.0
            dmg_taken = int(r["dmg_taken"]) / 100.0

            key = str(r["key"])
            name = str(r["name"])
            ptypes = poke_type_map.get(key, [])
            if selected_types and not selected_types.intersection(ptypes):
                continue

            items.append(
                {
                    "key": key,
                    "name": name,
                    "games": games,
                    "wins": wins,
                    "winrate": winrate,
                    "popularity": popularity,
                    "avg_elo": avg_elo,
                    "lead_rate": lead_rate,
                    "kd": kd,
                    "dmg_dealt": dmg_dealt,
                    "dmg_taken": dmg_taken,
                    "types": ptypes,
                    "sprite_urls": sprite_urls(key, name),
                }
            )

        reverse = (order != "asc")

        if sort == "popularity":
            items.sort(key=lambda x: x["popularity"], reverse=reverse)
        elif sort == "games":
            items.sort(key=lambda x: x["games"], reverse=reverse)
        elif sort == "avg_elo":
            items.sort(key=lambda x: x["avg_elo"], reverse=reverse)
        elif sort == "lead_rate":
            items.sort(key=lambda x: x["lead_rate"], reverse=reverse)
        elif sort == "kd":
            items.sort(key=lambda x: x["kd"], reverse=reverse)
        elif sort == "dmg_dealt":
            items.sort(key=lambda x: x["dmg_dealt"], reverse=reverse)
        elif sort == "dmg_taken":
            items.sort(key=lambda x: x["dmg_taken"], reverse=reverse)
        elif sort == "name":
            items.sort(key=lambda x: x["name"].lower(), reverse=reverse)
        elif sort == "types":
            items.sort(key=lambda x: "/".join(x.get("types", [])).lower(), reverse=reverse)
        else:
            items.sort(key=lambda x: x["winrate"], reverse=reverse)

        return jsonify(
            {
                "formatid": formatid,
                "elo_min": elo_min,
                "elo_max": elo_max,
                "items": items[:limit],
                "meta": {"matches": matches, "total_games_sum": total_games_sum},
            }
        )

    @app.get("/api/pokemon/<key>/detail")
    def api_pokemon_detail(key: str):
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        elo_min = clamp_int(request.args.get("elo_min", 0), 0, 10000)
        elo_max = clamp_int(request.args.get("elo_max", 10000), 0, 10000)
        if elo_min > elo_max:
            elo_min, elo_max = elo_max, elo_min

        min_pair_games = clamp_int(request.args.get("min_pair_games", min_pair_games_default), 0, 10_000_000)
        min_vs_games = clamp_int(request.args.get("min_vs_games", min_vs_games_default), 0, 10_000_000)

        conn = get_conn(db_path)

        base = conn.execute(
            """
            SELECT
              MIN(name) AS name,
              SUM(games) AS games,
              SUM(wins) AS wins,
              SUM(sum_elo) AS sum_elo,
              SUM(used) AS used,
              SUM(leads) AS leads,
              SUM(kills) AS kills,
              SUM(deaths) AS deaths,
              SUM(dmg_dealt) AS dmg_dealt,
              SUM(dmg_taken) AS dmg_taken
            FROM pokemon_bucket
            WHERE formatid=? AND elo_bucket BETWEEN ? AND ? AND key=?
            """,
            (formatid, elo_min, elo_max, key),
        ).fetchone()

        if not base or base["games"] is None:
            conn.close()
            return jsonify({"error": "not found"}), 404

        name = str(base["name"])
        games = int(base["games"])
        wins = int(base["wins"])
        sum_elo = int(base["sum_elo"])
        used = int(base["used"])
        leads = int(base["leads"])
        kills = int(base["kills"])
        deaths = int(base["deaths"])
        dmg_dealt = int(base["dmg_dealt"]) / 100.0
        dmg_taken = int(base["dmg_taken"]) / 100.0

        winrate = wins / games if games else 0.0
        avg_elo = sum_elo / games if games else 0.0
        lead_rate = leads / used if used else 0.0
        kd = kills / max(1, deaths)

        ser_rows = conn.execute(
            """
            SELECT d.day AS day, d.games AS g, t.games_sum AS tot
            FROM pokemon_day d
            JOIN day_totals t ON (t.formatid=d.formatid AND t.day=d.day)
            WHERE d.formatid=? AND d.key=?
            ORDER BY d.day ASC
            """,
            (formatid, key),
        ).fetchall()

        days: List[str] = []
        pops: List[float] = []
        for r in ser_rows:
            tot = int(r["tot"]) or 1
            g = int(r["g"])
            days.append(str(r["day"]))
            pops.append(g / tot)

        mate_rows = conn.execute(
            """
            SELECT a, b, SUM(games) AS g, SUM(wins) AS w
            FROM mates_bucket
            WHERE formatid=? AND elo_bucket BETWEEN ? AND ? AND (a=? OR b=?)
            GROUP BY a, b
            """,
            (formatid, elo_min, elo_max, key, key),
        ).fetchall()

        mates = []
        for r in mate_rows:
            g = int(r["g"])
            if g < min_pair_games:
                continue
            w = int(r["w"])
            a = str(r["a"])
            b = str(r["b"])
            other = b if a == key else a
            wr = w / g if g else 0.0
            score = wilson_lower_bound(w, g)
            mates.append((score, other, g, wr))

        mates.sort(key=lambda x: x[0], reverse=True)
        mates = mates[:3]

        vs_rows = conn.execute(
            """
            SELECT b, SUM(games) AS g, SUM(wins) AS w
            FROM vs_bucket
            WHERE formatid=? AND elo_bucket BETWEEN ? AND ? AND a=?
            GROUP BY b
            """,
            (formatid, elo_min, elo_max, key),
        ).fetchall()

        counters = []
        for r in vs_rows:
            g = int(r["g"])
            if g < min_vs_games:
                continue
            w = int(r["w"])
            b = str(r["b"])
            wr = w / g if g else 0.0
            if wr >= 0.5:
                continue
            score = wilson_upper_bound(w, g)
            if score >= 0.5:
                continue
            counters.append((score, b, g, wr))

        counters.sort(key=lambda x: x[0])
        counters = counters[:3]

        other_keys = [k for _, k, _, _ in mates] + [k for _, k, _, _ in counters]
        name_map: Dict[str, str] = {}
        if other_keys:
            qs = ",".join("?" for _ in other_keys)
            rows_nm = conn.execute(
                f"""
                SELECT key, MIN(name) AS name
                FROM pokemon_bucket
                WHERE formatid=? AND key IN ({qs})
                GROUP BY key
                """,
                (formatid, *other_keys),
            ).fetchall()
            name_map = {str(r["key"]): str(r["name"]) for r in rows_nm if r["name"] is not None}

        moves_rows = conn.execute(
            "SELECT move, uses FROM pokemon_moves WHERE formatid=? AND key=? ORDER BY uses DESC LIMIT 15",
            (formatid, key),
        ).fetchall()

        items_rows = conn.execute(
            "SELECT item, uses FROM pokemon_items WHERE formatid=? AND key=? ORDER BY uses DESC LIMIT 15",
            (formatid, key),
        ).fetchall()

        abilities_map = load_pokedex_abilities_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
        expected_abilities = abilities_map.get(key, abilities_map.get(_to_id(name), []))
        expected_set = set(expected_abilities)

        abilities_payload: List[Dict[str, Any]] = []
        try:
            ab_rows = conn.execute(
                "SELECT ability, uses FROM pokemon_abilities WHERE formatid=? AND key=? ORDER BY uses DESC",
                (formatid, key),
            ).fetchall()
            if expected_set:
                ab_rows = [r for r in ab_rows if str(r["ability"]) in expected_set]
            total_ab = sum(int(r["uses"]) for r in ab_rows)
            if total_ab > 0:
                for r in ab_rows:
                    uses = int(r["uses"])
                    abilities_payload.append(
                        {
                            "ability": str(r["ability"]),
                            "uses": uses,
                            "pct": uses / total_ab,
                        }
                    )
        except sqlite3.OperationalError:
            pass

        conn.close()
        move_type_map = load_move_type_map(os.environ.get("PKMETA_MOVES_JSON", ""))

        if not abilities_payload:
            abilities = expected_abilities
            n = max(1, len(abilities))
            abilities_payload = [{"ability": a, "uses": 0, "pct": 1.0 / n} for a in abilities]

        base_stats_map = load_pokedex_base_stats_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
        base_stats = base_stats_map.get(key, base_stats_map.get(_to_id(name), {}))

        def kname(k: str) -> str:
            return name_map.get(k, k)

        mates_payload = []
        for score, other, g, wr in mates:
            nm = kname(other)
            mates_payload.append(
                {
                    "key": other,
                    "name": nm,
                    "games": g,
                    "winrate": wr,
                    "score": score,
                    "sprite_urls": sprite_urls(other, nm),
                }
            )

        counters_payload = []
        for score, other, g, wr in counters:
            nm = kname(other)
            counters_payload.append(
                {
                    "key": other,
                    "name": nm,
                    "games": g,
                    "winrate": 1.0 - wr,
                    "score": score,
                    "sprite_urls": sprite_urls(other, nm),
                }
            )

        return jsonify(
            {
                "key": key,
                "name": name,
                "games": games,
                "wins": wins,
                "winrate": winrate,
                "avg_elo": avg_elo,
                "lead_rate": lead_rate,
                "kills": kills,
                "deaths": deaths,
                "kd": kd,
                "dmg_dealt": dmg_dealt,
                "dmg_taken": dmg_taken,
                "sprite_urls": sprite_urls(key, name),
                "series": {"days": days, "popularity": pops},
                "mates": mates_payload,
                "counters": counters_payload,
                "moves": [
                    {
                        "move": str(r["move"]),
                        "uses": int(r["uses"]),
                        "type": move_type_map.get(_to_id(str(r["move"])), "Unknown"),
                    }
                    for r in moves_rows
                ],
                "items": [{"item": str(r["item"]), "uses": int(r["uses"])} for r in items_rows],
                "abilities": abilities_payload,
                "base_stats": base_stats,
                "min_pair_games": min_pair_games,
                "min_vs_games": min_vs_games,
            }
        )

    @app.get("/api/attack_types")
    def api_attack_types():
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        elo_min = clamp_int(request.args.get("elo_min", 0), 0, 10000)
        elo_max = clamp_int(request.args.get("elo_max", 10000), 0, 10000)
        if elo_min > elo_max:
            elo_min, elo_max = elo_max, elo_min

        conn = get_conn(attacks_db_path)
        try:
            rows = conn.execute(
                """
                SELECT move_type, SUM(games) AS games
                FROM move_bucket
                WHERE formatid=? AND elo_bucket BETWEEN ? AND ?
                GROUP BY move_type
                ORDER BY games DESC, move_type ASC
                """,
                (formatid, elo_min, elo_max),
            ).fetchall()
        except sqlite3.OperationalError:
            conn.close()
            return jsonify({"types": []})

        conn.close()
        types = [str(r["move_type"]) for r in rows if r["move_type"] is not None]
        return jsonify({"types": types})

    @app.get("/api/attacks")
    def api_attacks():
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        q = (request.args.get("q", "") or "").strip().lower()
        selected_types = _parse_types_param(request.args.get("types", "") or request.args.get("type", "") or "")
        sort = (request.args.get("sort", "winrate") or "winrate").strip().lower()
        order = (request.args.get("order", "desc") or "desc").strip().lower()
        min_games = clamp_int(request.args.get("min_games", 2000), 0, 10_000_000)
        limit = clamp_int(request.args.get("limit", 200), 10, 2000)

        elo_min = clamp_int(request.args.get("elo_min", 0), 0, 10000)
        elo_max = clamp_int(request.args.get("elo_max", 10000), 0, 10000)
        if elo_min > elo_max:
            elo_min, elo_max = elo_max, elo_min

        conn = get_conn(attacks_db_path)
        try:
            matches_row = conn.execute(
                "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
                (formatid, elo_min, elo_max),
            ).fetchone()

            q_sql = """
            SELECT
              move_id,
              MIN(move_name) AS move_name,
              MIN(move_type) AS move_type,
              SUM(games) AS games,
              SUM(wins) AS wins,
              SUM(uses) AS uses,
              SUM(sum_elo) AS sum_elo
            FROM move_bucket
            WHERE formatid=? AND elo_bucket BETWEEN ? AND ?
            """
            params: List[Any] = [formatid, elo_min, elo_max]
            if selected_types:
                qs = ",".join("?" for _ in selected_types)
                q_sql += f" AND move_type IN ({qs})"
                params.extend(sorted(selected_types))
            q_sql += " GROUP BY move_id HAVING SUM(games) >= ?"
            params.append(min_games)

            rows = conn.execute(q_sql, params).fetchall()
        except sqlite3.OperationalError:
            conn.close()
            return jsonify(
                {
                    "formatid": formatid,
                    "elo_min": elo_min,
                    "elo_max": elo_max,
                    "items": [],
                    "meta": {"matches": 0},
                }
            )

        conn.close()

        matches = int(matches_row["m"]) if matches_row else 0
        items: List[Dict[str, Any]] = []
        for r in rows:
            games = int(r["games"])
            wins = int(r["wins"])
            uses = int(r["uses"])
            sum_elo = int(r["sum_elo"])
            move_name = str(r["move_name"])
            move_id = str(r["move_id"])

            if q and q not in move_name.lower() and q not in move_id.lower():
                continue

            items.append(
                {
                    "move_id": move_id,
                    "move_name": move_name,
                    "move_type": str(r["move_type"]),
                    "games": games,
                    "wins": wins,
                    "uses": uses,
                    "winrate": (wins / games) if games else 0.0,
                    "avg_elo": (sum_elo / games) if games else 0.0,
                }
            )

        reverse = (order != "asc")
        if sort == "uses":
            items.sort(key=lambda x: x["uses"], reverse=reverse)
        elif sort == "games":
            items.sort(key=lambda x: x["games"], reverse=reverse)
        elif sort == "avg_elo":
            items.sort(key=lambda x: x["avg_elo"], reverse=reverse)
        elif sort == "type":
            items.sort(key=lambda x: x["move_type"].lower(), reverse=reverse)
        elif sort == "move":
            items.sort(key=lambda x: x["move_name"].lower(), reverse=reverse)
        else:
            items.sort(key=lambda x: x["winrate"], reverse=reverse)

        return jsonify(
            {
                "formatid": formatid,
                "elo_min": elo_min,
                "elo_max": elo_max,
                "items": items[:limit],
                "meta": {"matches": matches},
            }
        )

    return app


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="stats.sqlite")
    ap.add_argument("--attacks_db", default="attacks.sqlite")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()

    app = make_app(args.db, attacks_db_path=args.attacks_db)
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
