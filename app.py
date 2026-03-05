#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import math
import re
import sqlite3
import unicodedata
from typing import Any, Dict, List

from flask import Flask, Response, jsonify, redirect, render_template, request
from werkzeug.middleware.proxy_fix import ProxyFix

SPRITE_HOME = "https://play.pokemonshowdown.com/sprites/home/"
SPRITE_GEN5 = "https://play.pokemonshowdown.com/sprites/gen5/"
SPRITE_ANI  = "https://play.pokemonshowdown.com/sprites/ani/"


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


# Flask app

def make_app(db_path: str, min_pair_games_default: int = 3000, min_vs_games_default: int = 3000) -> Flask:
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

    @app.get("/api/pokemon")
    def api_pokemon():
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        q = (request.args.get("q", "") or "").strip().lower()
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

        conn.close()

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
                "moves": [{"move": str(r["move"]), "uses": int(r["uses"])} for r in moves_rows],
                "items": [{"item": str(r["item"]), "uses": int(r["uses"])} for r in items_rows],
                "min_pair_games": min_pair_games,
                "min_vs_games": min_vs_games,
            }
        )

    return app


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="stats.sqlite")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()

    app = make_app(args.db)
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
