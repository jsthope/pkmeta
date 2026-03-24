#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sqlite3
import unicodedata
from io import StringIO
from typing import Any, Dict, List, Optional, Set
from urllib.request import Request, urlopen

from flask import Flask, Response, jsonify, redirect, render_template, request
from translations import FOOTER_COPY, LANG_TO_OG_LOCALE, SEO_COPY, SUPPORTED_LANGS, UI_I18N
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
POKEMON_SPECIES_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/pokemon_species.csv",
)
POKEMON_SPECIES_NAMES_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/pokemon_species_names.csv",
)
POKEMON_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/pokemon.csv",
)
POKEMON_FORM_NAMES_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/pokemon_form_names.csv",
)
POKEMON_FORMS_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/pokemon_forms.csv",
)
MOVES_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/moves.csv",
)
MOVE_NAMES_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/move_names.csv",
)
ITEMS_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/items.csv",
)
ITEM_NAMES_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/item_names.csv",
)
ABILITIES_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/abilities.csv",
)
ABILITY_NAMES_CSV_URLS = (
    "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/ability_names.csv",
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
_POKEDEX_IDENTITY_CACHE: Dict[str, Dict[str, Any]] | None = None
_MOVE_TYPE_CACHE: Dict[str, str] | None = None
_POKEMON_LOCALIZED_NAME_CACHE: Dict[str, Dict[str, str]] = {}
_MOVE_LOCALIZED_NAME_CACHE: Dict[str, Dict[str, str]] = {}
_ITEM_LOCALIZED_NAME_CACHE: Dict[str, Dict[str, str]] = {}
_ABILITY_LOCALIZED_NAME_CACHE: Dict[str, Dict[str, str]] = {}
_POKEMON_PICKER_OPTION_CACHE: Dict[str, List[Dict[str, Any]]] = {}
DEFAULT_HOME_FORMAT = "gen9ou"
DEFAULT_HOME_LIMIT = 50
DEFAULT_MIN_USAGE_RATE = 0.005
DEFAULT_HOME_TEAM_SIZE = 6
DEFAULT_TEAM_MIN_GAMES = 100
DATASET_SOURCE_NAME = "pokemon-showdown-replays"
DATASET_SOURCE_URL = "https://huggingface.co/datasets/HolidayOugi/pokemon-showdown-replays"
SITE_BRAND = (os.environ.get("PKMETA_SITE_BRAND", "Pkmeta") or "Pkmeta").strip()
CANONICAL_HOST = (os.environ.get("PKMETA_CANONICAL_HOST", "pokemonchampionsmeta.net") or "pokemonchampionsmeta.net").strip().lower()
if CANONICAL_HOST.startswith("www."):
    CANONICAL_HOST = CANONICAL_HOST[4:]
SECONDARY_HOSTS: Set[str] = {
    host.strip().lower()
    for host in os.environ.get("PKMETA_SECONDARY_HOSTS", "pkmeta.net,www.pkmeta.net").split(",")
    if host.strip()
}
SECONDARY_HOSTS.add(f"www.{CANONICAL_HOST}")
SECONDARY_HOSTS.discard(CANONICAL_HOST)
PUBLIC_HOSTS: Set[str] = set(SECONDARY_HOSTS)
PUBLIC_HOSTS.add(CANONICAL_HOST)
PUBLIC_BASE_URL = f"https://{CANONICAL_HOST}"


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

def get_table_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(row["name"]) for row in rows if row["name"] is not None}

def clamp_int(x: Any, lo: int, hi: int) -> int:
    try:
        v = int(x)
    except Exception:
        v = lo
    return max(lo, min(hi, v))


def _to_id(s: str) -> str:
    t = unicodedata.normalize("NFKD", (s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    return _TOID_RE.sub("", t.lower())


def _read_text_url(url: str) -> str:
    req = Request(url, headers={"User-Agent": "pkmeta/1.0"})
    with urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8")


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


def load_pokedex_identity_map(local_json_path: str = "") -> Dict[str, Dict[str, Any]]:
    global _POKEDEX_IDENTITY_CACHE
    if _POKEDEX_IDENTITY_CACHE is not None:
        return _POKEDEX_IDENTITY_CACHE

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

    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            kid = _to_id(str(k))
            if not kid:
                continue
            entry = {
                "key": kid,
                "name": str(v.get("name") or ""),
                "num": int(v.get("num") or 0),
                "base_species": str(v.get("baseSpecies") or ""),
                "forme": str(v.get("forme") or ""),
            }
            out[kid] = entry

    _POKEDEX_IDENTITY_CACHE = out
    return out


def _read_csv_rows(urls: tuple[str, ...]) -> List[Dict[str, str]]:
    for url in urls:
        try:
            txt = _read_text_url(url)
            return list(csv.DictReader(StringIO(txt)))
        except Exception:
            continue
    return []


def _lang_to_pokeapi_id(lang: str) -> int:
    lang_map = {
        "en": 9,
        "fr": 5,
        "de": 6,
        "es": 7,
        "it": 8,
        "ja": 11,
        "ko": 3,
        "zh-hans": 12,
        "zh-hant": 4,
    }
    return lang_map.get(lang, 9)


def _normalize_lang(lang: str) -> str:
    raw = (lang or "en").strip().lower()
    if raw in {"zh", "zh-cn", "zh-sg", "zh-hans"}:
        return "zh-hans"
    if raw in {"zh-tw", "zh-hk", "zh-mo", "zh-hant"}:
        return "zh-hans"
    if raw.startswith("fr"):
        return "fr"
    if raw.startswith("de"):
        return "de"
    if raw.startswith("es"):
        return "es"
    if raw.startswith("it"):
        return "it"
    if raw.startswith("ja"):
        return "ja"
    if raw.startswith("ko"):
        return "ko"
    if raw.startswith("en"):
        return "en"
    return "en"


def _language_url(lang: str) -> str:
    return PUBLIC_BASE_URL if lang == "en" else f"{PUBLIC_BASE_URL}/{lang}"


def _seo_context_for_lang(lang: str) -> Dict[str, Any]:
    lang_norm = _normalize_lang(lang)
    if lang_norm not in SUPPORTED_LANGS:
        lang_norm = "en"

    copy = SEO_COPY.get(lang_norm, SEO_COPY["en"])
    canonical = _language_url(lang_norm)
    alternates = [{"lang": "x-default", "href": _language_url("en")}]
    alternates.extend({"lang": x, "href": _language_url(x)} for x in SUPPORTED_LANGS)

    return {
        "lang": lang_norm,
        "site_name": SITE_BRAND,
        "title": copy["title"],
        "description": copy["description"],
        "keywords": copy["keywords"],
        "canonical": canonical,
        "og_locale": LANG_TO_OG_LOCALE.get(lang_norm, "en_US"),
        "alternates": alternates,
        "json_ld": {
            "@context": "https://schema.org",
            "@type": "WebSite",
            "name": SITE_BRAND,
            "url": canonical,
            "inLanguage": lang_norm,
            "description": copy["description"],
            "keywords": copy["keywords"],
        },
    }


def _human_int(x: int) -> str:
    return f"{int(x):,}".replace(",", "\u202f")


def _recommended_min_games_from_matches(matches: int) -> int:
    mm = max(0, int(matches))
    if mm <= 0:
        return 0
    return int(math.ceil((2 * mm) * DEFAULT_MIN_USAGE_RATE))


def _recommended_team_min_games(matches: int) -> int:
    mm = max(0, int(matches))
    if mm <= 0:
        return 0
    return DEFAULT_TEAM_MIN_GAMES


def _pokemon_winrate_warning_meta(db_path: str, formatid: str, elo_min: int, elo_max: int) -> Dict[str, Any]:
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            """
            SELECT
              COALESCE((SELECT SUM(matches) FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?), 0) AS matches,
              COALESCE((SELECT SUM(brought) FROM pokemon_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?), 0) AS brought,
              COALESCE((SELECT SUM(wins) FROM pokemon_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?), 0) AS wins
            """,
            (formatid, elo_min, elo_max, formatid, elo_min, elo_max, formatid, elo_min, elo_max),
        ).fetchone()
    finally:
        conn.close()

    matches = int(row["matches"]) if row and row["matches"] is not None else 0
    brought = int(row["brought"]) if row and row["brought"] is not None else 0
    wins = int(row["wins"]) if row and row["wins"] is not None else 0
    if matches <= 0 or brought <= 0:
        return {"show": False}

    avg_brought_per_side = brought / max(1.0, 2.0 * matches)
    win_per_brought = wins / brought if brought else 0.0
    show = avg_brought_per_side < 5.8 and win_per_brought < 0.49
    return {
        "show": show,
        "avg_brought_per_side": avg_brought_per_side,
        "win_per_brought": win_per_brought,
        "message": (
            "Winrate is biased low in this format because replays often do not reveal full teams. "
            "Pokemon winrate here reflects revealed mons more than complete team slots."
        ) if show else "",
    }


def _matches_for_window(db_path: str, formatid: str, elo_min: int, elo_max: int) -> int:
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (formatid, elo_min, elo_max),
        ).fetchone()
    finally:
        conn.close()
    return int(row["m"]) if row else 0


def _resolve_teams_db_path(teams_db_path: str, formatid: str) -> Optional[str]:
    if not teams_db_path:
        return None
    if os.path.isdir(teams_db_path):
        candidate = os.path.join(teams_db_path, f"{formatid}.sqlite")
        return candidate if os.path.exists(candidate) else None
    return teams_db_path if os.path.exists(teams_db_path) else None


_DECIMAL_COMMA_LANGS = {"fr", "de", "es", "it"}


def _decimal_separator_for_lang(lang: str) -> str:
    return "," if _normalize_lang(lang) in _DECIMAL_COMMA_LANGS else "."


def _format_number_for_lang(value: Any, lang: str, digits: int = 0, grouping: bool = True) -> str:
    try:
        num = float(value)
    except Exception:
        num = 0.0

    if digits <= 0:
        out = f"{int(round(num)):,}" if grouping else str(int(round(num)))
        return out.replace(",", "\u202f") if grouping else out

    out = f"{num:,.{digits}f}" if grouping else f"{num:.{digits}f}"
    if "." in out:
        whole, frac = out.split(".", 1)
    else:
        whole, frac = out, ""

    if grouping:
        whole = whole.replace(",", "\u202f")
    else:
        whole = whole.replace(",", "")

    if not frac:
        return whole
    return f"{whole}{_decimal_separator_for_lang(lang)}{frac}"


def fmt_int_lang(value: Any, lang: str) -> str:
    return _format_number_for_lang(value, lang, digits=0, grouping=True)


def fmt_1_lang(value: Any, lang: str) -> str:
    return _format_number_for_lang(value, lang, digits=1, grouping=True)


def fmt_1_nogroup_lang(value: Any, lang: str) -> str:
    return _format_number_for_lang(value, lang, digits=1, grouping=False)


def fmt_pct_lang(value: Any, lang: str) -> str:
    try:
        num = float(value) * 100.0
    except Exception:
        num = 0.0
    return f"{_format_number_for_lang(num, lang, digits=1, grouping=False)}%"


def _footer_copy_for_lang(lang: str) -> Dict[str, str]:
    lang_norm = _normalize_lang(lang)
    return FOOTER_COPY.get(lang_norm, FOOTER_COPY["en"])


def _pokemon_picker_options(lang: str) -> List[Dict[str, Any]]:
    lang_norm = _normalize_lang(lang)
    cached = _POKEMON_PICKER_OPTION_CACHE.get(lang_norm)
    if cached is not None:
        return cached

    identity_map = load_pokedex_identity_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
    localized_name_map = load_pokemon_localized_name_map(lang_norm)
    poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
    options: List[Dict[str, Any]] = []
    for key, info in identity_map.items():
        if not key:
            continue
        name = str(info.get("name") or key)
        localized_name = localized_name_map.get(key, localized_name_map.get(_to_id(name), name))
        options.append(
            {
                "key": key,
                "name": name,
                "localized_name": localized_name,
                "types": poke_type_map.get(key, []),
                "sprite_urls": sprite_urls(key, name),
            }
        )

    options.sort(key=lambda x: (_to_id(x["localized_name"]), _to_id(x["name"])))
    _POKEMON_PICKER_OPTION_CACHE[lang_norm] = options
    return options


def _available_formats(db_path: str) -> List[str]:
    conn = get_conn(db_path)
    try:
        rows = conn.execute("SELECT DISTINCT formatid FROM matches_bucket ORDER BY formatid").fetchall()
    finally:
        conn.close()
    return [str(r["formatid"]) for r in rows if r["formatid"] is not None]


def _default_home_format(formats: List[str]) -> str:
    if DEFAULT_HOME_FORMAT in formats:
        return DEFAULT_HOME_FORMAT
    if "all" in formats:
        return "all"
    return formats[0] if formats else "all"


def _elo_bounds_for_format(db_path: str, formatid: str) -> Dict[str, int]:
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT MIN(elo_bucket) AS mn, MAX(elo_bucket) AS mx FROM matches_bucket WHERE formatid=?",
            (formatid,),
        ).fetchone()
    finally:
        conn.close()
    return {
        "min": int(row["mn"] if row and row["mn"] is not None else 0),
        "max": int(row["mx"] if row and row["mx"] is not None else 2000),
        "step": 100,
    }


def _home_pokemon_rows(
    db_path: str,
    formatid: str,
    lang: str,
    min_games: int,
    limit: int,
    elo_min: int,
    elo_max: int,
) -> Dict[str, Any]:
    conn = get_conn(db_path)
    try:
        matches_row = conn.execute(
            "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (formatid, elo_min, elo_max),
        ).fetchone()
        denom_row = conn.execute(
            "SELECT COALESCE(SUM(games),0) AS s FROM pokemon_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (formatid, elo_min, elo_max),
        ).fetchone()
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
            ORDER BY SUM(games) DESC
            LIMIT ?
            """,
            (formatid, elo_min, elo_max, min_games, limit),
        ).fetchall()
    finally:
        conn.close()

    matches = int(matches_row["m"]) if matches_row else 0
    total_games_sum = int(denom_row["s"]) if denom_row else 0
    denom = max(1, total_games_sum)
    poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
    localized_name_map = load_pokemon_localized_name_map(lang)

    items: List[Dict[str, Any]] = []
    for r in rows:
        games = int(r["games"])
        wins = int(r["wins"])
        used = int(r["used"])
        leads = int(r["leads"])
        sum_elo = int(r["sum_elo"])
        kills = int(r["kills"])
        deaths = int(r["deaths"])
        key = str(r["key"])
        name = str(r["name"])
        localized_name = localized_name_map.get(key, localized_name_map.get(_to_id(name), name))

        dmg_dealt = int(r["dmg_dealt"]) / 100.0
        dmg_taken = int(r["dmg_taken"]) / 100.0
        items.append(
            {
                "key": key,
                "name": name,
                "localized_name": localized_name,
                "games": games,
                "wins": wins,
                "winrate": (wins / games) if games else 0.0,
                "popularity": games / denom,
                "avg_elo": (sum_elo / games) if games else 0.0,
                "lead_rate": (leads / used) if used else 0.0,
                "kd": wilson_lower_bound(wins, games),
                "dmg_dealt": dmg_dealt,
                "dmg_taken": dmg_taken,
                "dmg_dealt_avg": (dmg_dealt / games) if games else 0.0,
                "dmg_taken_avg": (dmg_taken / games) if games else 0.0,
                "types": poke_type_map.get(key, []),
                "sprite_urls": sprite_urls(key, name),
            }
        )

    return {"matches": matches, "items": items}


def _default_type_options(
    db_path: str,
    attacks_db_path: str,
    formatid: str,
    elo_min: int,
    elo_max: int,
) -> List[str]:
    type_counts: Dict[str, int] = {}
    poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))

    conn = get_conn(db_path)
    try:
        rows = conn.execute(
            """
            SELECT key, SUM(games) AS games
            FROM pokemon_bucket
            WHERE formatid=? AND elo_bucket BETWEEN ? AND ?
            GROUP BY key
            """,
            (formatid, elo_min, elo_max),
        ).fetchall()
    finally:
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
    return [t for t, _ in types_sorted]


def _attack_items_payload(
    attacks_db_path: str,
    formatid: str,
    q: str,
    lang: str,
    selected_types: Set[str],
    sort: str,
    order: str,
    min_games: int,
    limit: int,
    elo_min: int,
    elo_max: int,
) -> Dict[str, Any]:
    qraw = (q or "").strip().lower()
    qid = _to_id(qraw)
    lang_norm = _normalize_lang(lang)

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
        return {
            "formatid": formatid,
            "elo_min": elo_min,
            "elo_max": elo_max,
            "items": [],
            "meta": {"matches": 0},
        }
    finally:
        conn.close()

    move_localized_map = load_move_localized_name_map(lang_norm)
    matches = int(matches_row["m"]) if matches_row else 0
    total_teams = matches * 2
    min_games_default = _recommended_min_games_from_matches(matches)
    items: List[Dict[str, Any]] = []
    for r in rows:
        games = int(r["games"])
        wins = int(r["wins"])
        uses = int(r["uses"])
        sum_elo = int(r["sum_elo"])
        move_name = str(r["move_name"])
        move_id = str(r["move_id"])
        localized_move_name = move_localized_map.get(_to_id(move_id), move_name)

        if (
            qraw
            and qid
            and qid not in _to_id(move_name)
            and qid not in _to_id(move_id)
            and qid not in _to_id(localized_move_name)
            and qraw not in move_name.lower()
            and qraw not in move_id.lower()
            and qraw not in localized_move_name.lower()
        ):
            continue
        if qraw and not qid and qraw not in move_name.lower() and qraw not in move_id.lower() and qraw not in localized_move_name.lower():
            continue

        items.append(
            {
                "move_id": move_id,
                "move_name": move_name,
                "localized_move_name": localized_move_name,
                "move_type": str(r["move_type"]),
                "games": games,
                "wins": wins,
                "uses": uses,
                "use_rate": (games / total_teams) if total_teams else 0.0,
                "winrate": (wins / games) if games else 0.0,
                "avg_elo": (sum_elo / games) if games else 0.0,
            }
        )

    reverse = (order != "asc")
    if sort == "uses":
        items.sort(key=lambda x: x["use_rate"], reverse=reverse)
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

    return {
        "formatid": formatid,
        "elo_min": elo_min,
        "elo_max": elo_max,
        "items": items[:limit],
        "meta": {"matches": matches, "min_games_default": min_games_default},
    }


def _home_page_context(db_path: str, attacks_db_path: str, teams_db_path: str, lang: str) -> Dict[str, Any]:
    formats = _available_formats(db_path)
    formatid = _default_home_format(formats)
    elo_bounds = _elo_bounds_for_format(db_path, formatid)
    footer_copy = _footer_copy_for_lang(lang)
    matches = _matches_for_window(db_path, formatid, elo_bounds["min"], elo_bounds["max"])
    default_min_games = _recommended_min_games_from_matches(matches)
    default_team_min_games = _recommended_team_min_games(matches)
    pokemon_winrate_warning = _pokemon_winrate_warning_meta(db_path, formatid, elo_bounds["min"], elo_bounds["max"])

    pokemon = _home_pokemon_rows(
        db_path=db_path,
        formatid=formatid,
        lang=lang,
        min_games=default_min_games,
        limit=DEFAULT_HOME_LIMIT,
        elo_min=elo_bounds["min"],
        elo_max=elo_bounds["max"],
    )
    attacks = _attack_items_payload(
        attacks_db_path=attacks_db_path,
        formatid=formatid,
        q="",
        lang=lang,
        selected_types=set(),
        sort="uses",
        order="desc",
        min_games=default_min_games,
        limit=DEFAULT_HOME_LIMIT,
        elo_min=elo_bounds["min"],
        elo_max=elo_bounds["max"],
    )
    team_rows_by_size: Dict[str, List[Dict[str, Any]]] = {}
    for combo_size in (6, 5, 4, 3, 2):
        payload = _team_items_payload(
            teams_db_path=teams_db_path,
            formatid=formatid,
            lang=lang,
        q="",
        selected_types=set(),
        required_member_keys=[],
        sort="popularity",
        order="desc",
            min_games=default_team_min_games,
            limit=DEFAULT_HOME_LIMIT,
            elo_min=elo_bounds["min"],
            elo_max=elo_bounds["max"],
            combo_size=combo_size,
        )
        team_rows_by_size[str(combo_size)] = payload["items"]
    initial_types = _default_type_options(
        db_path=db_path,
        attacks_db_path=attacks_db_path,
        formatid=formatid,
        elo_min=elo_bounds["min"],
        elo_max=elo_bounds["max"],
    )
    return {
        "formats": formats,
        "formatid": formatid,
        "elo_min": elo_bounds["min"],
        "elo_max": elo_bounds["max"],
        "elo_step": elo_bounds["step"],
        "matches": pokemon["matches"],
        "matches_display": _human_int(pokemon["matches"]),
        "min_games": default_min_games,
        "min_games_display": _human_int(default_min_games),
        "limit": DEFAULT_HOME_LIMIT,
        "rows": pokemon["items"],
        "attack_rows": attacks["items"],
        "team_rows": team_rows_by_size[str(DEFAULT_HOME_TEAM_SIZE)],
        "source_name": DATASET_SOURCE_NAME,
        "source_url": DATASET_SOURCE_URL,
        "footer": footer_copy,
        "pokemon_winrate_warning": pokemon_winrate_warning,
        "initial_state": {
            "formats": formats,
            "default_format": formatid,
            "elo_min": elo_bounds["min"],
            "elo_max": elo_bounds["max"],
            "elo_step": elo_bounds["step"],
            "types": initial_types,
            "matches": pokemon["matches"],
            "pokemon": {
                "min_games": default_min_games,
                "sort": "popularity",
                "order": "desc",
                "preloaded": True,
                "min_games_auto": True,
                "winrate_warning": pokemon_winrate_warning,
            },
            "attacks": {"min_games": default_min_games, "sort": "uses", "order": "desc", "preloaded": True, "min_games_auto": True},
            "teams": {
                "min_games": default_team_min_games,
                "sort": "popularity",
                "order": "desc",
                "combo_size": DEFAULT_HOME_TEAM_SIZE,
                "preloaded": True,
                "min_games_auto": True,
                "by_size": team_rows_by_size,
            },
            "pokemon_picker_options": _pokemon_picker_options(lang),
            "limit": DEFAULT_HOME_LIMIT,
        },
    }


def _ensure_team_cache_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
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

        CREATE INDEX IF NOT EXISTS idx_combo_query_cache_window_games
          ON combo_query_cache(formatid, elo_min, elo_max, combo_size, games);

        CREATE INDEX IF NOT EXISTS idx_combo_query_cache_window_names
          ON combo_query_cache(formatid, elo_min, elo_max, combo_size, combo_names);
        """
    )
    conn.commit()


def _combo_cache_metadata(
    combo_key: str,
    identity_map: Dict[str, Dict[str, Any]],
    poke_type_map: Dict[str, List[str]],
) -> Dict[str, str]:
    member_keys = [k for k in combo_key.split("|") if k]
    names: List[str] = []
    types: Set[str] = set()
    for key in member_keys:
        info = identity_map.get(key, {})
        name = str(info.get("name") or key)
        names.append(name.lower())
        types.update(poke_type_map.get(key, []))
    return {
        "combo_names": " | ".join(names),
        "combo_types": "|" + "|".join(sorted(types)) + "|" if types else "",
    }


def _populate_team_query_cache(
    conn: sqlite3.Connection,
    formatid: str,
    combo_size: int,
    elo_min: int,
    elo_max: int,
) -> int:
    meta_row = conn.execute(
        """
        SELECT matches
        FROM combo_query_cache_meta
        WHERE formatid=? AND elo_min=? AND elo_max=? AND combo_size=?
        """,
        (formatid, elo_min, elo_max, combo_size),
    ).fetchone()
    if meta_row is not None:
        return int(meta_row["matches"])

    matches_row = conn.execute(
        "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
        (formatid, elo_min, elo_max),
    ).fetchone()
    matches = int(matches_row["m"]) if matches_row else 0

    rows = conn.execute(
        """
        SELECT combo_key, SUM(games) AS games, SUM(wins) AS wins, SUM(sum_elo) AS sum_elo
        FROM combo_bucket
        WHERE formatid=? AND combo_size=? AND elo_bucket BETWEEN ? AND ?
        GROUP BY combo_key
        """,
        (formatid, combo_size, elo_min, elo_max),
    ).fetchall()

    identity_map = load_pokedex_identity_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
    poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))

    conn.execute("BEGIN")
    conn.execute(
        "DELETE FROM combo_query_cache WHERE formatid=? AND elo_min=? AND elo_max=? AND combo_size=?",
        (formatid, elo_min, elo_max, combo_size),
    )
    if rows:
        conn.executemany(
            """
            INSERT INTO combo_query_cache(
              formatid, elo_min, elo_max, combo_size, combo_key, combo_names, combo_types, games, wins, sum_elo
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    formatid,
                    elo_min,
                    elo_max,
                    combo_size,
                    combo_key,
                    meta["combo_names"],
                    meta["combo_types"],
                    int(r["games"]),
                    int(r["wins"]),
                    int(r["sum_elo"]),
                )
                for r in rows
                for combo_key in [str(r["combo_key"])]
                for meta in [_combo_cache_metadata(combo_key, identity_map, poke_type_map)]
            ],
        )
    conn.execute(
        """
        INSERT INTO combo_query_cache_meta(formatid, elo_min, elo_max, combo_size, matches, cached_at)
        VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(formatid, elo_min, elo_max, combo_size) DO UPDATE SET
          matches=excluded.matches,
          cached_at=CURRENT_TIMESTAMP
        """,
        (formatid, elo_min, elo_max, combo_size, matches),
    )
    conn.execute("COMMIT")
    return matches


def _team_items_payload(
    teams_db_path: str,
    formatid: str,
    lang: str,
    q: str,
    selected_types: Set[str],
    required_member_keys: List[str],
    sort: str,
    order: str,
    min_games: int,
    limit: int,
    elo_min: int,
    elo_max: int,
    combo_size: int,
) -> Dict[str, Any]:
    resolved_teams_db_path = _resolve_teams_db_path(teams_db_path, formatid)
    if not resolved_teams_db_path:
        return {
            "formatid": formatid,
            "elo_min": elo_min,
            "elo_max": elo_max,
            "combo_size": combo_size,
            "items": [],
            "meta": {"matches": 0},
        }

    qraw = (q or "").strip().lower()
    qid = _to_id(qraw)
    lang_norm = _normalize_lang(lang)
    required_member_keys = sorted({_to_id(x) for x in required_member_keys if _to_id(x)})
    localized_name_map = load_pokemon_localized_name_map(lang_norm)
    identity_map = load_pokedex_identity_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
    poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))

    conn = get_conn(resolved_teams_db_path)
    try:
        _ensure_team_cache_schema(conn)
        matches = _populate_team_query_cache(conn, formatid, combo_size, elo_min, elo_max)

        query_sql = """
        SELECT combo_key, combo_names, combo_types, games, wins, sum_elo
        FROM combo_query_cache
        WHERE formatid=? AND elo_min=? AND elo_max=? AND combo_size=? AND games >= ?
        """
        params: List[Any] = [formatid, elo_min, elo_max, combo_size, min_games]

        selected_type = next(iter(selected_types), "") if selected_types else ""
        if selected_type:
            query_sql += " AND combo_types LIKE ?"
            params.append(f"%|{selected_type}|%")

        for member_key in required_member_keys:
            query_sql += " AND ('|' || combo_key || '|') LIKE ?"
            params.append(f"%|{member_key}|%")

        localized_search_required = bool(qraw and lang_norm != "en")
        if qid and not localized_search_required:
            query_sql += " AND combo_key LIKE ?"
            params.append(f"%{qid}%")
        elif qraw and not localized_search_required:
            query_sql += " AND combo_names LIKE ?"
            params.append(f"%{qraw}%")

        fast_path = not qraw and not selected_type and sort in {"popularity", "games", "winrate", "avg_elo"}
        if fast_path:
            if sort in {"popularity", "games"}:
                order_sql = "games"
            elif sort == "avg_elo":
                order_sql = "CASE WHEN games > 0 THEN (1.0 * sum_elo / games) ELSE 0 END"
            else:
                order_sql = "CASE WHEN games > 0 THEN (1.0 * wins / games) ELSE 0 END"
            direction = "ASC" if order == "asc" else "DESC"
            query_sql += f" ORDER BY {order_sql} {direction}, combo_key ASC LIMIT ?"
            params.append(limit)

        rows = conn.execute(query_sql, params).fetchall()
    except sqlite3.OperationalError:
        conn.close()
        return {
            "formatid": formatid,
            "elo_min": elo_min,
            "elo_max": elo_max,
            "combo_size": combo_size,
            "items": [],
            "meta": {"matches": 0},
        }
    finally:
        conn.close()

    denom = max(1, 2 * matches)
    min_games_default = _recommended_team_min_games(matches)

    items: List[Dict[str, Any]] = []
    for r in rows:
        combo_key = str(r["combo_key"])
        member_keys = [k for k in combo_key.split("|") if k]
        if required_member_keys and not all(member in member_keys for member in required_member_keys):
            continue
        members: List[Dict[str, Any]] = []
        searchable_parts: List[str] = []
        member_types: Set[str] = set()

        for key in member_keys:
            info = identity_map.get(key, {})
            name = str(info.get("name") or key)
            localized_name = localized_name_map.get(key, localized_name_map.get(_to_id(name), name))
            types = poke_type_map.get(key, [])
            member_types.update(types)
            searchable_parts.extend([key.lower(), name.lower(), localized_name.lower()])
            members.append(
                {
                    "key": key,
                    "name": name,
                    "localized_name": localized_name,
                    "types": types,
                    "sprite_urls": sprite_urls(key, name),
                }
            )

        if selected_types and not selected_types.intersection(member_types):
            continue

        if qraw:
            haystack = " ".join(searchable_parts)
            if qid:
                if qid not in _to_id(haystack):
                    continue
            elif qraw not in haystack:
                continue

        games = int(r["games"])
        wins = int(r["wins"])
        sum_elo = int(r["sum_elo"])
        localized_label = " / ".join(m["localized_name"] for m in members)
        raw_label = " / ".join(m["name"] for m in members)

        items.append(
            {
                "combo_key": combo_key,
                "combo_size": combo_size,
                "games": games,
                "wins": wins,
                "winrate": (wins / games) if games else 0.0,
                "avg_elo": (sum_elo / games) if games else 0.0,
                "popularity": games / denom,
                "members": members,
                "label": localized_label,
                "raw_label": raw_label,
            }
        )

    reverse = (order != "asc")
    if not fast_path:
        if sort == "games":
            items.sort(key=lambda x: x["games"], reverse=reverse)
        elif sort == "avg_elo":
            items.sort(key=lambda x: x["avg_elo"], reverse=reverse)
        elif sort == "name":
            items.sort(key=lambda x: x["raw_label"].lower(), reverse=reverse)
        else:
            key_name = "popularity" if sort == "popularity" else "winrate"
            items.sort(key=lambda x: x[key_name], reverse=reverse)

    return {
        "formatid": formatid,
        "elo_min": elo_min,
        "elo_max": elo_max,
        "combo_size": combo_size,
        "items": items[:limit] if not fast_path else items,
        "meta": {"matches": matches, "min_games_default": min_games_default},
    }


def _load_species_name_by_id(lang: str) -> Dict[int, str]:
    rows = _read_csv_rows(POKEMON_SPECIES_NAMES_CSV_URLS)
    out: Dict[int, str] = {}
    lang_id = _lang_to_pokeapi_id(lang)
    for r in rows:
        try:
            if int(r.get("local_language_id") or 0) != lang_id:
                continue
            sid = int(r.get("pokemon_species_id") or 0)
            nm = str(r.get("name") or "").strip()
            if sid > 0 and nm:
                out[sid] = nm
        except Exception:
            continue
    return out


def _load_pokemon_name_by_id(lang: str) -> Dict[int, str]:
    pokemon_rows = _read_csv_rows(POKEMON_CSV_URLS)
    out: Dict[int, str] = {}
    if not pokemon_rows:
        return out

    lang_id = _lang_to_pokeapi_id(lang)
    species_name_by_id = _load_species_name_by_id(lang)
    form_rows = _read_csv_rows(POKEMON_FORM_NAMES_CSV_URLS)
    forms_rows = _read_csv_rows(POKEMON_FORMS_CSV_URLS)
    pokemon_id_by_form_id: Dict[int, int] = {}
    for r in forms_rows:
        try:
            form_id = int(r.get("id") or 0)
            pokemon_id = int(r.get("pokemon_id") or 0)
            if form_id > 0 and pokemon_id > 0:
                pokemon_id_by_form_id[form_id] = pokemon_id
        except Exception:
            continue

    form_pokemon_name_by_id: Dict[int, str] = {}
    for r in form_rows:
        try:
            if int(r.get("local_language_id") or 0) != lang_id:
                continue
            form_id = int(r.get("pokemon_form_id") or 0)
            full_name = str(r.get("pokemon_name") or "").strip()
            pokemon_id = pokemon_id_by_form_id.get(form_id, 0)
            if pokemon_id > 0 and full_name:
                form_pokemon_name_by_id[pokemon_id] = full_name
        except Exception:
            continue

    for r in pokemon_rows:
        try:
            pid = int(r.get("id") or 0)
            sid = int(r.get("species_id") or 0)
            if pid <= 0:
                continue
            nm = form_pokemon_name_by_id.get(pid, species_name_by_id.get(sid, ""))
            if nm:
                out[pid] = nm
        except Exception:
            continue

    return out


def _load_identifier_map_by_id(urls: tuple[str, ...]) -> Dict[int, str]:
    rows = _read_csv_rows(urls)
    out: Dict[int, str] = {}
    for r in rows:
        try:
            rid = int(r.get("id") or 0)
            identifier = str(r.get("identifier") or "").strip()
            if rid > 0 and identifier:
                out[rid] = identifier
        except Exception:
            continue
    return out


def _load_localized_name_by_id(name_rows_urls: tuple[str, ...], id_col: str, lang: str) -> Dict[int, str]:
    rows = _read_csv_rows(name_rows_urls)
    out: Dict[int, str] = {}
    lang_id = _lang_to_pokeapi_id(lang)
    for r in rows:
        try:
            if int(r.get("local_language_id") or 0) != lang_id:
                continue
            rid = int(r.get(id_col) or 0)
            nm = str(r.get("name") or "").strip()
            if rid > 0 and nm:
                out[rid] = nm
        except Exception:
            continue
    return out


def _identifier_localized_map(
    identifier_rows_urls: tuple[str, ...],
    localized_rows_urls: tuple[str, ...],
    localized_id_col: str,
    lang: str,
) -> Dict[str, str]:
    ident_by_id = _load_identifier_map_by_id(identifier_rows_urls)
    local_by_id = _load_localized_name_by_id(localized_rows_urls, localized_id_col, lang)
    out: Dict[str, str] = {}
    for rid, identifier in ident_by_id.items():
        if rid not in local_by_id:
            continue
        out[_to_id(identifier)] = local_by_id[rid]
    return out


def load_pokemon_localized_name_map(lang: str = "en") -> Dict[str, str]:
    lang_norm = _normalize_lang(lang)

    cached = _POKEMON_LOCALIZED_NAME_CACHE.get(lang_norm)
    if cached is not None:
        return cached

    identity_map = load_pokedex_identity_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
    out: Dict[str, str] = {}

    pokemon_rows = _read_csv_rows(POKEMON_CSV_URLS)
    pokemon_name_by_id = _load_pokemon_name_by_id(lang_norm)
    localized_by_identifier: Dict[str, str] = {}
    for r in pokemon_rows:
        try:
            pid = int(r.get("id") or 0)
            ident = str(r.get("identifier") or "")
            loc = pokemon_name_by_id.get(pid, "")
            if pid > 0 and ident and loc:
                localized_by_identifier[_to_id(ident)] = loc
        except Exception:
            continue

    species_by_num = _load_species_name_by_id(lang_norm)

    for key, info in identity_map.items():
        en_name = str(info.get("name") or "")
        num = int(info.get("num") or 0)
        base_species = str(info.get("base_species") or "")
        forme = str(info.get("forme") or "")

        candidates = [
            key,
            _to_id(en_name),
            _to_id(f"{base_species}-{forme}"),
            _to_id(f"{base_species}-{forme}-mask"),
            _to_id(f"{base_species}-{forme}-style"),
            _to_id(base_species),
        ]
        localized = ""
        for cid in candidates:
            if cid and cid in localized_by_identifier:
                localized = localized_by_identifier[cid]
                break

        if not localized and num > 0:
            localized = species_by_num.get(num, "")
            if localized and forme:
                localized = f"{localized}-{forme}"

        if not localized:
            localized = en_name

        out[key] = localized
        enid = _to_id(en_name)
        if enid and enid not in out:
            out[enid] = localized

    _POKEMON_LOCALIZED_NAME_CACHE[lang_norm] = out
    return out


def load_move_localized_name_map(lang: str = "en") -> Dict[str, str]:
    lang_norm = _normalize_lang(lang)

    cached = _MOVE_LOCALIZED_NAME_CACHE.get(lang_norm)
    if cached is not None:
        return cached

    out = _identifier_localized_map(MOVES_CSV_URLS, MOVE_NAMES_CSV_URLS, "move_id", lang_norm)
    _MOVE_LOCALIZED_NAME_CACHE[lang_norm] = out
    return out


def load_item_localized_name_map(lang: str = "en") -> Dict[str, str]:
    lang_norm = _normalize_lang(lang)

    cached = _ITEM_LOCALIZED_NAME_CACHE.get(lang_norm)
    if cached is not None:
        return cached

    out = _identifier_localized_map(ITEMS_CSV_URLS, ITEM_NAMES_CSV_URLS, "item_id", lang_norm)
    _ITEM_LOCALIZED_NAME_CACHE[lang_norm] = out
    return out


def load_ability_localized_name_map(lang: str = "en") -> Dict[str, str]:
    lang_norm = _normalize_lang(lang)

    cached = _ABILITY_LOCALIZED_NAME_CACHE.get(lang_norm)
    if cached is not None:
        return cached

    out = _identifier_localized_map(ABILITIES_CSV_URLS, ABILITY_NAMES_CSV_URLS, "ability_id", lang_norm)
    _ABILITY_LOCALIZED_NAME_CACHE[lang_norm] = out
    return out


# Flask app

def make_app(
    db_path: str,
    attacks_db_path: str = "attacks.sqlite",
    teams_db_path: str = "teams.sqlite",
    min_pair_games_default: int = 3000,
    min_vs_games_default: int = 3000,
) -> Flask:
    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
    app.jinja_env.globals.update(
        fmt_int_lang=fmt_int_lang,
        fmt_1_lang=fmt_1_lang,
        fmt_1_nogroup_lang=fmt_1_nogroup_lang,
        fmt_pct_lang=fmt_pct_lang,
    )

    @app.before_request
    def force_https_and_canonical_host():
        host = (request.host.split(":", 1)[0] or "").lower()
        if host not in PUBLIC_HOSTS:
            return None

        canonical_url = f"{PUBLIC_BASE_URL}{request.full_path.rstrip('?')}"
        if host != CANONICAL_HOST or not request.is_secure:
            return redirect(canonical_url, code=301)

        return None

    @app.after_request
    def add_security_headers(resp: Response):
        host = (request.host.split(":", 1)[0] or "").lower()
        if host == CANONICAL_HOST and request.is_secure:
            resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
        return resp

    @app.get("/")
    def index() -> Any:
        lang_from_query = _normalize_lang(request.args.get("lang", "") or "")
        if lang_from_query in SUPPORTED_LANGS and lang_from_query != "en":
            return redirect(f"/{lang_from_query}", code=302)
        seo = _seo_context_for_lang("en")
        home = _home_page_context(db_path, attacks_db_path, teams_db_path, seo["lang"])
        return render_template("index.html", seo=seo, server_lang=seo["lang"], home=home, ui_i18n=UI_I18N)

    @app.get("/<lang_code>")
    def index_lang(lang_code: str) -> Any:
        raw = (lang_code or "").strip().lower()
        allowed_inputs = set(SUPPORTED_LANGS) | {"zh", "zh-cn", "zh-sg", "zh-tw", "zh-hk", "zh-mo", "zh-hant"}
        if raw not in allowed_inputs:
            return Response("Not found", status=404)
        lang_norm = _normalize_lang(lang_code)
        if lang_norm not in SUPPORTED_LANGS:
            return Response("Not found", status=404)
        if lang_code != lang_norm:
            target = "/" if lang_norm == "en" else f"/{lang_norm}"
            return redirect(target, code=301)
        if lang_norm == "en":
            return redirect("/", code=301)
        seo = _seo_context_for_lang(lang_norm)
        home = _home_page_context(db_path, attacks_db_path, teams_db_path, seo["lang"])
        return render_template("index.html", seo=seo, server_lang=seo["lang"], home=home, ui_i18n=UI_I18N)

    @app.get("/robots.txt")
    def robots_txt() -> Response:
        body = "\n".join(
            [
                "User-agent: *",
                "Allow: /",
                f"Sitemap: {PUBLIC_BASE_URL}/sitemap.xml",
            ]
        )
        return Response(body + "\n", mimetype="text/plain")

    @app.get("/sitemap.xml")
    def sitemap_xml() -> Response:
        langs = ["en"] + [lang for lang in SUPPORTED_LANGS if lang != "en"]
        urls = [_language_url(lang) for lang in langs]
        alternates = [(lang, _language_url(lang)) for lang in langs]
        alternates.insert(0, ("x-default", _language_url("en")))
        parts = [
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
            "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\" xmlns:xhtml=\"http://www.w3.org/1999/xhtml\">",
        ]
        for u in urls:
            alt_lines = [f"    <xhtml:link rel=\"alternate\" hreflang=\"{lang}\" href=\"{href}\" />" for lang, href in alternates]
            parts.extend(
                [
                    "  <url>",
                    f"    <loc>{u}</loc>",
                    *alt_lines,
                    "    <changefreq>daily</changefreq>",
                    "    <priority>1.0</priority>",
                    "  </url>",
                ]
            )
        parts.append("</urlset>")
        xml = "\n".join(parts) + "\n"
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
        qraw = q
        qid = _to_id(q)
        lang = _normalize_lang(request.args.get("lang", "en") or "en")
        selected_types = _parse_types_param(request.args.get("types", "") or "")
        sort = (request.args.get("sort", "winrate") or "winrate").strip()
        order = (request.args.get("order", "desc") or "desc").strip().lower()
        limit = clamp_int(request.args.get("limit", DEFAULT_HOME_LIMIT), 10, 2000)

        elo_min = clamp_int(request.args.get("elo_min", 0), 0, 10000)
        elo_max = clamp_int(request.args.get("elo_max", 10000), 0, 10000)
        if elo_min > elo_max:
            elo_min, elo_max = elo_max, elo_min

        conn = get_conn(db_path)

        matches_row = conn.execute(
            "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (formatid, elo_min, elo_max),
        ).fetchone()
        matches = int(matches_row["m"]) if matches_row else 0
        min_games_default = _recommended_min_games_from_matches(matches)
        min_games = clamp_int(request.args.get("min_games", min_games_default), 0, 10_000_000)
        winrate_warning = _pokemon_winrate_warning_meta(db_path, formatid, elo_min, elo_max)

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
        localized_name_map = load_pokemon_localized_name_map(lang)

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
            kd = wilson_lower_bound(wins, games)

            dmg_dealt = int(r["dmg_dealt"]) / 100.0
            dmg_taken = int(r["dmg_taken"]) / 100.0

            key = str(r["key"])
            name = str(r["name"])
            localized_name = localized_name_map.get(key, localized_name_map.get(_to_id(name), name))
            if qraw:
                if (
                    qid
                    and qid not in _to_id(name)
                    and qid not in _to_id(key)
                    and qid not in _to_id(localized_name)
                    and qraw not in name.lower()
                    and qraw not in key.lower()
                    and qraw not in localized_name.lower()
                ):
                    continue
                if not qid and qraw not in name.lower() and qraw not in key.lower() and qraw not in localized_name.lower():
                    continue
            ptypes = poke_type_map.get(key, [])
            if selected_types and not selected_types.intersection(ptypes):
                continue

            items.append(
                {
                    "key": key,
                    "name": name,
                    "localized_name": localized_name,
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
                "meta": {"matches": matches, "min_games_default": min_games_default, "winrate_warning": winrate_warning},
            }
        )

    @app.get("/api/pokemon_options")
    def api_pokemon_options():
        lang = _normalize_lang(request.args.get("lang", "en") or "en")
        return jsonify({"items": _pokemon_picker_options(lang)})

    @app.get("/api/pokemon/<key>/detail")
    def api_pokemon_detail(key: str):
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        lang = _normalize_lang(request.args.get("lang", "en") or "en")
        elo_min = clamp_int(request.args.get("elo_min", 0), 0, 10000)
        elo_max = clamp_int(request.args.get("elo_max", 10000), 0, 10000)
        if elo_min > elo_max:
            elo_min, elo_max = elo_max, elo_min

        conn = get_conn(db_path)
        matches_row = conn.execute(
            "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (formatid, elo_min, elo_max),
        ).fetchone()
        matches = int(matches_row["m"]) if matches_row else 0
        min_synergy_games_default = _recommended_min_games_from_matches(matches)
        min_pair_games = clamp_int(request.args.get("min_pair_games", min_synergy_games_default), 0, 10_000_000)
        min_vs_games = clamp_int(request.args.get("min_vs_games", min_synergy_games_default), 0, 10_000_000)

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
        popularity = games / max(1, 2 * matches)
        avg_elo = sum_elo / games if games else 0.0
        lead_rate = leads / used if used else 0.0
        kd = wilson_lower_bound(wins, games)

        day_cols = get_table_columns(conn, "pokemon_day")
        day_total_cols = get_table_columns(conn, "day_totals")
        ser_rows: List[sqlite3.Row] = []
        try:
            if "elo_bucket" in day_cols and "elo_bucket" in day_total_cols:
                ser_rows = conn.execute(
                    """
                    SELECT d.day AS day, SUM(d.games) AS g, SUM(d.wins) AS w, SUM(t.matches) AS m
                    FROM pokemon_day d
                    JOIN day_totals t
                      ON (t.formatid=d.formatid AND t.elo_bucket=d.elo_bucket AND t.day=d.day)
                    WHERE d.formatid=? AND d.key=? AND d.elo_bucket BETWEEN ? AND ?
                    GROUP BY d.day
                    ORDER BY d.day ASC
                    """,
                    (formatid, key, elo_min, elo_max),
                ).fetchall()
            else:
                ser_rows = conn.execute(
                    """
                    SELECT d.day AS day, SUM(d.games) AS g, SUM(d.wins) AS w, SUM(t.matches) AS m
                    FROM pokemon_day d
                    JOIN day_totals t ON (t.formatid=d.formatid AND t.day=d.day)
                    WHERE d.formatid=? AND d.key=?
                    GROUP BY d.day
                    ORDER BY d.day ASC
                    """,
                    (formatid, key),
                ).fetchall()
        except sqlite3.OperationalError:
            ser_rows = []

        days: List[str] = []
        pops: List[float] = []
        day_winrates: List[float] = []
        day_games: List[int] = []
        day_wins: List[int] = []
        day_totals: List[int] = []
        for r in ser_rows:
            day_matches = int(r["m"] or 0)
            g = int(r["g"] or 0)
            w = int(r["w"] or 0)
            if g <= 0 or day_matches <= 0:
                continue
            days.append(str(r["day"]))
            day_games.append(g)
            day_wins.append(w)
            day_totals.append(2 * day_matches)
            pops.append(g / max(1, 2 * day_matches))
            day_winrates.append(w / g)

        first_day = days[0] if days else ""
        last_day = days[-1] if days else ""

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

        format_rows = conn.execute(
            """
            SELECT formatid, SUM(games) AS g
            FROM pokemon_bucket
            WHERE key=? AND formatid <> 'all'
            GROUP BY formatid
            ORDER BY g DESC, formatid ASC
            """,
            (key,),
        ).fetchall()
        available_formats = [str(r["formatid"]) for r in format_rows if r["formatid"] is not None]
        if available_formats:
            available_formats = ["all", *available_formats]

        moves_rows = conn.execute(
            "SELECT move, uses FROM pokemon_moves WHERE formatid=? AND key=? ORDER BY uses DESC",
            (formatid, key),
        ).fetchall()

        items_rows = conn.execute(
            "SELECT item, uses FROM pokemon_items WHERE formatid=? AND key=? ORDER BY uses DESC",
            (formatid, key),
        ).fetchall()

        abilities_map = load_pokedex_abilities_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
        expected_abilities = abilities_map.get(key, abilities_map.get(_to_id(name), []))
        expected_set = set(expected_abilities)
        ability_localized_map = load_ability_localized_name_map(lang)

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
                    ability_name = str(r["ability"])
                    abilities_payload.append(
                        {
                            "ability": ability_name,
                            "localized_ability": ability_localized_map.get(_to_id(ability_name), ability_name),
                            "uses": uses,
                            "pct": uses / total_ab,
                        }
                    )
        except sqlite3.OperationalError:
            pass

        conn.close()
        move_type_map = load_move_type_map(os.environ.get("PKMETA_MOVES_JSON", ""))
        poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
        localized_name_map = load_pokemon_localized_name_map(lang)
        move_localized_map = load_move_localized_name_map(lang)
        item_localized_map = load_item_localized_name_map(lang)
        localized_name = localized_name_map.get(key, localized_name_map.get(_to_id(name), name))

        if not abilities_payload:
            abilities = expected_abilities
            n = max(1, len(abilities))
            abilities_payload = [
                {
                    "ability": a,
                    "localized_ability": ability_localized_map.get(_to_id(a), a),
                    "uses": 0,
                    "pct": 1.0 / n,
                }
                for a in abilities
            ]

        base_stats_map = load_pokedex_base_stats_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
        base_stats = base_stats_map.get(key, base_stats_map.get(_to_id(name), {}))

        def kname(k: str) -> str:
            return name_map.get(k, k)

        mates_payload = []
        for score, other, g, wr in mates:
            nm = kname(other)
            local_nm = localized_name_map.get(other, localized_name_map.get(_to_id(nm), nm))
            mates_payload.append(
                {
                    "key": other,
                    "name": nm,
                    "localized_name": local_nm,
                    "games": g,
                    "winrate": wr,
                    "score": score,
                    "sprite_urls": sprite_urls(other, nm),
                }
            )

        counters_payload = []
        for score, other, g, wr in counters:
            nm = kname(other)
            local_nm = localized_name_map.get(other, localized_name_map.get(_to_id(nm), nm))
            counters_payload.append(
                {
                    "key": other,
                    "name": nm,
                    "localized_name": local_nm,
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
                "localized_name": localized_name,
                "types": poke_type_map.get(key, []),
                "available_formats": available_formats,
                "games": games,
                "wins": wins,
                "popularity": popularity,
                "winrate": winrate,
                "avg_elo": avg_elo,
                "lead_rate": lead_rate,
                "kills": kills,
                "deaths": deaths,
                "kd": kd,
                "dmg_dealt": dmg_dealt,
                "dmg_taken": dmg_taken,
                "sprite_urls": sprite_urls(key, name),
                "series": {
                    "days": days,
                    "games": day_games,
                    "wins": day_wins,
                    "totals": day_totals,
                    "popularity": pops,
                    "winrate": day_winrates,
                    "overall_winrate": winrate,
                    "first_day": first_day,
                    "last_day": last_day,
                },
                "mates": mates_payload,
                "counters": counters_payload,
                "moves": [
                    {
                        "move": str(r["move"]),
                        "localized_move": move_localized_map.get(_to_id(str(r["move"])), str(r["move"])),
                        "uses": int(r["uses"]),
                        "type": move_type_map.get(_to_id(str(r["move"])), "Unknown"),
                    }
                    for r in moves_rows
                ],
                "items": [
                    {
                        "item": str(r["item"]),
                        "localized_item": item_localized_map.get(_to_id(str(r["item"])), str(r["item"])),
                        "uses": int(r["uses"]),
                    }
                    for r in items_rows
                ],
                "abilities": abilities_payload,
                "base_stats": base_stats,
                "min_pair_games": min_pair_games,
                "min_vs_games": min_vs_games,
                "min_synergy_games_default": min_synergy_games_default,
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
        qraw = q
        qid = _to_id(q)
        lang = _normalize_lang(request.args.get("lang", "en") or "en")
        selected_types = _parse_types_param(request.args.get("types", "") or request.args.get("type", "") or "")
        sort = (request.args.get("sort", "uses") or "uses").strip().lower()
        order = (request.args.get("order", "desc") or "desc").strip().lower()
        limit = clamp_int(request.args.get("limit", DEFAULT_HOME_LIMIT), 10, 2000)

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
            matches = int(matches_row["m"]) if matches_row else 0
            min_games_default = _recommended_min_games_from_matches(matches)
            min_games = clamp_int(request.args.get("min_games", min_games_default), 0, 10_000_000)

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
        move_localized_map = load_move_localized_name_map(lang)

        total_teams = matches * 2
        items: List[Dict[str, Any]] = []
        for r in rows:
            games = int(r["games"])
            wins = int(r["wins"])
            uses = int(r["uses"])
            sum_elo = int(r["sum_elo"])
            move_name = str(r["move_name"])
            move_id = str(r["move_id"])
            localized_move_name = move_localized_map.get(_to_id(move_id), move_name)

            if (
                qraw
                and qid
                and qid not in _to_id(move_name)
                and qid not in _to_id(move_id)
                and qid not in _to_id(localized_move_name)
                and qraw not in move_name.lower()
                and qraw not in move_id.lower()
                and qraw not in localized_move_name.lower()
            ):
                continue
            if qraw and not qid and qraw not in move_name.lower() and qraw not in move_id.lower() and qraw not in localized_move_name.lower():
                continue

            items.append(
                {
                    "move_id": move_id,
                    "move_name": move_name,
                    "localized_move_name": localized_move_name,
                    "move_type": str(r["move_type"]),
                    "games": games,
                    "wins": wins,
                    "uses": uses,
                    "use_rate": (games / total_teams) if total_teams else 0.0,
                    "winrate": (wins / games) if games else 0.0,
                    "avg_elo": (sum_elo / games) if games else 0.0,
                }
            )

        reverse = (order != "asc")
        if sort == "uses":
            items.sort(key=lambda x: x["use_rate"], reverse=reverse)
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
                "meta": {"matches": matches, "min_games_default": min_games_default},
            }
        )

    @app.get("/api/teams")
    def api_teams():
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        lang = _normalize_lang(request.args.get("lang", "en") or "en")
        q = (request.args.get("q", "") or "").strip()
        selected_types = _parse_types_param(request.args.get("types", "") or "")
        sort = (request.args.get("sort", "popularity") or "popularity").strip().lower()
        order = (request.args.get("order", "desc") or "desc").strip().lower()
        limit = clamp_int(request.args.get("limit", DEFAULT_HOME_LIMIT), 10, 2000)
        combo_size = clamp_int(request.args.get("combo_size", 6), 2, 6)

        elo_min = clamp_int(request.args.get("elo_min", 0), 0, 10000)
        elo_max = clamp_int(request.args.get("elo_max", 10000), 0, 10000)
        if elo_min > elo_max:
            elo_min, elo_max = elo_max, elo_min

        matches = _matches_for_window(db_path, formatid, elo_min, elo_max)
        min_games_default = _recommended_min_games_from_matches(matches)
        min_games = clamp_int(request.args.get("min_games", min_games_default), 0, 10_000_000)

        return jsonify(
            _team_items_payload(
                teams_db_path=teams_db_path,
                formatid=formatid,
                lang=lang,
                q=q,
                selected_types=selected_types,
                required_member_keys=[x for x in (request.args.get("members", "") or "").split(",") if x.strip()],
                sort=sort,
                order=order,
                min_games=min_games,
                limit=limit,
                elo_min=elo_min,
                elo_max=elo_max,
                combo_size=combo_size,
            )
        )

    return app


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="stats.sqlite")
    ap.add_argument("--attacks_db", default="attacks.sqlite")
    ap.add_argument("--teams_db", default="teams.sqlite")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()

    app = make_app(args.db, attacks_db_path=args.attacks_db, teams_db_path=args.teams_db)
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
