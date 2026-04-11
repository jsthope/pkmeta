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
import time
import unicodedata
from collections import Counter
from io import StringIO
from typing import Any, Dict, List, Optional, Set, Tuple
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
DEFAULT_HOME_FORMAT = "champions"
DEFAULT_HOME_LIMIT = 50
DEFAULT_MIN_USAGE_RATE = 0.005
DEFAULT_HOME_TEAM_SIZE = 6
DEFAULT_TEAM_MIN_GAMES = 100
SPECIAL_FORMAT_CHAMPIONS = "champions"
SPECIAL_FORMAT_BASE = {SPECIAL_FORMAT_CHAMPIONS: "all"}
# Snapshot of Bulbapedia's Pokemon Champions list, including regional forms,
# Mega Evolutions, and the other forms listed on the same page.
CHAMPIONS_ALLOWED_POKEMON_KEYS = frozenset(
    """
    venusaur charizard blastoise beedrill pidgeot arbok pikachu raichu raichualola clefable
    ninetales ninetalesalola arcanine arcaninehisui alakazam machamp victreebel slowbro slowbrogalar
    gengar kangaskhan starmie pinsir tauros taurospaldeaaqua taurospaldeablaze taurospaldeacombat
    gyarados ditto vaporeon jolteon flareon aerodactyl snorlax dragonite meganium typhlosion
    typhlosionhisui feraligatr ariados ampharos azumarill politoed espeon umbreon slowking
    slowkinggalar forretress steelix scizor heracross skarmory houndoom tyranitar pelipper gardevoir
    sableye aggron medicham manectric sharpedo camerupt torkoal altaria milotic castform banette
    chimecho absol glalie torterra infernape empoleon luxray roserade rampardos bastiodon lopunny
    spiritomb garchomp lucario hippowdon toxicroak abomasnow weavile rhyperior leafeon glaceon
    gliscor mamoswine gallade froslass rotom serperior emboar samurott samurotthisui watchog
    liepard simisage simisear simipour excadrill audino conkeldurr whimsicott krookodile cofagrigus
    garbodor zoroark zoroarkhisui reuniclus vanilluxe emolga chandelure beartic stunfisk
    stunfiskgalar golurk hydreigon volcarona chesnaught delphox greninja diggersby talonflame
    vivillon floetteeternal florges pangoro furfrou meowstic aegislash aromatisse slurpuff
    clawitzer heliolisk tyrantrum aurorus sylveon hawlucha dedenne goodra goodrahisui klefki
    trevenant gourgeist avalugg avalugghisui noivern decidueye decidueyehisui incineroar primarina
    toucannon crabominable lycanroc toxapex mudsdale araquanid salazzle tsareena oranguru passimian
    mimikyu drampa kommoo corviknight flapple appletun sandaconda polteageist hatterene mrrime
    runerigus alcremie morpeko dragapult wyrdeer kleavor basculegion sneasler meowscarada
    skeledirge quaquaval maushold mausholdfour garganacl armarouge ceruledge bellibolt scovillain
    espathra tinkaton palafin orthworm glimmora farigiraf kingambit sinistcha archaludon hydrapple
    venusaurmega charizardmegax charizardmegay blastoisemega beedrillmega pidgeotmega clefablemega
    alakazammega victreebelmega slowbromega gengarmega kangaskhanmega starmiemega pinsirmega
    gyaradosmega aerodactylmega dragonitemega meganiummega feraligatrmega ampharosmega steelixmega
    scizormega heracrossmega skarmorymega houndoommega tyranitarmega gardevoirmega sableyemega
    aggronmega medichammega manectricmega sharpedomega cameruptmega altariamega banettemega
    chimechomega absolmega glaliemega lopunnymega garchompmega lucariomega abomasnowmega
    gallademega froslassmega emboarmega excadrillmega audinomega chandeluremega golurkmega
    chesnaughtmega delphoxmega greninjamega floettemega hawluchamega crabominablemega drampamega
    scovillainmega glimmoramega castformsunny castformrainy castformsnowy rotomheat rotomwash
    rotomfrost rotomfan rotommow vivillonicysnow vivillonpolar vivillontundra vivilloncontinental
    vivillongarden vivillonelegant vivillonmodern vivillonmarine vivillonarchipelago
    vivillonhighplains vivillonsandstorm vivillonriver vivillonmonsoon vivillonsavanna vivillonsun
    vivillonocean vivillonjungle vivillonfancy vivillonpokeball furfrouheart furfroustar
    furfroudiamond furfroudebutante furfroumatron furfroudandy furfroulareine furfroukabuki
    furfroupharaoh meowsticf aegislashblade gourgeistsmall gourgeistlarge gourgeistsuper
    lycanrocmidnight lycanrocdusk alcremierubycream alcremiematchacream alcremiemintcream
    alcremielemoncream alcremiesaltedcream alcremierubyswirl alcremiecaramelswirl
    alcremierainbowswirl morpekohangry basculegionf palafinhero
    """.split()
)
CHAMPIONS_ALLOWED_POKEMON_KEY_LIST = tuple(sorted(CHAMPIONS_ALLOWED_POKEMON_KEYS))
_PICKER_MERGE_TO_BASE = {"minior", "florges", "squawkabilly", "pikachu"}
DATASET_SOURCE_NAME = "pokemon-showdown-replays"
DATASET_SOURCE_URL = "https://huggingface.co/datasets/HolidayOugi/pokemon-showdown-replays"
CHAMPIONS_SHEET_SOURCE_NAME = "VGCPastes Repository (Champions)"
CHAMPIONS_SHEET_VIEW_URL = "https://docs.google.com/spreadsheets/d/1axlwmzPA49rYkqXh7zHvAtSP-TKbM0ijGYBPRflLSWw/htmlview?usp=sharing&pru=AAABnZwziio*y1cRxcp4gB4n0X9zMwN2RA#gid=791705272"
CHAMPIONS_SHEET_GVIZ_URL = "https://docs.google.com/spreadsheets/d/1axlwmzPA49rYkqXh7zHvAtSP-TKbM0ijGYBPRflLSWw/gviz/tq?tqx=out:json&gid=791705272"
CHAMPIONS_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/1axlwmzPA49rYkqXh7zHvAtSP-TKbM0ijGYBPRflLSWw/export?format=csv&gid=791705272"
CHAMPIONS_SHEET_CACHE_TTL_SECONDS = 3600
CHAMPIONS_SHEET_REQUEST_TIMEOUT_SECONDS = 2
CHAMPIONS_SHEET_DISK_CACHE_PATH = os.environ.get("PKMETA_CHAMPIONS_SHEET_CACHE", "/tmp/pkmeta_champions_sheet_rows.json")
CHAMPIONS_SHEET_ITEM_COLUMNS = (7, 10, 13, 16, 19, 22)
CHAMPIONS_SHEET_MEMBER_COLUMNS = (37, 38, 39, 40, 41, 42)
CUSTOM_SPRITE_FALLBACKS: Dict[str, tuple[str, ...]] = {
    "floettemega": (
        "https://www.pokepedia.fr/images/5/50/M%C3%A9ga-Floette-LPZA.png",
    ),
}
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
_CHAMPIONS_SHEET_CACHE: Dict[str, Any] | None = None
_CHAMPIONS_SHEET_CACHE_AT = 0.0


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


def _mega_form_parts(key: str) -> Optional[Tuple[str, str]]:
    kid = _to_id(key)
    m = re.fullmatch(r"([a-z0-9]+?)mega([a-z0-9]*)", kid)
    if not m:
        return None
    return (m.group(1), m.group(2))


def _champions_pokemon_key_candidates(name: str, key: str = "") -> List[str]:
    out: List[str] = []

    def add(candidate: str) -> None:
        cid = _to_id(candidate)
        if cid and cid not in out:
            out.append(cid)

    raw_name = str(name or "").strip()

    tokens = [tok for tok in re.split(r"[\s\-]+", raw_name) if tok]
    if len(tokens) >= 2 and tokens[0].lower() == "mega":
        body = tokens[1:]
        if len(body) >= 2 and len(body[-1]) <= 2 and body[-1].isalnum():
            base = " ".join(body[:-1]).strip()
            suffix = body[-1].strip()
            if base:
                add(f"{base} mega {suffix}")
                add(base)
        else:
            full_base = " ".join(body).strip()
            if full_base:
                add(f"{full_base} mega")
                add(full_base)

    add(key)
    add(raw_name)

    return out


def sprite_urls(key: str, name: str) -> List[str]:
    out: List[str] = []
    seen_variants: Set[str] = set()

    def add_url(u: str) -> None:
        if u not in out:
            out.append(u)

    def add_variant(variant: str) -> None:
        slug = _dashify(variant)
        if not slug or slug in seen_variants:
            return
        seen_variants.add(slug)
        slug2 = _keep_only_first_dash(slug)
        add_url(f"{SPRITE_ANI}{slug}.gif")
        if slug2 != slug:
            add_url(f"{SPRITE_ANI}{slug2}.gif")
        for base in (SPRITE_HOME, SPRITE_GEN5):
            add_url(f"{base}{slug}.png")
            if slug2 != slug:
                add_url(f"{base}{slug2}.png")

    base_fallback_key = ""
    for candidate_key in _champions_pokemon_key_candidates(name, key):
        mega_parts = _mega_form_parts(candidate_key)
        if mega_parts:
            base_key, mega_suffix = mega_parts
            mega_slug = f"{base_key}-mega{mega_suffix}" if mega_suffix else f"{base_key}-mega"
            add_variant(mega_slug)
            if base_key:
                base_fallback_key = base_key
        for custom_url in CUSTOM_SPRITE_FALLBACKS.get(candidate_key, ()):
            add_url(custom_url)
        add_variant(candidate_key)

    add_variant(name)
    add_variant(key)

    if base_fallback_key:
        add_variant(base_fallback_key)

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
    conn = sqlite3.connect(db_path, timeout=10)
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


def _format_db_id(formatid: str) -> str:
    fmt = (formatid or "all").strip().lower()
    return SPECIAL_FORMAT_BASE.get(fmt, fmt)


def _special_format_allowed_keys(formatid: str) -> Optional[Set[str]]:
    fmt = (formatid or "").strip().lower()
    if fmt == SPECIAL_FORMAT_CHAMPIONS:
        return CHAMPIONS_ALLOWED_POKEMON_KEYS
    return None


def _special_format_allowed_key_list(formatid: str) -> tuple[str, ...]:
    fmt = (formatid or "").strip().lower()
    if fmt == SPECIAL_FORMAT_CHAMPIONS:
        return CHAMPIONS_ALLOWED_POKEMON_KEY_LIST
    return ()


def _is_pokemon_allowed_for_format(formatid: str, key: str) -> bool:
    allowed_keys = _special_format_allowed_keys(formatid)
    return allowed_keys is None or _to_id(key) in allowed_keys


def _read_text_url(url: str, timeout: int = 20) -> str:
    req = Request(url, headers={"User-Agent": "pkmeta/1.0"})
    with urlopen(req, timeout=timeout) as resp:
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


def _format_uses_champions_sheet(formatid: str) -> bool:
    return (formatid or "").strip().lower() == SPECIAL_FORMAT_CHAMPIONS


def _matches_search(qraw: str, qid: str, *values: str) -> bool:
    if not qraw:
        return True
    lowered = [str(v or "").lower() for v in values]
    if qid and any(qid in _to_id(v) for v in values if v):
        return True
    return any(qraw in v for v in lowered)


def _sheet_cell_text(cells: List[Any], index: int) -> str:
    if index < 0 or index >= len(cells):
        return ""
    cell = cells[index]
    if not isinstance(cell, dict):
        return ""
    if cell.get("f") is not None:
        return str(cell.get("f") or "").strip()
    if cell.get("v") is not None:
        return str(cell.get("v") or "").strip()
    return ""


def _champions_sheet_date(cells: List[Any], index: int) -> tuple[int, str]:
    display = _sheet_cell_text(cells, index)
    if index < 0 or index >= len(cells):
        return (0, display)
    cell = cells[index]
    if not isinstance(cell, dict):
        return (0, display)
    raw = str(cell.get("v") or "")
    m = re.fullmatch(r"Date\((\d+),(\d+),(\d+)\)", raw)
    if not m:
        for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
            try:
                parsed = time.strptime(display, fmt)
                return (parsed.tm_year * 10000 + parsed.tm_mon * 100 + parsed.tm_mday, display)
            except Exception:
                continue
        return (0, display)
    year = int(m.group(1))
    month = int(m.group(2)) + 1
    day = int(m.group(3))
    return (year * 10000 + month * 100 + day, display)


def _champions_sheet_csv_rows() -> List[Dict[str, Any]]:
    text = _read_text_url(CHAMPIONS_SHEET_CSV_URL, timeout=CHAMPIONS_SHEET_REQUEST_TIMEOUT_SECONDS)
    rows: List[Dict[str, Any]] = []
    for row in csv.reader(StringIO(text)):
        cells = []
        for value in row:
            value_str = str(value or "")
            cells.append({"v": value_str, "f": value_str} if value_str else None)
        rows.append({"c": cells})
    return rows


def _champions_sheet_table_rows() -> List[Dict[str, Any]]:
    text = _read_text_url(CHAMPIONS_SHEET_GVIZ_URL, timeout=CHAMPIONS_SHEET_REQUEST_TIMEOUT_SECONDS)
    m = re.search(r"setResponse\((.*)\);\s*$", text, re.S)
    if not m:
        raise ValueError("Could not parse Champions sheet response")
    payload = json.loads(m.group(1))
    return payload.get("table", {}).get("rows") or []


def _load_champions_sheet_rows_from_disk() -> List[Dict[str, Any]]:
    try:
        with open(CHAMPIONS_SHEET_DISK_CACHE_PATH, "r", encoding="utf-8") as fh:
            rows = json.load(fh)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def _store_champions_sheet_rows_to_disk(rows: List[Dict[str, Any]]) -> None:
    try:
        with open(CHAMPIONS_SHEET_DISK_CACHE_PATH, "w", encoding="utf-8") as fh:
            json.dump(rows, fh, ensure_ascii=True)
    except Exception:
        return None


def _empty_champions_sheet_data() -> Dict[str, Any]:
    return {
        "team_count": 0,
        "slot_count": 0,
        "teams": [],
        "pokemon_counts": {},
        "pokemon_names": {},
        "item_uses": {},
        "item_team_counts": {},
        "type_counts": {},
        "types": [],
        "used_keys": tuple(),
    }


def _mega_badge_for_name(name: str) -> str:
    nm = str(name or "")
    parts = [part for part in re.split(r"[\s\-]+", nm) if part]
    if len(parts) >= 2 and parts[0].lower() == "mega":
        if len(parts) >= 3 and len(parts[-1]) <= 2 and parts[-1].isalnum():
            return f"Mega {parts[-1].upper()}"
        return "Mega"
    if "-Mega-X" in nm:
        return "Mega X"
    if "-Mega-Y" in nm:
        return "Mega Y"
    if "-Mega" in nm:
        return "Mega"
    return ""


def _resolve_champions_member_identity(name: str, identity_map: Dict[str, Dict[str, Any]]) -> Tuple[str, str]:
    raw_name = str(name or "").strip()
    for candidate_key in _champions_pokemon_key_candidates(raw_name):
        info = identity_map.get(candidate_key)
        if info:
            return (candidate_key, str(info.get("name") or raw_name))
        if candidate_key in CHAMPIONS_ALLOWED_POKEMON_KEYS:
            return (candidate_key, raw_name)
    return (_to_id(raw_name), raw_name)


def _clean_champions_link_value(value: str) -> str:
    raw = str(value or "").strip()
    if not raw or raw == "-":
        return ""
    if raw.lower() == "discord submission":
        return ""
    if raw.startswith("//"):
        return f"https:{raw}"
    if not re.match(r"^[a-z][a-z0-9+.-]*://", raw, re.I) and re.match(r"^(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)+(?:[/?#].*)?$", raw, re.I):
        return f"https://{raw}"
    return raw


def _champions_sheet_raw_data() -> Dict[str, Any]:
    global _CHAMPIONS_SHEET_CACHE, _CHAMPIONS_SHEET_CACHE_AT
    now = time.time()
    if _CHAMPIONS_SHEET_CACHE is not None and (now - _CHAMPIONS_SHEET_CACHE_AT) < CHAMPIONS_SHEET_CACHE_TTL_SECONDS:
        return _CHAMPIONS_SHEET_CACHE

    try:
        rows = _champions_sheet_table_rows()
        _store_champions_sheet_rows_to_disk(rows)
    except Exception:
        try:
            rows = _champions_sheet_csv_rows()
            _store_champions_sheet_rows_to_disk(rows)
        except Exception:
            rows = _load_champions_sheet_rows_from_disk()
            if rows:
                pass
            elif _CHAMPIONS_SHEET_CACHE is not None:
                return _CHAMPIONS_SHEET_CACHE
            else:
                _CHAMPIONS_SHEET_CACHE = _empty_champions_sheet_data()
                _CHAMPIONS_SHEET_CACHE_AT = now
                return _CHAMPIONS_SHEET_CACHE

    pokedex_json_path = os.environ.get("PKMETA_POKEDEX_JSON", "")
    poke_type_map = load_pokedex_type_map(pokedex_json_path)
    identity_map = load_pokedex_identity_map(pokedex_json_path)
    teams: List[Dict[str, Any]] = []
    pokemon_counts: Counter[str] = Counter()
    item_uses: Counter[str] = Counter()
    item_team_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    pokemon_names: Dict[str, str] = {}
    used_keys: Set[str] = set()

    for row in rows:
        cells = row.get("c") or []
        team_id = _sheet_cell_text(cells, 0)
        if not team_id.startswith("PC"):
            continue

        slots: List[Dict[str, Any]] = []
        team_item_names: List[str] = []
        team_member_names: List[str] = []
        team_type_set: Set[str] = set()
        seen_items_this_team: Set[str] = set()

        for item_col, member_col in zip(CHAMPIONS_SHEET_ITEM_COLUMNS, CHAMPIONS_SHEET_MEMBER_COLUMNS):
            member_name = _sheet_cell_text(cells, member_col)
            item_name = _sheet_cell_text(cells, item_col)
            if not member_name:
                continue
            key, canonical_name = _resolve_champions_member_identity(member_name, identity_map)
            types = poke_type_map.get(key, poke_type_map.get(_to_id(member_name), []))
            mega_label = _mega_badge_for_name(canonical_name or member_name)
            slots.append(
                {
                    "key": key,
                    "name": canonical_name or member_name,
                    "sheet_name": member_name,
                    "item": item_name,
                    "types": types,
                    "mega_label": mega_label,
                }
            )
            team_member_names.append(member_name)
            if canonical_name and canonical_name != member_name:
                team_member_names.append(canonical_name)
            if item_name and item_name != "-":
                team_item_names.append(item_name)
                item_uses[item_name] += 1
                if item_name not in seen_items_this_team:
                    item_team_counts[item_name] += 1
                    seen_items_this_team.add(item_name)
            if key:
                pokemon_counts[key] += 1
                used_keys.add(key)
                pokemon_names.setdefault(key, canonical_name or member_name)
            for type_name in types:
                type_counts[type_name] += 1
                team_type_set.add(type_name)

        if not slots:
            continue

        date_sort, date_display = _champions_sheet_date(cells, 29)
        team_id_num_match = re.search(r"(\d+)$", team_id)
        team_id_num = int(team_id_num_match.group(1)) if team_id_num_match else 0
        search_values = [
            team_id,
            _sheet_cell_text(cells, 1),
            _sheet_cell_text(cells, 3),
            _sheet_cell_text(cells, 24),
            _sheet_cell_text(cells, 25),
            _sheet_cell_text(cells, 26),
            _sheet_cell_text(cells, 27),
            _sheet_cell_text(cells, 28),
            date_display,
            _sheet_cell_text(cells, 30),
            _sheet_cell_text(cells, 31),
            _sheet_cell_text(cells, 32),
            _sheet_cell_text(cells, 33),
            _sheet_cell_text(cells, 34),
            _sheet_cell_text(cells, 35),
            *team_item_names,
            *team_member_names,
        ]
        teams.append(
            {
                "team_id": team_id,
                "team_id_num": team_id_num,
                "description": _sheet_cell_text(cells, 1),
                "full_name": _sheet_cell_text(cells, 3),
                "pokepaste": _sheet_cell_text(cells, 24),
                "evs": _sheet_cell_text(cells, 25),
                "extracted_paste": _sheet_cell_text(cells, 26),
                "replica_status": _sheet_cell_text(cells, 27),
                "replica_code": _sheet_cell_text(cells, 28),
                "date": date_display,
                "date_sort": date_sort,
                "event": _sheet_cell_text(cells, 30),
                "rank": _sheet_cell_text(cells, 31),
                "source_link": _sheet_cell_text(cells, 32),
                "report_video": _sheet_cell_text(cells, 33),
                "other_links": _sheet_cell_text(cells, 34),
                "owner": _sheet_cell_text(cells, 35),
                "slots": slots,
                "member_keys": [slot["key"] for slot in slots if slot.get("key")],
                "member_key_set": {slot["key"] for slot in slots if slot.get("key")},
                "member_names": team_member_names,
                "item_names": team_item_names,
                "type_set": team_type_set,
                "search_blob": " ".join(v for v in search_values if v).lower(),
                "search_id": _to_id(" ".join(v for v in search_values if v)),
            }
        )

    teams.sort(key=lambda team: (team.get("date_sort", 0), team.get("team_id_num", 0)), reverse=True)
    types_sorted = [
        type_name
        for type_name, _ in sorted(
            type_counts.items(),
            key=lambda kv: (-kv[1], _TYPE_ORDER_INDEX.get(kv[0], 999), kv[0]),
        )
    ]

    _CHAMPIONS_SHEET_CACHE = {
        "team_count": len(teams),
        "slot_count": sum(len(team["slots"]) for team in teams),
        "teams": teams,
        "pokemon_counts": dict(pokemon_counts),
        "pokemon_names": pokemon_names,
        "item_uses": dict(item_uses),
        "item_team_counts": dict(item_team_counts),
        "type_counts": dict(type_counts),
        "types": types_sorted,
        "used_keys": tuple(sorted(used_keys)),
    }
    _CHAMPIONS_SHEET_CACHE_AT = now
    return _CHAMPIONS_SHEET_CACHE


def _champions_member_payload(slot: Dict[str, Any], lang: str) -> Dict[str, Any]:
    name = str(slot.get("name") or slot.get("key") or "")
    key = str(slot.get("key") or _to_id(name))
    item_name = str(slot.get("item") or "")
    localized_name_map = load_pokemon_localized_name_map(lang)
    item_localized_map = load_item_localized_name_map(lang)
    localized_name = localized_name_map.get(key, localized_name_map.get(_to_id(name), name))
    localized_item = item_localized_map.get(_to_id(item_name), item_name) if item_name else ""
    return {
        "key": key,
        "name": name,
        "localized_name": localized_name,
        "item": item_name,
        "localized_item": localized_item,
        "types": list(slot.get("types") or []),
        "sprite_urls": sprite_urls(key, name),
        "is_mega": bool(slot.get("mega_label")),
        "mega_label": str(slot.get("mega_label") or ""),
    }


def _champions_pokemon_payload(
    formatid: str,
    q: str,
    lang: str,
    selected_types: Set[str],
    sort: str,
    order: str,
    min_games: int,
    limit: int,
) -> Dict[str, Any]:
    raw = _champions_sheet_raw_data()
    total_teams = int(raw.get("team_count") or 0)
    localized_name_map = load_pokemon_localized_name_map(lang)
    poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
    qraw = (q or "").strip().lower()
    qid = _to_id(qraw)
    items: List[Dict[str, Any]] = []

    for key, games in raw.get("pokemon_counts", {}).items():
        count = int(games or 0)
        if count < min_games:
            continue
        name = str(raw.get("pokemon_names", {}).get(key) or key)
        localized_name = localized_name_map.get(key, localized_name_map.get(_to_id(name), name))
        types = poke_type_map.get(key, [])
        if selected_types and not selected_types.intersection(types):
            continue
        if not _matches_search(qraw, qid, name, localized_name, key):
            continue
        mega_label = _mega_badge_for_name(name)
        items.append(
            {
                "key": key,
                "name": name,
                "localized_name": localized_name,
                "games": count,
                "popularity": (count / total_teams) if total_teams else 0.0,
                "types": types,
                "sprite_urls": sprite_urls(key, name),
                "is_mega": bool(mega_label),
                "mega_label": mega_label,
            }
        )

    reverse = (order != "asc")
    if sort == "name":
        items.sort(key=lambda x: (x["localized_name"] or x["name"]).lower(), reverse=reverse)
    elif sort == "types":
        items.sort(key=lambda x: "/".join(x.get("types", [])).lower(), reverse=reverse)
    elif sort == "games":
        items.sort(key=lambda x: x["games"], reverse=reverse)
    else:
        items.sort(key=lambda x: x["popularity"], reverse=reverse)

    return {
        "formatid": formatid,
        "elo_min": 0,
        "elo_max": 0,
        "items": items[:limit],
        "meta": {
            "matches": total_teams,
            "min_games_default": 0,
            "winrate_warning": {"show": False, "message": ""},
        },
    }


def _champions_items_payload(
    formatid: str,
    q: str,
    lang: str,
    selected_types: Set[str],
    sort: str,
    order: str,
    min_games: int,
    limit: int,
) -> Dict[str, Any]:
    raw = _champions_sheet_raw_data()
    total_teams = int(raw.get("team_count") or 0)
    qraw = (q or "").strip().lower()
    qid = _to_id(qraw)
    item_localized_map = load_item_localized_name_map(lang)
    item_uses: Counter[str] = Counter()
    item_team_counts: Counter[str] = Counter()
    total_slots = 0

    for team in raw.get("teams", []):
        seen_items_this_team: Set[str] = set()
        for slot in team.get("slots", []):
            types = set(slot.get("types") or [])
            if selected_types and not selected_types.intersection(types):
                continue
            item_name = str(slot.get("item") or "")
            if not item_name or item_name == "-":
                continue
            total_slots += 1
            item_uses[item_name] += 1
            seen_items_this_team.add(item_name)
        for item_name in seen_items_this_team:
            item_team_counts[item_name] += 1

    items: List[Dict[str, Any]] = []
    for item_name, uses in item_uses.items():
        uses_int = int(uses or 0)
        if uses_int < min_games:
            continue
        localized_item = item_localized_map.get(_to_id(item_name), item_name)
        if not _matches_search(qraw, qid, item_name, localized_item):
            continue
        teams_with_item = int(item_team_counts.get(item_name, 0))
        items.append(
            {
                "item": item_name,
                "localized_item": localized_item,
                "uses": uses_int,
                "games": teams_with_item,
                "use_rate": (uses_int / total_slots) if total_slots else 0.0,
                "team_rate": (teams_with_item / total_teams) if total_teams else 0.0,
            }
        )

    reverse = (order != "asc")
    if sort == "item":
        items.sort(key=lambda x: (x["localized_item"] or x["item"]).lower(), reverse=reverse)
    elif sort == "games":
        items.sort(key=lambda x: x["games"], reverse=reverse)
    elif sort == "popularity":
        items.sort(key=lambda x: x["use_rate"], reverse=reverse)
    else:
        items.sort(key=lambda x: x["uses"], reverse=reverse)

    return {
        "formatid": formatid,
        "elo_min": 0,
        "elo_max": 0,
        "items": items[:limit],
        "meta": {"matches": total_teams, "min_games_default": 0},
    }


def _champions_team_payload(
    formatid: str,
    q: str,
    lang: str,
    selected_types: Set[str],
    required_member_keys: List[str],
    sort: str,
    order: str,
    limit: int,
) -> Dict[str, Any]:
    raw = _champions_sheet_raw_data()
    total_teams = int(raw.get("team_count") or 0)
    qraw = (q or "").strip().lower()
    qid = _to_id(qraw)
    required_keys = {str(key or "").strip() for key in required_member_keys if str(key or "").strip()}
    items: List[Dict[str, Any]] = []

    for team in raw.get("teams", []):
        if required_keys and not required_keys.issubset(team.get("member_key_set", set())):
            continue
        if selected_types and not selected_types.intersection(team.get("type_set", set())):
            continue
        if not _matches_search(
            qraw,
            qid,
            team.get("team_id", ""),
            team.get("description", ""),
            team.get("full_name", ""),
            team.get("owner", ""),
            team.get("replica_code", ""),
            team.get("source_link", ""),
            team.get("report_video", ""),
            team.get("other_links", ""),
            *team.get("member_names", []),
            *team.get("item_names", []),
        ):
            continue

        members = [_champions_member_payload(slot, lang) for slot in team.get("slots", [])]
        member_summary = " / ".join(member.get("localized_name") or member.get("name") or "-" for member in members)
        source_link = _clean_champions_link_value(str(team.get("source_link") or ""))
        report_video = _clean_champions_link_value(str(team.get("report_video") or ""))
        other_links = _clean_champions_link_value(str(team.get("other_links") or ""))
        links = []
        if source_link:
            links.append({"label": "Source", "url": source_link})
        if report_video:
            links.append({"label": "Video", "url": report_video})
        if other_links:
            links.append({"label": "Other", "url": other_links})
        items.append(
            {
                "team_id": team.get("team_id", ""),
                "label": team.get("description", "") or member_summary,
                "description": team.get("description", ""),
                "member_summary": member_summary,
                "full_name": team.get("full_name", ""),
                "owner": team.get("owner", ""),
                "date": team.get("date", ""),
                "date_sort": int(team.get("date_sort") or 0),
                "event": team.get("event", ""),
                "rank": team.get("rank", ""),
                "replica_code": team.get("replica_code", ""),
                "replica_status": team.get("replica_status", ""),
                "pokepaste": team.get("pokepaste", ""),
                "evs": team.get("evs", ""),
                "extracted_paste": team.get("extracted_paste", ""),
                "source_link": source_link,
                "report_video": report_video,
                "other_links": other_links,
                "links": links,
                "members": members,
                "combo_size": len(members),
                "team_id_num": int(team.get("team_id_num") or 0),
            }
        )

    reverse = (order != "asc")
    if sort == "name":
        items.sort(key=lambda x: (x.get("label") or "").lower(), reverse=reverse)
    elif sort == "owner":
        items.sort(key=lambda x: (x.get("owner") or "").lower(), reverse=reverse)
    elif sort == "code":
        items.sort(key=lambda x: (x.get("replica_code") or "").lower(), reverse=reverse)
    elif sort == "source":
        items.sort(key=lambda x: (x.get("source_link") or "").lower(), reverse=reverse)
    elif sort == "team_id":
        items.sort(key=lambda x: int(x.get("team_id_num") or 0), reverse=reverse)
    else:
        items.sort(key=lambda x: (int(x.get("date_sort") or 0), int(x.get("team_id_num") or 0)), reverse=reverse)

    return {
        "formatid": formatid,
        "elo_min": 0,
        "elo_max": 0,
        "items": items[:limit],
        "meta": {"matches": total_teams, "min_games_default": 0},
    }


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
    db_formatid = _format_db_id(formatid)
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            """
            SELECT
              COALESCE((SELECT SUM(matches) FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?), 0) AS matches,
              COALESCE((SELECT SUM(brought) FROM pokemon_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?), 0) AS brought,
              COALESCE((SELECT SUM(wins) FROM pokemon_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?), 0) AS wins
            """,
            (db_formatid, elo_min, elo_max, db_formatid, elo_min, elo_max, db_formatid, elo_min, elo_max),
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
    db_formatid = _format_db_id(formatid)
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (db_formatid, elo_min, elo_max),
        ).fetchone()
    finally:
        conn.close()
    return int(row["m"]) if row else 0


def _resolve_teams_db_path(teams_db_path: str, formatid: str) -> Optional[str]:
    db_formatid = _format_db_id(formatid)
    if not teams_db_path:
        return None
    if os.path.isdir(teams_db_path):
        candidate = os.path.join(teams_db_path, f"{db_formatid}.sqlite")
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


def _picker_key_and_name(key: str, name: str) -> Tuple[str, str]:
    clean_key = _to_id(key)
    clean_name = (name or key or "").strip()
    if not clean_key:
        return ("", "")
    base_name = clean_name.split("-", 1)[0].strip() or clean_name
    base_key = _to_id(base_name)
    if base_key in _PICKER_MERGE_TO_BASE:
        return (base_key, base_name)
    return (clean_key, clean_name)


def _pokemon_picker_options(lang: str, formatid: str = "") -> List[Dict[str, Any]]:
    lang_norm = _normalize_lang(lang)
    format_norm = (formatid or "").strip().lower()
    cache_key = f"{lang_norm}|{format_norm}"
    cached = _POKEMON_PICKER_OPTION_CACHE.get(cache_key)
    if cached is not None:
        return cached

    if _format_uses_champions_sheet(format_norm):
        raw = _champions_sheet_raw_data()
        localized_name_map = load_pokemon_localized_name_map(lang_norm)
        poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
        options: List[Dict[str, Any]] = []
        for key in raw.get("used_keys", ()): 
            name = str(raw.get("pokemon_names", {}).get(key) or key)
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
        _POKEMON_PICKER_OPTION_CACHE[cache_key] = options
        return options

    identity_map = load_pokedex_identity_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
    localized_name_map = load_pokemon_localized_name_map(lang_norm)
    poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
    options_by_key: Dict[str, Dict[str, Any]] = {}
    for key, info in identity_map.items():
        if not key:
            continue
        name = str(info.get("name") or key)
        picker_key, picker_name = _picker_key_and_name(str(key), name)
        if not picker_key:
            continue
        localized_name = localized_name_map.get(picker_key, localized_name_map.get(_to_id(picker_name), picker_name))
        if picker_key in options_by_key:
            continue
        options_by_key[picker_key] = {
            "key": picker_key,
            "name": picker_name,
            "localized_name": localized_name,
            "types": poke_type_map.get(picker_key, []),
            "sprite_urls": sprite_urls(picker_key, picker_name),
        }

    options = list(options_by_key.values())
    allowed_keys = _special_format_allowed_keys(format_norm)
    if allowed_keys is not None:
        options = [opt for opt in options if str(opt.get("key") or "") in allowed_keys]
    options.sort(key=lambda x: (_to_id(x["localized_name"]), _to_id(x["name"])))
    _POKEMON_PICKER_OPTION_CACHE[cache_key] = options
    return options


def _available_formats(db_path: str) -> List[str]:
    conn = get_conn(db_path)
    try:
        rows = conn.execute("SELECT DISTINCT formatid FROM matches_bucket ORDER BY formatid").fetchall()
    finally:
        conn.close()
    formats = [str(r["formatid"]) for r in rows if r["formatid"] is not None]
    if "all" in formats and SPECIAL_FORMAT_CHAMPIONS not in formats:
        formats = [SPECIAL_FORMAT_CHAMPIONS, *formats]
    return formats


def _default_home_format(formats: List[str]) -> str:
    if DEFAULT_HOME_FORMAT in formats:
        return DEFAULT_HOME_FORMAT
    if "all" in formats:
        return "all"
    return formats[0] if formats else "all"


def _elo_bounds_for_format(db_path: str, formatid: str) -> Dict[str, int]:
    db_formatid = _format_db_id(formatid)
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT MIN(elo_bucket) AS mn, MAX(elo_bucket) AS mx FROM matches_bucket WHERE formatid=?",
            (db_formatid,),
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
    db_formatid = _format_db_id(formatid)
    allowed_keys = _special_format_allowed_key_list(formatid)
    conn = get_conn(db_path)
    try:
        matches_row = conn.execute(
            "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (db_formatid, elo_min, elo_max),
        ).fetchone()
        denom_row = conn.execute(
            "SELECT COALESCE(SUM(games),0) AS s FROM pokemon_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (db_formatid, elo_min, elo_max),
        ).fetchone()
        query_sql = """
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
        """
        params: List[Any] = [db_formatid, elo_min, elo_max]
        if allowed_keys:
            qs = ",".join("?" for _ in allowed_keys)
            query_sql += f" AND key IN ({qs})"
            params.extend(allowed_keys)
        query_sql += """
            GROUP BY key
            HAVING SUM(games) >= ?
            ORDER BY SUM(games) DESC
            LIMIT ?
        """
        params.extend([min_games, limit])
        rows = conn.execute(query_sql, params).fetchall()
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
    db_formatid = _format_db_id(formatid)
    allowed_keys = _special_format_allowed_key_list(formatid)

    conn = get_conn(db_path)
    try:
        query_sql = """
            SELECT key, SUM(games) AS games
            FROM pokemon_bucket
            WHERE formatid=? AND elo_bucket BETWEEN ? AND ?
        """
        params: List[Any] = [db_formatid, elo_min, elo_max]
        if allowed_keys:
            qs = ",".join("?" for _ in allowed_keys)
            query_sql += f" AND key IN ({qs})"
            params.extend(allowed_keys)
        query_sql += " GROUP BY key"
        rows = conn.execute(query_sql, params).fetchall()
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
            (db_formatid, elo_min, elo_max),
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
    db_formatid = _format_db_id(formatid)

    conn = get_conn(attacks_db_path)
    try:
        matches_row = conn.execute(
            "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
            (db_formatid, elo_min, elo_max),
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
        params: List[Any] = [db_formatid, elo_min, elo_max]
        if selected_types:
            qs = ",".join("?" for _ in selected_types)
            q_sql += f" AND move_type IN ({qs})"
            params.extend(sorted(selected_types))
        q_sql += " GROUP BY move_id HAVING SUM(games) >= ?"
        params.append(min_games)

        rows = conn.execute(q_sql, params).fetchall()
    except sqlite3.OperationalError:
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


def _champions_home_page_context(db_path: str, lang: str) -> Dict[str, Any]:
    formats = _available_formats(db_path)
    footer_copy = _footer_copy_for_lang(lang)
    raw = _champions_sheet_raw_data()
    team_count = int(raw.get("team_count") or 0)
    champions_limit = team_count if team_count > 0 else DEFAULT_HOME_LIMIT
    teams = _champions_team_payload(
        formatid=SPECIAL_FORMAT_CHAMPIONS,
        q="",
        lang=lang,
        selected_types=set(),
        required_member_keys=[],
        sort="date",
        order="desc",
        limit=champions_limit,
    )
    pokemon = _champions_pokemon_payload(
        formatid=SPECIAL_FORMAT_CHAMPIONS,
        q="",
        lang=lang,
        selected_types=set(),
        sort="games",
        order="desc",
        min_games=0,
        limit=champions_limit,
    )
    items = _champions_items_payload(
        formatid=SPECIAL_FORMAT_CHAMPIONS,
        q="",
        lang=lang,
        selected_types=set(),
        sort="uses",
        order="desc",
        min_games=0,
        limit=champions_limit,
    )
    team_rows_by_size = {str(DEFAULT_HOME_TEAM_SIZE): teams["items"]}
    return {
        "formats": formats,
        "formatid": SPECIAL_FORMAT_CHAMPIONS,
        "elo_min": 0,
        "elo_max": 0,
        "elo_step": 1,
        "matches": team_count,
        "matches_display": _human_int(team_count),
        "min_games": 0,
        "min_games_display": "0",
        "limit": champions_limit,
        "limit_max": champions_limit,
        "rows": pokemon["items"],
        "attack_rows": items["items"],
        "team_rows": teams["items"],
        "source_name": CHAMPIONS_SHEET_SOURCE_NAME,
        "source_url": CHAMPIONS_SHEET_VIEW_URL,
        "footer": footer_copy,
        "pokemon_winrate_warning": {"show": False, "message": ""},
        "champions_mode": True,
        "default_view": "teams",
        "initial_state": {
            "formats": formats,
            "default_format": SPECIAL_FORMAT_CHAMPIONS,
            "elo_min": 0,
            "elo_max": 0,
            "elo_step": 1,
            "types": list(raw.get("types") or []),
            "matches": team_count,
            "active_view": "teams",
            "champions_mode": True,
            "pokemon": {
                "min_games": 0,
                "sort": "games",
                "order": "desc",
                "limit": champions_limit,
                "preloaded": True,
                "min_games_auto": True,
                "winrate_warning": {"show": False, "message": ""},
            },
            "attacks": {"min_games": 0, "sort": "uses", "order": "desc", "limit": champions_limit, "preloaded": True, "min_games_auto": True},
            "teams": {
                "min_games": 0,
                "sort": "date",
                "order": "desc",
                "limit": champions_limit,
                "combo_size": DEFAULT_HOME_TEAM_SIZE,
                "preloaded": True,
                "min_games_auto": True,
                "by_size": team_rows_by_size,
            },
            "pokemon_picker_options": _pokemon_picker_options(lang, SPECIAL_FORMAT_CHAMPIONS),
            "limit": champions_limit,
            "limit_max": champions_limit,
        },
    }


def _home_page_context(db_path: str, attacks_db_path: str, teams_db_path: str, lang: str) -> Dict[str, Any]:
    formats = _available_formats(db_path)
    formatid = _default_home_format(formats)
    if _format_uses_champions_sheet(formatid):
        return _champions_home_page_context(db_path, lang)
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
        "limit_max": 2000,
        "rows": pokemon["items"],
        "attack_rows": attacks["items"],
        "team_rows": team_rows_by_size[str(DEFAULT_HOME_TEAM_SIZE)],
        "source_name": DATASET_SOURCE_NAME,
        "source_url": DATASET_SOURCE_URL,
        "footer": footer_copy,
        "pokemon_winrate_warning": pokemon_winrate_warning,
        "champions_mode": False,
        "default_view": "pokemon",
        "initial_state": {
            "formats": formats,
            "default_format": formatid,
            "elo_min": elo_bounds["min"],
            "elo_max": elo_bounds["max"],
            "elo_step": elo_bounds["step"],
            "types": initial_types,
            "matches": pokemon["matches"],
            "active_view": "pokemon",
            "champions_mode": False,
            "pokemon": {
                "min_games": default_min_games,
                "sort": "popularity",
                "order": "desc",
                "limit": DEFAULT_HOME_LIMIT,
                "preloaded": True,
                "min_games_auto": True,
                "winrate_warning": pokemon_winrate_warning,
            },
            "attacks": {"min_games": default_min_games, "sort": "uses", "order": "desc", "limit": DEFAULT_HOME_LIMIT, "preloaded": True, "min_games_auto": True},
            "teams": {
                "min_games": default_team_min_games,
                "sort": "popularity",
                "order": "desc",
                "limit": DEFAULT_HOME_LIMIT,
                "combo_size": DEFAULT_HOME_TEAM_SIZE,
                "preloaded": True,
                "min_games_auto": True,
                "by_size": team_rows_by_size,
            },
            "pokemon_picker_options": _pokemon_picker_options(lang, formatid),
            "limit": DEFAULT_HOME_LIMIT,
            "limit_max": 2000,
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
    db_formatid = _format_db_id(formatid)
    allowed_keys = _special_format_allowed_keys(formatid)
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
        (db_formatid, elo_min, elo_max),
    ).fetchone()
    matches = int(matches_row["m"]) if matches_row else 0

    rows = conn.execute(
        """
        SELECT combo_key, SUM(games) AS games, SUM(wins) AS wins, SUM(sum_elo) AS sum_elo
        FROM combo_bucket
        WHERE formatid=? AND combo_size=? AND elo_bucket BETWEEN ? AND ?
        GROUP BY combo_key
        """,
        (db_formatid, combo_size, elo_min, elo_max),
    ).fetchall()
    if allowed_keys is not None:
        rows = [
            r
            for r in rows
            if all(member in allowed_keys for member in str(r["combo_key"]).split("|") if member)
        ]

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
        return jsonify({"formats": _available_formats(db_path)})

    @app.get("/api/elo_bounds")
    def api_elo_bounds():
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        if _format_uses_champions_sheet(formatid):
            return jsonify({"min": 0, "max": 0, "step": 1})
        db_formatid = _format_db_id(formatid)
        conn = get_conn(db_path)
        try:
            row = conn.execute(
                "SELECT MIN(elo_bucket) AS mn, MAX(elo_bucket) AS mx FROM matches_bucket WHERE formatid=?",
                (db_formatid,),
            ).fetchone()
        finally:
            conn.close()
        mn = int(row["mn"] if row and row["mn"] is not None else 0)
        mx = int(row["mx"] if row and row["mx"] is not None else 2000)
        return jsonify({"min": mn, "max": mx, "step": 100})

    @app.get("/api/types")
    def api_types():
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        if _format_uses_champions_sheet(formatid):
            raw = _champions_sheet_raw_data()
            return jsonify({"types": list(raw.get("types") or [])})
        db_formatid = _format_db_id(formatid)
        allowed_keys = _special_format_allowed_key_list(formatid)
        elo_min = clamp_int(request.args.get("elo_min", 0), 0, 10000)
        elo_max = clamp_int(request.args.get("elo_max", 10000), 0, 10000)
        if elo_min > elo_max:
            elo_min, elo_max = elo_max, elo_min

        type_counts: Dict[str, int] = {}
        poke_type_map = load_pokedex_type_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))

        conn = get_conn(db_path)
        query_sql = """
            SELECT key, SUM(games) AS games
            FROM pokemon_bucket
            WHERE formatid=? AND elo_bucket BETWEEN ? AND ?
        """
        params: List[Any] = [db_formatid, elo_min, elo_max]
        if allowed_keys:
            qs = ",".join("?" for _ in allowed_keys)
            query_sql += f" AND key IN ({qs})"
            params.extend(allowed_keys)
        query_sql += " GROUP BY key"
        rows = conn.execute(query_sql, params).fetchall()
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
                (db_formatid, elo_min, elo_max),
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
        if _format_uses_champions_sheet(formatid):
            q = (request.args.get("q", "") or "").strip().lower()
            lang = _normalize_lang(request.args.get("lang", "en") or "en")
            selected_types = _parse_types_param(request.args.get("types", "") or "")
            sort = (request.args.get("sort", "games") or "games").strip().lower()
            order = (request.args.get("order", "desc") or "desc").strip().lower()
            min_games = clamp_int(request.args.get("min_games", 0), 0, 10_000_000)
            limit = clamp_int(request.args.get("limit", DEFAULT_HOME_LIMIT), 10, 2000)
            return jsonify(
                _champions_pokemon_payload(
                    formatid=formatid,
                    q=q,
                    lang=lang,
                    selected_types=selected_types,
                    sort=sort,
                    order=order,
                    min_games=min_games,
                    limit=limit,
                )
            )
        db_formatid = _format_db_id(formatid)
        allowed_keys = _special_format_allowed_key_list(formatid)
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
        try:
            matches_row = conn.execute(
                "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
                (db_formatid, elo_min, elo_max),
            ).fetchone()
            matches = int(matches_row["m"]) if matches_row else 0
            min_games_default = _recommended_min_games_from_matches(matches)
            min_games = clamp_int(request.args.get("min_games", min_games_default), 0, 10_000_000)
            winrate_warning = _pokemon_winrate_warning_meta(db_path, formatid, elo_min, elo_max)

            query_sql = """
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
            """
            params: List[Any] = [db_formatid, elo_min, elo_max]
            if allowed_keys:
                qs = ",".join("?" for _ in allowed_keys)
                query_sql += f" AND key IN ({qs})"
                params.extend(allowed_keys)
            query_sql += """
                GROUP BY key
                HAVING SUM(games) >= ?
            """
            params.append(min_games)
            rows = conn.execute(query_sql, params).fetchall()
        finally:
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
        formatid = (request.args.get("formatid", "") or "").strip().lower()
        return jsonify({"items": _pokemon_picker_options(lang, formatid)})

    @app.get("/api/pokemon/<key>/detail")
    def api_pokemon_detail(key: str):
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        if _format_uses_champions_sheet(formatid):
            return jsonify({"error": "not available"}), 404
        db_formatid = _format_db_id(formatid)
        if not _is_pokemon_allowed_for_format(formatid, key):
            return jsonify({"error": "not found"}), 404
        lang = _normalize_lang(request.args.get("lang", "en") or "en")
        elo_min = clamp_int(request.args.get("elo_min", 0), 0, 10000)
        elo_max = clamp_int(request.args.get("elo_max", 10000), 0, 10000)
        if elo_min > elo_max:
            elo_min, elo_max = elo_max, elo_min

        conn = get_conn(db_path)
        try:
            matches_row = conn.execute(
                "SELECT COALESCE(SUM(matches),0) AS m FROM matches_bucket WHERE formatid=? AND elo_bucket BETWEEN ? AND ?",
                (db_formatid, elo_min, elo_max),
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
                (db_formatid, elo_min, elo_max, key),
            ).fetchone()
    
            if not base or base["games"] is None:
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
                        (db_formatid, key, elo_min, elo_max),
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
                        (db_formatid, key),
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
                (db_formatid, elo_min, elo_max, key, key),
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
                if not _is_pokemon_allowed_for_format(formatid, other):
                    continue
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
                (db_formatid, elo_min, elo_max, key),
            ).fetchall()
    
            counters = []
            for r in vs_rows:
                g = int(r["g"])
                if g < min_vs_games:
                    continue
                w = int(r["w"])
                b = str(r["b"])
                if not _is_pokemon_allowed_for_format(formatid, b):
                    continue
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
                    (db_formatid, *other_keys),
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
            else:
                available_formats = ["all"]
            if _is_pokemon_allowed_for_format(SPECIAL_FORMAT_CHAMPIONS, key):
                available_formats = [SPECIAL_FORMAT_CHAMPIONS, *available_formats]

            moves_rows = conn.execute(
                "SELECT move, uses FROM pokemon_moves WHERE formatid=? AND key=? ORDER BY uses DESC",
                (db_formatid, key),
            ).fetchall()

            items_rows = conn.execute(
                "SELECT item, uses FROM pokemon_items WHERE formatid=? AND key=? ORDER BY uses DESC",
                (db_formatid, key),
            ).fetchall()
    
            abilities_map = load_pokedex_abilities_map(os.environ.get("PKMETA_POKEDEX_JSON", ""))
            expected_abilities = abilities_map.get(key, abilities_map.get(_to_id(name), []))
            expected_set = set(expected_abilities)
            ability_localized_map = load_ability_localized_name_map(lang)
    
            abilities_payload: List[Dict[str, Any]] = []
            try:
                ab_rows = conn.execute(
                    "SELECT ability, uses FROM pokemon_abilities WHERE formatid=? AND key=? ORDER BY uses DESC",
                    (db_formatid, key),
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
    
        finally:
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
        db_formatid = _format_db_id(formatid)
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
                (db_formatid, elo_min, elo_max),
            ).fetchall()
        except sqlite3.OperationalError:
            return jsonify({"types": []})
        finally:
            conn.close()
        types = [str(r["move_type"]) for r in rows if r["move_type"] is not None]
        return jsonify({"types": types})

    @app.get("/api/attacks")
    def api_attacks():
        formatid = (request.args.get("formatid", "all") or "all").strip().lower()
        if _format_uses_champions_sheet(formatid):
            q = (request.args.get("q", "") or "").strip().lower()
            lang = _normalize_lang(request.args.get("lang", "en") or "en")
            selected_types = _parse_types_param(request.args.get("types", "") or request.args.get("type", "") or "")
            sort = (request.args.get("sort", "uses") or "uses").strip().lower()
            order = (request.args.get("order", "desc") or "desc").strip().lower()
            min_games = clamp_int(request.args.get("min_games", 0), 0, 10_000_000)
            limit = clamp_int(request.args.get("limit", DEFAULT_HOME_LIMIT), 10, 2000)
            return jsonify(
                _champions_items_payload(
                    formatid=formatid,
                    q=q,
                    lang=lang,
                    selected_types=selected_types,
                    sort=sort,
                    order=order,
                    min_games=min_games,
                    limit=limit,
                )
            )
        db_formatid = _format_db_id(formatid)
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
                (db_formatid, elo_min, elo_max),
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
            params: List[Any] = [db_formatid, elo_min, elo_max]
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
        required_member_keys = [x for x in (request.args.get("members", "") or "").split(",") if x.strip()]

        if _format_uses_champions_sheet(formatid):
            return jsonify(
                _champions_team_payload(
                    formatid=formatid,
                    q=q,
                    lang=lang,
                    selected_types=selected_types,
                    required_member_keys=required_member_keys,
                    sort=(sort if sort not in {"popularity", "games", "avg_elo", "winrate"} else "date"),
                    order=order,
                    limit=limit,
                )
            )

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
                required_member_keys=required_member_keys,
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
