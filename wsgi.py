#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os

from app import make_app


DB_PATH = os.environ.get("PKMETA_DB", "stats.sqlite")
ATTACKS_DB_PATH = os.environ.get("PKMETA_ATTACKS_DB", "attacks.sqlite")
TEAMS_DB_PATH = os.environ.get("PKMETA_TEAMS_DB", "teams.sqlite")
app = make_app(DB_PATH, attacks_db_path=ATTACKS_DB_PATH, teams_db_path=TEAMS_DB_PATH)
