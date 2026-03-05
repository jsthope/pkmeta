#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os

from app import make_app


DB_PATH = os.environ.get("PKMETA_DB", "stats.sqlite")
app = make_app(DB_PATH)
