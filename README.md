# Pkmeta

Various statistics on the current showdown meta :)

## Data source

Replay data used to build the database comes from:

- https://huggingface.co/datasets/jakegrigsby/metamon-raw-replays

Use the parquet files from that dataset (default path: `metamon-raw-replays/data`) to generate `stats.sqlite` and `attacks.sqlite`.

## Build the database

Install dependencies:

```bash
pip install -r requirements.txt
```

Generate the SQLite database from parquet files:

```bash
python3 tools/build_stats.py --out stats.sqlite
python3 tools/build_attack_stats.py --out attacks.sqlite
python3 tools/build_team_stats.py --out teams.sqlite
```

## Run the app

```bash
python3 app.py --db stats.sqlite --attacks_db attacks.sqlite --teams_db teams.sqlite --host 127.0.0.1 --port 8000
```

Then open `http://127.0.0.1:8000`.

## Contributing

Contributions are welcome. If you want to improve the project, feel free to open an issue or submit a pull request.
