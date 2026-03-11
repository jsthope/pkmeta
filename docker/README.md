# Docker profiles

- `debug/`: local debug, no SSL, app exposed on `http://localhost:8000`
- `deploy/`: production with Caddy (HTTPS + redirect)

## Debug

```bash
docker compose -f docker/debug/docker-compose.yml up -d --build
```

The app mounts `stats.sqlite`, `attacks.sqlite`, and `teams.sqlite` into the container.

## Deploy

```bash
docker compose -f docker/deploy/docker-compose.yml up -d --build
```

DNS must point `pokemonchampionsmeta.net`, `www.pokemonchampionsmeta.net`, `pkmeta.net`, and `www.pkmeta.net` to the server for automatic TLS.
