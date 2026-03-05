# Docker profiles

- `debug/`: local debug, no SSL, app exposed on `http://localhost:8000`
- `deploy/`: production with Caddy (HTTPS + redirect)

## Debug

```bash
docker compose -f docker/debug/docker-compose.yml up -d --build
```

## Deploy

```bash
docker compose -f docker/deploy/docker-compose.yml up -d --build
```

DNS must point `pkmeta.net` and `www.pkmeta.net` to the server for automatic TLS.
