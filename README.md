# chain-indexer

Multi-chain block indexer with pluggable notification channels.

## Quick start

```bash
make install
cp config.example.yaml config.yaml
make migrate
make web    # in one terminal
make worker # in another
```

See `docs/superpowers/specs/` for design.
