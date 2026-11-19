# Contributing

Bug reports should include the Relay version, operating system, reproduction steps, and expected behavior. Replace tokens, endpoint secrets, and customer payloads with synthetic values.

For a change, open an issue describing the behavior first when it affects delivery semantics or the database schema. Small fixes can go directly to a pull request.

```sh
python -m venv .venv
# Activate the environment, then:
python -m pip install -e '.[dev]'
python -m pytest -q
python -m ruff format src tests examples
```

Keep queue mutations inside the store's transaction boundary. Network I/O must happen outside database transactions. Changes to retry, replay, signing, or lease handling need a regression test that checks the externally observable outcome.

The browser console has no build step. Its standalone simulation lives in `demo.js`; the server uses the same interface against authenticated API routes. Keep simulated state separate from server behavior and label it clearly.
