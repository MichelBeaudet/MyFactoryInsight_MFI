# Architecture Comparison & Development Roadmap

This document covers all four active projects: **MyFactoryInsight_MFI**, **MySportPredictor**, **MyUrbanLens**, **MyModalAnalyzer**.

---

## The Four Projects at a Glance

| | MFI | MySportPredictor | MyUrbanLens | MyModalAnalyzer |
|---|---|---|---|---|
| **Purpose** | Industrial IoT platform | Sports prediction | Urban geodata platform | Fourier analysis tool |
| **Language** | Python 3.12+ | Python 3.x | Node.js | Python 3.12+ |
| **Framework** | FastAPI + Pydantic | None | Native `http` | FreeSimpleGUI |
| **Data Store** | In-memory + InfluxDB (opt) | CSV/in-memory | PostgreSQL | In-memory |
| **Architecture** | 11-file phased pipeline | 6-phase, 40+ files | Layered routes/services/DB | Single file, 9 phases |
| **UI** | Browser + Mobile PWA | Browser (359KB JS) | Browser SPA | Desktop Tkinter |
| **Testing** | `--self-test` per phase | Phase-5 leaderboard | `npm test` (grants only) | Built-in QA |
| **CI/CD** | None | None | None | None |

---

## What All Four Projects Share

These patterns appear in every project and should be preserved and replicated in new work:

1. **Phase-based code organization** — data flow is divided into named, sequential phases. This is a clear strength: any developer can understand the pipeline by reading phase names.
2. **Minimal external dependencies** — each project uses only what it needs. UrbanLens has 2 deps; ModalAnalyzer has 3. Avoids dependency hell.
3. **Self-validating design** — every project ships its own correctness check without relying on a separate test framework.
4. **Custom logging infrastructure** — all projects have a well-configured logger with timestamps, levels, and function context.
5. **In-memory state first** — persistence is optional or deferred. Enables fast dev and testing.

---

## Architecture Divergences

### Code Organization Scale
- **MyModalAnalyzer**: Single file (23KB) — appropriate for a focused tool.
- **MFI**: One file per phase, flat dir (12 files) — clean and readable.
- **MySportPredictor**: 40+ flat files — the flat structure breaks down at this scale; finding related files requires knowing naming conventions.
- **MyUrbanLens**: Well-structured subdirs (`config/`, `routes/`, `services/`, `extractors/`, `normalizers/`, `database/`) — the most mature structure of the four. **Use this as the model for new projects.**

### Data Validation
- **MFI**: Pydantic v2 throughout — excellent type safety.
- **MySportPredictor**: Dict-based configs and DataFrames — flexible but no schema enforcement, silent errors possible.
- **MyUrbanLens**: Manual validation in `security_helpers.js` — adequate but no schema library.
- **MyModalAnalyzer**: Frozen dataclass config — well-suited for its scope.

### Frontend
- **MFI**: FastAPI serves HTML — clean for a tool-style app.
- **UrbanLens**: Vanilla JS SPA — clean and minimal.
- **MySportPredictor**: `ui/app.js` at **359KB is the largest technical debt item** in the codebase. A single JS file this size is very hard to maintain or extend.
- **MyModalAnalyzer**: Desktop Tkinter — appropriate for its purpose.

---

## Development Roadmap (Prioritized)

### Priority 1 — Add CI/CD to All Four Projects

None of the four repos run tests automatically on push. This is the single highest-leverage improvement.

Add `.github/workflows/ci.yml` to each repo:

```yaml
# .github/workflows/ci.yml (Python projects)
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.12' }
      - run: pip install -r requirements.txt
      - run: python MyFactoryInsight_MFI.py --self-test
```

```yaml
# .github/workflows/ci.yml (Node.js)
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '20' }
      - run: npm install
      - run: npm test
```

### Priority 2 — Reorganize MySportPredictor Into Subdirectories

At 40+ flat files, the project needs subdirectories. This is a directory rename only — no logic changes:

```
mysportpredictor/
├── data/         # nba_data.py, nfl_data.py, mlb_data.py, nhl_data.py
├── engines/      # mysport_engine_*.py (16 files)
├── models/       # mysport_model_*.py, mysport_dimensions.py
├── hints/        # mysport_hints_*.py
├── pipeline/     # mysport_phase_*.py
├── ui/           # app.js, index.html, styles.css
└── MySportPredictor.py  # entry point
```

### Priority 3 — Break Up MySportPredictor ui/app.js

Split the 359KB file into feature modules:
- `leaderboard.js` — accuracy rankings table
- `log_stream.js` — real-time log output
- `prediction_table.js` — game predictions display
- `controls.js` — sport/engine selectors and run buttons

Add `esbuild` or `vite` as a dev bundler to enable proper module imports without shipping 359KB to the browser.

### Priority 4 — Add Pydantic Validation to MySportPredictor

Engine inputs/outputs are currently untyped dicts. Follow the pattern from `mfi_phase_03_json_model.py`:

```python
from pydantic import BaseModel

class EngineConfig(BaseModel):
    model_params: dict[str, float | bool | int]

class PredictionResult(BaseModel):
    date: str
    home_team: str
    away_team: str
    prediction: str
    confidence: float
```

### Priority 5 — Add Tests to MyUrbanLens

UrbanLens has 8 extractors and 8 normalizers but only one test. Add mocked HTTP tests using Node.js built-in `node:test` (no new deps):

```js
// tests/test_foret_ouverte_extractor.js
import { test } from 'node:test';
import assert from 'node:assert';
// Mock fetch, call extractor, assert output shape
```

### Priority 6 — Cache Trained Engine State in MySportPredictor

Elo ratings and LightGBM weights are recalculated from scratch on every run. Serialize `trained_engine` after Phase 4:

```python
import json, pickle, hashlib

# After Phase 4
cache_path = f'cache/{sport}_{model_id}_{data_hash}.pkl'
with open(cache_path, 'wb') as f:
    pickle.dump(trained_engine, f)

# At Phase 4 start: load if cache exists and data unchanged
```

### Priority 7 — Extract Shared Python Utilities

MFI, SportPredictor, and ModalAnalyzer all have near-identical logger implementations. Consider a shared `mytools/` package:
- `mytools/logger.py` — thread-safe AppLogger
- `mytools/config.py` — frozen config dataclass base
- `mytools/selftest.py` — self-test runner pattern

---

## Architecture Maturity Ranking

1. **MyUrbanLens** — best directory structure, clear layering, DB migrations, env config, security helpers. Use as the layout template for new projects.
2. **MyFactoryInsight_MFI** — excellent phase separation, Pydantic validation, thorough self-tests, dual API servers, feature flags. Gap: no CI/CD, no Docker.
3. **MyModalAnalyzer** — appropriate for its scope. Single-file is valid for a focused desktop tool. Built-in QA is exemplary for scientific software.
4. **MySportPredictor** — most powerful, but flat structure and 359KB JS file are starting to limit extensibility.

---

## What to Build Next (Per Project)

| Project | Next Feature |
|---------|--------------|
| **MFI** | Implement Phase 8 (auth) + `requirements.txt` + `Dockerfile` |
| **MySportPredictor** | Reorganize into subdirs + add CI + serialize trained_engine cache |
| **MyUrbanLens** | Add extractor unit tests + `Dockerfile` + OpenAPI endpoint |
| **MyModalAnalyzer** | Load real CSV data instead of synthetic + export results to CSV |

---

## Recommended Setup for New Node.js + Python Hybrid Projects

For a new app combining a Node.js HTTP API with Python processing scripts and a JS frontend:

### IDE
**VS Code** with a `.code-workspace` file covering both the Node.js and Python roots. Extensions: `Python`, `Pylance`, `ESLint`, `Prettier`, `GitLens`. Replace `.pyproj`/`.slnx` with the workspace file for portability.

### Scaffold
```
my-new-app/
├── server/             # Node.js HTTP API (MyUrbanLens pattern)
│   ├── config/
│   ├── routes/
│   ├── services/
│   └── server.js
├── scripts/            # Python pipeline (MFI pattern)
│   ├── phase_01_ingest.py
│   ├── phase_02_process.py
│   └── shared/logger.py
├── ui/                 # Frontend JS (keep files <50KB each)
│   ├── index.html
│   └── app/
│       ├── main.js
│       ├── table.js
│       └── controls.js
├── database/
│   ├── schema.sql
│   └── migrations/
├── .github/workflows/ci.yml
├── .env.example
├── requirements.txt
├── package.json
├── CLAUDE.md           # Always include this
└── .code-workspace
```

### Claude Code Setup
- Add `CLAUDE.md` at the root (follow the pattern in each project in this repo set)
- Use the `/session-start-hook` skill to auto-run `npm install` + `pip install -r requirements.txt`
- Use the `/fewer-permission-prompts` skill to pre-approve common read-only operations
- Develop on feature branches; never push directly to `main` without CI passing

### Patterns to Carry Forward
- Phase-based pipeline structure (from MFI/SportPredictor)
- Frozen config dataclass (from ModalAnalyzer)
- Layered directory structure (from UrbanLens)
- `--self-test` CLI entry point on every Python script
- Pydantic at all data boundaries (from MFI)
- Minimal dependencies rule
