# dsarch

Python project to archive and maintain datasets for the [NSF NCAR Geoscience Data Exchange (GDEX)](https://gdex.ucar.edu).

The user guide for this utility tool can be viewed at: [User guide](https://gdex-docs-dsarch.readthedocs.io).

## Code overview

The package is built around three Python modules in `src/rda_python_dsarch/`:

- **`dsarch.py`** — Command-line entry point.  Defines `DsArch`, the action
  class that inherits from `PgArch` and `PgMeta`.  Its two driver methods
  `read_parameters()` and `start_actions()` parse the CLI and dispatch to
  the matching handler (archive, get, set, move, delete, restore) for web,
  saved, help, and Quasar-backup files, plus dataset, group, and version
  metadata.  After the primary action, it also performs cross-cutting
  follow-up: dataset period updates, file-count resets (`-WN`/`-WM`/`-RT`),
  final logging, and optional email notification.

- **`pg_arch.py`** — Shared state and helpers.  Defines the `PgArch` mixin
  (extends `PgOPT`, `PgCMD`, `PgSplit` from `rda_python_common`).  Holds the
  master `OPTS` table that maps short action codes (e.g. `AW`, `GD`, `SG`,
  `RQ`) to their bit flags, names, and write-mode levels; the runtime path
  cache (`RTPATH`, `webpaths`, `savedpaths`); and group-type/display-order
  caches.  Provides the utility methods used across all actions: SQL
  condition building, path resolution, file-name validation, and dataset/
  group metadata caching.

- **`pg_meta.py`** — File-count bookkeeping and metadata-XML queueing.
  Defines the `PgMeta` mixin (extends `PgCMD`, `PgSplit`).  Tracks pending
  changes to per-dataset/per-group file counts in `GCOUNTS` (a 13-slot
  array covering MSS, web-D/N, and saved buckets) and flushes them to RDADB
  in batch.  Maintains `META`, a queue of metadata operations dispatched
  to the external tools `gatherxml`, `dcm`, `rcm`, `scm`, and `sml`.  Also
  provides `switch_logfile()` for redirecting per-action log/error output.

The inheritance chain is `DsArch -> PgArch -> PgMeta -> PgCMD/PgSplit`, so
`DsArch` instances expose every option, path, count, and database helper
through a single object.

## Environment setup

Create a Python environment first; package installs in the next section run
inside whichever environment you activate here.

### Option A — Python venv (DECS machines)

```bash
python3 -m venv $ENVHOME          # e.g. /glade/u/home/gdexdata/gdexmsenv
source $ENVHOME/bin/activate
```

### Option B — Conda (DAV/Casper)

```bash
conda create -n pg-gdex python=3.12
conda activate pg-gdex            # e.g. /glade/work/gdexdata/conda-envs/pg-gdex
```

## Installing rda-python-dsarch

Pick whichever install mode fits your workflow.  All three pull in the
transitive dependencies (`rda_python_common`, `rda_python_setuid`,
`rda_python_miscs`) automatically.

For local development, clone this repo alongside your project and install it
in editable mode so that changes are picked up without re-installing:

```bash
git clone https://github.com/NCAR/rda-python-dsarch.git
cd rda-python-dsarch
pip install -e .
```

To test a specific branch (e.g. an in-progress feature or fix branch), pass
`-b/--branch` to `git clone`:

```bash
git clone -b <branch-name> https://github.com/NCAR/rda-python-dsarch.git
cd rda-python-dsarch
pip install -e .
```

For a regular (non-editable) install from a checkout:

```bash
pip install /path/to/rda-python-dsarch
```

For a production install on a system that uses the published distribution:

```bash
pip install rda_python_dsarch
```

## Setuid Setup

`dsarch` is executed as the common user `gdexdata` via the `rda_python_setuid`
setuid mechanism, which is pulled in automatically as a dependency.  After
`pip install` above, choose one of the wiring options below.

### Full setuid install (requires sudo access to gdexdata)

Run these steps once per environment:

```bash
# Compile the pywrapper C binary (once per environment):
pywrapper-install -c|--compile -n|--username gdexdata

# Wire up dsarch as a setuid entry (or use 'all' to link every setuid_* at once):
pywrapper-install -l|--link dsarch
pywrapper-install -l|--link all
```

`pywrapper-install` with no arguments displays the full user guide.

### Simple install (no sudo required, runs as current user)

Users who do not need the setuid mechanism can create a direct symlink instead:

```bash
pywrapper-install -l|--link dsarch -s|--simple
pywrapper-install -l|--link all -s|--simple   # or link every setuid_* at once
```

This creates `bin/dsarch -> bin/setuid_dsarch` and the program runs as the
current user with no privilege change.

### Update an existing installation (no sudo required)

When the package is upgraded and a new `pywrapper.c` is bundled, recompile and
reinstall all setuid binaries using the existing `pgstart_*` binaries:

```bash
pywrapper-install -u|--update
```

### Setup guide

The shared setuid setup guide is shown automatically if `setuid_dsarch` is
invoked directly before the setuid wrapper has been configured.

## Documentation sync

The user guide rendered at
[gdex-docs-dsarch.readthedocs.io](https://gdex-docs-dsarch.readthedocs.io) is
generated from `src/rda_python_dsarch/dsarch.usg` in this repository.  When a
pull request that modifies `dsarch.usg` is merged here, an automated workflow
converts the updated `dsarch.usg` into the RST-format source files in the
[gdex-docs-dsarch](https://github.com/NCAR/gdex-docs-dsarch) repository and
opens a pull request there with the regenerated docs, ready for review and
merge.  No manual RST editing is required — keep all user-facing content in
`dsarch.usg` and let the sync produce the docs.
