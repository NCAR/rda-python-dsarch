# dsarch

Python project to archive and maintain datasets for the [NSF NCAR Geoscience Data Exchange (GDEX)](https://gdex.ucar.edu).

The user guide for this utility tool can be viewed at: [User guide](https://gdex-docs-dsarch.readthedocs.io).

## Setuid Setup

`dsarch` is executed as the common user `gdexdata` via the `rda_python_setuid`
setuid mechanism.  `rda_python_setuid` is declared as a dependency and is
installed automatically with this package.

### Environment setup

#### Option A — Python venv (DECS machines)

```bash
python3 -m venv $ENVHOME          # e.g. /glade/u/home/gdexdata/gdexmsenv
source $ENVHOME/bin/activate
pip install rda_python_dsarch
```

#### Option B — Conda (DAV/Casper)

```bash
conda activate pg-gdex            # e.g. /glade/work/gdexdata/conda-envs/pg-gdex
pip install rda_python_dsarch
```

### Full setuid install (requires sudo access to gdexdata)

Run these steps once per environment after `pip install`:

```bash
# Compile the pywrapper C binary (once per environment):
pywrapper-install --user gdexdata

# Wire up dsarch as a setuid entry:
pywrapper-install --link dsarch --user gdexdata
```

`pywrapper-install` with no arguments displays the full user guide.

### Simple install (no sudo required, runs as current user)

Users who do not need the setuid mechanism can create a direct symlink instead:

```bash
pywrapper-install --link dsarch --simple
```

This creates `bin/dsarch -> bin/setuid_dsarch` and the program runs as the
current user with no privilege change.
