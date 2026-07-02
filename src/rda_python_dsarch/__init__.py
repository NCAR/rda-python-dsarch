"""rda_python_dsarch: dataset archive (dsarch) utility package.

This package exposes two parallel APIs:

1. Legacy module-based API (back-compat). Import the capitalized
   submodules and call their module-level functions, e.g.::

       from rda_python_dsarch import PgArch, PgMeta

2. Class-based API (preferred for new code). Import the class from the
   lower-case module and either instantiate or subclass it, e.g.::

       from rda_python_dsarch.pg_arch import PgArch
       from rda_python_dsarch.pg_meta import PgMeta

The legacy submodules are eagerly imported below so that
``from rda_python_dsarch import PgArch`` continues to return the module
object that existing callers expect.
"""

from . import PgArch, PgMeta

__version__ = "2.0.22"

__all__ = [
   "PgArch",
   "PgMeta",
   "__version__",
]
