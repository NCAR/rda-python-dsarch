#!/usr/bin/env python3
#
##################################################################################
#
#     Title: dsarch-setup
#    Author: Zaihua Ji, zji@ucar.edu
#      Date: 2026-05-12
#   Purpose: Display the setuid setup guide for dsarch after pip install.
#            Run automatically when setuid_dsarch is invoked directly instead
#            of through the dsarch -> pywrapper setuid symlink.
#
#    Github: https://github.com/NCAR/rda-python-dsarch.git
#
##################################################################################

import os
import sys


SETUP_GUIDE = """
 dsarch - Setuid Setup Guide
 ===========================

 dsarch must be run as the common user 'gdexdata' via the rda_python_setuid
 mechanism.  rda_python_setuid is installed automatically as a dependency.

 If you are seeing this message after running 'setuid_dsarch' directly, it
 means the setuid wrapper has not been set up yet.  Follow the steps below.

 Run 'pywrapper-install' with no arguments for the full pywrapper user guide.

 Environment Setup
 -----------------

   Option A - Python venv (DECS machines):
      python3 -m venv $ENVHOME    # e.g. /glade/u/home/gdexdata/gdexmsenv
      source $ENVHOME/bin/activate
      pip install rda_python_dsarch

   Option B - Conda (DAV/Casper):
      conda activate pg-gdex      # e.g. /glade/work/gdexdata/conda-envs/pg-gdex
      pip install rda_python_dsarch

 Full Setuid Install (requires sudo access to gdexdata)
 ------------------------------------------------------

   # Compile the pywrapper C binary (once per environment):
   pywrapper-install -c|--compile -u|--user gdexdata

   # Wire up dsarch as a setuid entry:
   pywrapper-install -l|--link dsarch -u|--user gdexdata

 Simple Install (no sudo required, runs as current user)
 -------------------------------------------------------

   pywrapper-install -l|--link dsarch -s|--simple

   This creates bin/dsarch -> bin/setuid_dsarch.
   The program runs as the current user with no privilege change.

"""


def main():
   print(SETUP_GUIDE)
   sys.exit(0)


if __name__ == '__main__': main()
