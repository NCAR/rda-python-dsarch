/***************************************************************************************\
 *
 *    Title: dsarch.c
 *   Author: Zaihua Ji, zji@ucar.edu
 *     Date: 2024-01-30
 *  Purpose: C wrapper to setuid for a common effective user to
 *           run dsarch python scripts
 *
 * Instruction:
 *    after python -m pip install rda_python_dsarch
 *    cd $ENVHOME/lib/python3.*/site-packages/rda_python_dsarch/
 *    cp dsarch.c $ENVHOME/bin/
 *    cd $ENVHOME/bin/
 *    sudo -u CommonUser gcc -o dsarch $ENVHOME/bin/dsarch.c
 *    sudo -u CommonUser chmod 4750 dsarch
 *
 *           *: python 3 release number, for example n = 10 in Python 3.10.12
 *  CommonUser: rdadata, for RDAMS configuration
 *    $ENVHOME: /glade/u/home/rdadata/rdamsenv (venv) on DECS machines, and
 *              /glade/work/rdadata/conda-envs/pg-rda (conda) on DAV;
 *
 \***************************************************************************************/

#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>

/* main program */
int main(int argc, char *argv[]) {
   char prog[128];
   char file[] = __FILE__;

   strcpy(prog, dirname(file));
   strcat(prog, "/dsarch.py");

   /* call Python script */
   execv(prog, argv);
}
