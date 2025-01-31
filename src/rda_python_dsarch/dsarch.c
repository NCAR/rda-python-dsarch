/***************************************************************************************\
 *
 *    Title: dsarch.c
 *   Author: Zaihua Ji, zji@ucar.edu
 *     Date: 2024-01-30
 *  Purpose: C wrapper to setuid for dsarch python scripts
 *
 * Instruction:
 *
 *    dsarch.c $LOCHOME/bin/dsarch.c
 *    cd $LOCHOME/bin/
 *    gcc -o dsarch dsarch.c
 *    chmod 4750 dsarch
 *
 *    $LOCHOME: /usr/local/decs On DECS machines, and /ncar/rda/setuid on DAV
 *    $ENVHOME: /glade/u/home/rdadata/rdamsenv on DECS machines, and
 *              /glade/work/zji/conda-envs/pg-rda on DAV
 *
 \***************************************************************************************/

#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* main program */
int main(int argc, char *argv[]) {
   char *name;
   char prog[128];
   char pext[] = ".py";

   strcpy(prog, getenv("ENVHOME"));
   strcat(prog, "/bin/");
   name = strrchr(argv[0], '/');
   if(name == (char *)NULL) {
      strcat(prog, argv[0]);
   } else {
      strcat(prog, ++name);
   }
   name = strrchr(prog, '.');
   if(name == (char *)NULL || strcmp(name, pext) != 0) {
      strcat(prog, pext);
   }

   /* call Python script */
   execv(prog, argv);
}
