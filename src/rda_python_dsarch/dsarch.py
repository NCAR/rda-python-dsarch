#!/usr/bin/env python3
#
##################################################################################
#
#     Title: dsarch
#    Author: Zaihua Ji, zji@ucar.edu
#      Date: 09/29/2020
#   Purpose: utility program to archive data files of a given dataset onto RDA
#            server; and save information of data files into RDADB
#
#    Github: https://github.com/NCAR/rda-python-dsarch.git
#
##################################################################################
#
import sys
import os
import re
from os import path as op
from rda_python_common import PgLOG
from rda_python_common import PgFile
from rda_python_common import PgUtil
from rda_python_common import PgOPT
from rda_python_common import PgSIG
from rda_python_common import PgCMD
from rda_python_common import PgGLBS
from rda_python_common import PgDBI
from rda_python_common import PgSplit
from . import PgMeta
from . import PgArch

ERRCNT = RETSTAT = ALLCNT = ADDCNT = MODCNT = OVRRIDE = 0
TARFILES = {}
DSACTIONS = {}
VINDEX = {}
CHGGRPS = {}
ERRMSG = ''

#
# main function to run dsarch
#
def main():

   pgname = "dsarch"
   PgLOG.PGLOG['PUSGDIR'] = op.dirname(op.abspath(__file__))
   PgOPT.parsing_input(pgname)
   PgLOG.set_suid(PgLOG.PGLOG['EUID'] if PgOPT.OPTS[PgOPT.PGOPT['CACT']][2] > 0 else PgLOG.PGLOG['RUID'])
   
   if 'DS' in PgOPT.params:
      dsid = PgOPT.params['DS']
   else:
      dsid = None
      if PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GH'][0] and 'HF' in PgOPT.params:
         dsid = PgArch.get_dsid(PgOPT.params['HF'], 'helpfile')
      elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GW'][0] and 'WF' in PgOPT.params:
         dsid = PgArch.get_dsid(PgOPT.params['WF'], 'wfile')
      elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GS'][0] and 'SF' in PgOPT.params:
         dsid = PgArch.get_dsid(PgOPT.params['SF'], 'sfile')
      elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GQ'][0] and 'QF' in PgOPT.params:
         dsid = PgArch.get_dsid(PgOPT.params['QF'], 'bfile')
      if dsid: PgOPT.params['DS'] = dsid

   if 'GI' in PgOPT.params and 'OG' in PgOPT.params:
      # try to gather the file names before set in case not given
      if PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['SS'][0]:
         PgOPT.params['SF'] = PgArch.get_filenames("sfile")
      elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['SW'][0]:
         PgOPT.params['WF'] = PgArch.get_filenames("wfile")

   PgArch.check_enough_options(PgOPT.PGOPT['CACT'], PgOPT.PGOPT['ACTS'])
   start_action(pgname, dsid)

   if RETSTAT:
      errmsg = "Action {} for {} finished, but unsuccessfully".format(PgOPT.PGOPT['CACT'], dsid)
      reset_errmsg(-1)
      if PgLOG.PGLOG['DSCHECK']: PgDBI.record_dscheck_error(errmsg)
      PgLOG.pglog(errmsg, PgLOG.LGEREX)
   else:
      if PgLOG.PGLOG['DSCHECK']: PgCMD.record_dscheck_status("D")
      if PgOPT.OPTS[PgOPT.PGOPT['CACT']][2]:
         if 'EM' in PgOPT.params:
            reset_errmsg(-1)
            PgLOG.set_email("Action {} for {} finished".format(PgOPT.PGOPT['CACT'], dsid), PgLOG.EMLTOP)
            PgLOG.cmdlog(None, 0, PgLOG.LOGWRN|PgLOG.SNDEML)
         else:
            PgLOG.cmdlog()

   PgLOG.pgexit(0)

#
# start action of dsarch
#
def start_action(pgname, dsid):

   global ALLCNT, OVERRIDE

   setds = 0
   OVERRIDE = PgLOG.OVRIDE if 'OE' in PgOPT.params else 0
   if 'RO' in PgOPT.params and 'ON' in PgOPT.params: del PgOPT.params['RO']
   if PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['AH'][0]:
      ALLCNT = len(PgOPT.params['HF']) if 'HF' in PgOPT.params else len(PgOPT.params['LF'])
      if 'XC' in PgOPT.params:
         crosscopy_help_files('Copy')
      elif 'XM' in PgOPT.params:
         crosscopy_help_files('Move')
      else:
         archive_help_files()
      if 'CL' in PgOPT.params: PgFile.delete_local_files()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['AQ'][0]:
      if 'XC' in PgOPT.params:
         ALLCNT = len(PgOPT.params['QF'])
         crosscopy_backup_files()
      else:
         archive_backup_file()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['AS'][0]:
      ALLCNT = len(PgOPT.params['SF']) if 'SF' in PgOPT.params else len(PgOPT.params['LF'])
      PgArch.cache_group_info(ALLCNT)
      if 'XC' in PgOPT.params:
         crosscopy_saved_files('Copy')
      elif 'XM' in PgOPT.params:
         crosscopy_saved_files('Move')
      else:
         archive_saved_files()
         if 'CL' in PgOPT.params: PgFile.delete_local_files()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['AW'][0]:
      ALLCNT = len(PgOPT.params['WF']) if 'WF' in PgOPT.params else len(PgOPT.params['LF'])
      if 'ML' in PgOPT.params and 'GX' in PgOPT.params: del PgOPT.params['ML']
      PgArch.cache_group_info(ALLCNT)
      if 'XC' in PgOPT.params:
         crosscopy_web_files('Copy')
      elif 'XM' in PgOPT.params:
         crosscopy_web_files('Move')
      else:
         archive_web_files()
      if PgSIG.PGSIG['BPROC'] > 1: set_webfile_info()
      if 'CL' in PgOPT.params: PgFile.delete_local_files()
   elif PgOPT.PGOPT['ACTS'] & PgOPT.OPTS['CG'][0]:
      ALLCNT = len(PgOPT.params['GI'])
      change_group_index()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['DG'][0]:
      ALLCNT = len(PgOPT.params['GI'])
      delete_group_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['DL'][0]:
      if 'DD' not in PgOPT.params: PgOPT.params['DD'] = -1
      ALLCNT = (len(PgOPT.params['HF']) if 'HF' in PgOPT.params else
                (len(PgOPT.params['SF']) if 'SF' in PgOPT.params else
                 (len(PgOPT.params['WF']) if 'WF' in PgOPT.params else len(PgOPT.params['QF']))))
      if 'DD' in PgOPT.params: PgFile.record_delete_directory(None, PgOPT.params['DD'])
      if 'HF' in PgOPT.params:
         delete_help_files()
      elif 'SF' in PgOPT.params:
         delete_saved_files()
      elif 'WF' in PgOPT.params:
         delete_web_files()
      else:
         delete_backup_files()
      if 'DD' in PgOPT.params: PgFile.clean_delete_directory(PgOPT.PGOPT['wrnlog'])
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GA'][0]:
      if 'ON' in PgOPT.params: del PgOPT.params['ON'] # use default order string
      if 'RG' in PgOPT.params: del PgOPT.params['RG'] # not recursively for sub groups
      if 'FN' not in PgOPT.params: PgOPT.params['FN'] = 'ALL'
      # get dataset info first
      if not get_dataset_info(): return
      # get group info second
      PgArch.cache_group_info(0)
      get_group_info()
      # get help file info
      get_helpfile_info()
      # get saved file info
      get_savedfile_info()
      # get web file info
      get_webfile_info()
      # get backup file info
      get_backfile_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GD'][0]:
      get_dataset_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GG'][0]:
      get_group_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GH'][0]:
      get_helpfile_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GQ'][0]:
      get_backfile_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GS'][0]:
      PgArch.cache_group_info(0)
      get_savedfile_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GW'][0]:
      PgArch.cache_group_info(0)
      get_webfile_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GV'][0]:
      get_version_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['MV'][0]:
      if 'DD' not in PgOPT.params: PgOPT.params['DD'] = -1
      ALLCNT = (len(PgOPT.params['HF']) if 'HF' in PgOPT.params else
                (len(PgOPT.params['SF']) if 'SF' in PgOPT.params else
                 (len(PgOPT.params['WF']) if 'WF' in PgOPT.params else
                  (len(PgOPT.params['QF']) if 'QF' in PgOPT.params else 0))))
      if 'DD' in PgOPT.params: PgFile.record_delete_directory(None, PgOPT.params['DD'])
      if 'HF' in PgOPT.params:
         move_help_files()
      elif 'TS' in PgOPT.params:
         web_to_saved_files()
      elif 'TW' in PgOPT.params:
         saved_to_web_files()
      elif 'SF' in PgOPT.params:
         move_saved_files()
      elif 'WF' in PgOPT.params:
         move_web_files()
      else:
         move_backup_files()
      if 'DD' in PgOPT.params: PgFile.clean_delete_directory(PgOPT.PGOPT['wrnlog'])
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['RQ'][0]:
      if 'WF' in PgOPT.params or 'SF' in PgOPT.params:
         PgArch.build_backup_filelist()
      ALLCNT = len(PgOPT.params['QF'])
      retrieve_backup_files()
      if 'WF' in PgOPT.params:
         ALLCNT = len(PgOPT.params['WF'])
         restore_backup_webfiles()
      if 'SF' in PgOPT.params:
         ALLCNT = len(PgOPT.params['SF'])
         restore_backup_savedfiles()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['SA'][0]:
      if 'IF' not in PgOPT.params:
         PgOPT.action_error("Missing input file via Option -IF")
      if PgOPT.get_input_info(PgOPT.params['IF'], "DATASET"):
         PgArch.check_enough_options('SD', PgOPT.OPTS['SD'][0])
         set_dataset_info()
      if PgOPT.get_input_info(PgOPT.params['IF'], "DSGROUP") and 'GI' in PgOPT.params:
         PgArch.check_enough_options('SG', PgOPT.OPTS['SG'][0])
         ALLCNT = len(PgOPT.params['GI'])
         set_group_info()
      PgOPT.params['RO'] = 1
      if PgOPT.get_input_info(PgOPT.params['IF'], "HELPFILE") and 'HF' in PgOPT.params:
         PgArch.check_enough_options('SH', PgOPT.OPTS['SH'][0])
         ALLCNT = len(PgOPT.params['HF'])
         PgArch.cache_group_info(ALLCNT)
         PgArch.get_next_disp_order()   # in case not empty
         set_helpfile_info()
      if PgOPT.get_input_info(PgOPT.params['IF'], "SAVEDFILE") and 'SF' in PgOPT.params:
         PgArch.check_enough_options('SS', PgOPT.OPTS['SS'][0])
         ALLCNT = len(PgOPT.params['SF'])
         PgArch.cache_group_info(ALLCNT)
         PgArch.get_next_disp_order()   # in case not empty
         set_savedfile_info()
      if PgOPT.get_input_info(PgOPT.params['IF'], "WEBFILE") and 'WF' in PgOPT.params:
         PgArch.check_enough_options('SW', PgOPT.OPTS['SW'][0])
         ALLCNT = len(PgOPT.params['WF'])
         PgArch.cache_group_info(ALLCNT)
         PgArch.get_next_disp_order()   # in case not empty
         set_webfile_info()
      if PgOPT.get_input_info(PgOPT.params['IF'], "BACKFILE") and 'QF' in PgOPT.params:
         PgArch.check_enough_options('SQ', PgOPT.OPTS['SQ'][0])
         ALLCNT = len(PgOPT.params['QF'])
         PgArch.get_next_disp_order()   # in case not empty
         set_backfile_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['SD'][0]:
      setds = 2
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['SG'][0]:
      ALLCNT = len(PgOPT.params['GI'])
      if 'WN' in PgOPT.params or 'WM' in PgOPT.params:
         for gindex in PgOPT.params['GI']:
            CHGGRPS[gindex] = 1
      else:
         set_group_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['SH'][0]:
      if 'HF' in PgOPT.params:
         ALLCNT = len(PgOPT.params['HF'])
         set_helpfile_info()
      elif 'ON' in PgOPT.params:
         PgArch.reorder_filelist('hfile')
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['SQ'][0]:
      if 'QF' in PgOPT.params:
         ALLCNT = len(PgOPT.params['QF'])
         set_backfile_info()
      elif 'ON' in PgOPT.params:
         PgArch.reorder_filelist('bfile')
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['SS'][0]:
      if 'SF' in PgOPT.params:
         ALLCNT = len(PgOPT.params['SF'])
         PgArch.cache_group_info(ALLCNT)
         set_savedfile_info()
      elif 'ON' in PgOPT.params:
         PgArch.reorder_filelist('sfile')
      if 'RD' in PgOPT.params: PgArch.clean_dataset_directory(1)
      if 'WM' in PgOPT.params: PgOPT.params['WM'] = 8
      if 'WN' in PgOPT.params: PgOPT.params['WN'] = 8
      if 'RT' in PgOPT.params: PgOPT.params['RT'] = 8
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['SW'][0]:
      if 'WF' in PgOPT.params:
         ALLCNT = len(PgOPT.params['WF'])
         PgArch.cache_group_info(ALLCNT)
         set_webfile_info()
      elif 'ON' in PgOPT.params:
         PgArch.reorder_filelist('wfile')
      if 'RD' in PgOPT.params: PgArch.clean_dataset_directory(0)
      if 'WM' in PgOPT.params: PgOPT.params['WM'] = 4
      if 'WN' in PgOPT.params: PgOPT.params['WN'] = 4
      if 'RT' in PgOPT.params: PgOPT.params['RT'] = 4
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['SV'][0]:
      ALLCNT = len(PgOPT.params['VI'])
      set_version_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['TV'][0]:
      ALLCNT = len(PgOPT.params['VI'])
      terminate_version_info()
   elif PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['UC'][0]:
      PgDBI.reset_rdadb_version(dsid)
   if not (setds or PgOPT.PGOPT['CACT'] == 'SA'):
      if('BD' in PgOPT.params or 'ED' in PgOPT.params or
         'PS' in PgOPT.params or 'BT' in PgOPT.params or 'ET' in PgOPT.params):
         setds = 1
   if setds:
      # dataset info needs to be updated
      set_dataset_info("P" if setds == 1 else None)
   if PgOPT.OPTS[PgOPT.PGOPT['CACT']][2] > 0:
      if 'WN' in PgOPT.params:
         # reset dataset/group file counts
         PgLOG.pglog("Reset file counts for of {} ...".format(dsid), PgLOG.WARNLG)
         cnt = reset_group_filenumber(dsid, PgOPT.params['WN'])
         s = 's' if cnt > 1 else ''
         if cnt > 0: PgDBI.reset_rdadb_version(dsid)
         PgLOG.pglog("{} Dataset/Group Record{} set for file counts".format(cnt, s), PgLOG.WARNLG)
      if 'WM' in PgOPT.params:
         # reset dataset/group metadata
         PgLOG.pglog("Reset Dataset/Group metadata for {} ...".format(dsid), PgLOG.WARNLG)
         reset_group_metadata(dsid, PgOPT.params['WM'])
      if 'RT' in PgOPT.params:
         # reset top group index
         PgLOG.pglog("Reset top group indices for of dsid ...", PgLOG.WARNLG)
         cnt = reset_top_group_index(dsid, PgOPT.params['RT'])
         s = 's' if cnt > 1 else ''
         if cnt > 0: PgDBI.reset_rdadb_version(dsid)
         PgLOG.pglog("{} file Record{} set for top group index".format(cnt), PgLOG.WARNLG)

#
# archive web/object files
#
def archive_web_files():

   global ADDCNT, MODCNT, RETSTAT
   tname = 'wfile'
   dftloc = None
   dsid = PgOPT.params['DS']
   bucket = "rda-data"    # default object store bucket
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0
   dslocflags = set()
   dflags = {}

   PgLOG.pglog("Archive {} Web file{} of {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   if 'ZD' in PgOPT.params or 'UZ' in PgOPT.params:
      PgArch.compress_localfile_list(PgOPT.PGOPT['CACT'], ALLCNT)
   if 'QF' in PgOPT.params: PgOPT.params['QF'] = PgArch.get_bid_numbers(PgOPT.params['QF'])

   PgOPT.validate_multiple_values(tname, ALLCNT)
   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} file{} processed for Web archive".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   if 'WT' not in PgOPT.params:
      PgOPT.params['WT'] = ['D']*ALLCNT
      PgOPT.OPTS['WT'][2] |= 2
   if 'WF' not in PgOPT.params:
      PgOPT.params['WF'] = [None]*ALLCNT
      PgOPT.OPTS['WF'][2] |= 2
   if 'AF' not in PgOPT.params:
      PgOPT.params['AF'] = [None]*ALLCNT
      PgOPT.OPTS['AF'][2] |= 2
   if 'LC' not in PgOPT.params:
      PgOPT.params['LC'] = [None]*ALLCNT
      PgOPT.OPTS['LC'][2] |= 2
   if 'SZ' not in PgOPT.params:
      PgOPT.params['SZ'] = [0]*ALLCNT
      PgOPT.OPTS['SZ'][2] |= 2
   if 'MC' not in PgOPT.params: PgOPT.params['MC'] = [None]*ALLCNT
   reorder = errcnt = metatotal = metacnt = ADDCNT = MODCNT = bgcnt = acnt = ocnt = 0
   perrcnt = ALLCNT
   efiles = [1]*ALLCNT
   if PgSIG.PGSIG['BPROC'] > 1:
      afiles = [None]*ALLCNT
      lfiles = [None]*ALLCNT
      ofiles = [None]*ALLCNT
   fnames = None
   override = OVERRIDE
   if not override and 'GF' in PgOPT.params: override = PgLOG.OVRIDE
   while True:
      for i in range(bidx, ALLCNT):
         if PgSIG.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
            if PgLOG.PGLOG['DSCHECK']:
               PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
            if metacnt >= PgOPT.PGOPT['RSMAX']:
               metatotal += PgMeta.process_metadata("W", metacnt, PgOPT.PGOPT['emerol'])
               metacnt = 0
            if 'EM' in PgOPT.params:
               PgLOG.PGLOG['PRGMSG'] = "{}/{} of {} web file{} archived/processed".format(acnt, i, ALLCNT, s)
         lfile = locfile = PgOPT.params['LF'][i]
         if not (efiles[i] and locfile): continue
         efiles[i] = 0
         if not PgOPT.params['AF'][i]:
            PgOPT.params['AF'][i] = PgFile.local_archive_format(lfile)
         lsize = PgFile.local_file_size(lfile, 6, PgOPT.PGOPT['emerol'])
         if lsize <= 0:
            if lsize == -2:
               errcnt += 1
               efiles[i] = 1
            else:
               PgOPT.params['LF'][i] = PgOPT.params['WF'][i] = None
            continue
         locflag = PgOPT.params['LC'][i]
         if not locflag or locflag == 'R':
            if not dftloc: dftloc = PgArch.get_dataset_locflag(dsid)
            locflag = PgOPT.params['LC'][i] = dftloc
         if locflag == 'C': PgLOG.pglog(lfile + ": Cannot Archive Web File for CGD data", PgOPT.PGOPT['extlog'])
         if locflag == 'B':
            oarch = warch = 1
         else:
            dslocflags.add(locflag)
            if locflag == 'O':
               oarch = 1
               warch = 0
            else:
               oarch = 0
               warch = 1
         type = PgOPT.params['WT'][i]
         if not PgOPT.params['MC'][i]: PgOPT.params['MC'][i] = PgFile.get_md5sum(locfile)  # re-get MD5 Checksum
         if not (PgOPT.params['SZ'][i] and PgOPT.params['SZ'][i] == lsize):
            PgOPT.params['SZ'][i] = lsize
         if not PgOPT.params['WF'][i]: PgOPT.params['WF'][i] = get_archive_filename(lfile)
         afile = (PgArch.get_web_path(i, PgOPT.params['WF'][i], 1, type) if warch else None)
         wfile = PgArch.get_web_path(i, PgOPT.params['WF'][i], 0, type)
         ofile = PgArch.get_object_path(wfile, dsid) if oarch else None
         pgrec = PgSplit.pgget_wfile(dsid, "*", "wfile = '{}'".format(wfile), PgOPT.PGOPT['extlog'])
         if pgrec and PgOPT.params['WF'][i] != wfile: PgOPT.params['WF'][i] = wfile
         if not re.search(r'^/', locfile): locfile = PgLOG.join_paths(PgLOG.PGLOG['CURDIR'], locfile)
         winfo = "{}-{}-{}".format(dsid, type, wfile)

         PgLOG.pglog("{}: Archive Web file from {} ...".format(wfile, locfile),  PgLOG.WARNLG)
         if warch and locfile == afile: warch = 0

         vsnctl = 1 if pgrec and pgrec['vindex'] and pgrec['data_size'] else 0
         chksum = PgOPT.params['MC'][i]
         if warch and (vsnctl or not override):
            info = PgFile.check_local_file(afile, 1, PgOPT.PGOPT['emerol'])
            if info:
               if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                  PgLOG.pglog("Web-{}: Same-Checksum ARCHIVED at {}:{}".format(winfo, info['date_modified'], info['time_modified']), PgOPT.PGOPT['emllog'])
                  warch = 0
               elif info['data_size'] == lsize:
                  PgLOG.pglog("Web-{}: Same-Size ARCHIVED at {}:{}".format(winfo, info['date_modified'], info['time_modified']), PgOPT.PGOPT['wrnlog'])
                  warch = 0
               elif vsnctl and not override:
                  PgLOG.pglog("Web-{}: Cannot rearchive version controlled file".format(winfo), PgOPT.PGOPT['extlog'])
                  PgOPT.params['WF'][i] = None
                  continue
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['G'] = PgLOG.PGLOG['DSDHOME']
               continue

         if oarch:
            replace = 0
            info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
            if info:
               if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                  PgLOG.pglog("Object-{}-{}: Same-Checksum ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), PgOPT.PGOPT['emllog'])
                  oarch = 0
               elif info['data_size'] == lsize:
                  PgLOG.pglog("Object-{}-{}: Same-Size ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), PgOPT.PGOPT['wrnlog'])
                  oarch = 0
               elif vsnctl and not override:
                  PgLOG.pglog("Object-{}-{}: Cannot rearchive version controlled file".format(bucket, ofile), PgOPT.PGOPT['extlog'])
                  PgOPT.params['WF'][i] = None
                  continue
               else:
                  replace = 1
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['O'] = bucket
               continue

         if (warch + oarch) > 0:
            if warch:
               if not PgFile.local_copy_local(afile, lfile, PgOPT.PGOPT['emerol']|override):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['G'] = PgLOG.PGLOG['DSDHOME']
                  continue
               acnt += 1
            if oarch:
               if replace: PgFile.delete_object_file(ofile, bucket)
               if not PgFile.local_copy_object(ofile, lfile, bucket, None, PgOPT.PGOPT['emerol']|override):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['O'] = bucket
                  continue
               ocnt += 1

            if PgLOG.PGLOG['DSCHECK']: chksize += lsize
            if PgSIG.PGSIG['BPROC'] > 1:
               afiles[i] = afile
               lfiles[i] = lfile
               ofiles[i] = ofile
               bgcnt += 1

         if PgSIG.PGSIG['BPROC'] < 2:
            if not fnames: fnames = PgOPT.get_field_keys(tname, None, "G")  # get setting fields if not yet
            info = None
            if warch:
               info = PgFile.check_local_file(afile, 1, PgOPT.PGOPT['emerol'])
            elif oarch:
               info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
            elif pgrec:
               info = get_file_origin_info(wfile, pgrec)
            elif locflag == 'O':
               info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
            wid = set_one_webfile(i, pgrec, wfile, fnames, type, info)
            if not wid:
               PgOPT.params['LF'][i] = PgOPT.params['WF'][i] = None
               continue
            if 'GX' in PgOPT.params and PgOPT.PGOPT['GXTYP'].find(type) > -1:
               metacnt += PgMeta.record_meta_gather('W', dsid, wfile, PgOPT.params['DF'][i])
               PgMeta.cache_meta_tindex(dsid, wid, 'W')
            if pgrec and pgrec['meta_link'] and pgrec['meta_link'] != 'N':
               if 'DX' in PgOPT.params or PgOPT.PGOPT['GXTYP'].find(type) < 0 and PgOPT.PGOPT['GXTYP'].find(pgrec['type']) > -1:
                  metacnt += PgMeta.record_meta_delete('W', dsid, wfile)
               elif 'GI' in PgOPT.params:
                  gindex = PgOPT.params['GI'][i]
                  if gindex != pgrec['gindex'] and (gindex or (PgOPT.OPTS['GI'][2]&2) == 0):
                     metacnt += PgMeta.record_meta_summary('W', dsid, gindex, pgrec['gindex'])

      if errcnt == 0 or errcnt >= perrcnt or PgLOG.PGLOG['DSCHECK']: break
      perrcnt = errcnt
      PgLOG.pglog("Rearchive failed {}/{} Web file{} for {}".format(errcnt, ALLCNT. s, dsid), PgOPT.PGOPT['emllog'])
      errcnt = reset_errmsg(0)

   if errcnt:
      if 'CL' in PgOPT.params: del PgOPT.params['CL']
      RETSTAT = reset_errmsg(errcnt)
      for i in range(bidx, ALLCNT):
         if efiles[i]: PgOPT.params['WF'][i] = ''
      if PgLOG.PGLOG['DSCHECK']:
         PgFile.check_storage_dflags(dflags, PgLOG.PGLOG['DSCHECK'], PgOPT.PGOPT['emerol'])
   if bgcnt:
      PgSIG.check_background(None, 0, PgLOG.LOGWRN, 1)
      for i in range(ALLCNT):
         if afiles[i]:
            validate_gladearch(afiles[i], lfiles[i], i)
         if ofiles[i]:
            validate_objectarch(ofiles[i], lfiles[i], bucket, i)
   if acnt > 0:
      PgLOG.pglog("{} of {} Web file{} archived for {}".format(acnt, ALLCNT, s, PgOPT.params['DS']), PgOPT.PGOPT['emllog'])
   if ocnt > 0:
      PgLOG.pglog("{} of {} Object file{} archived for {}".format(ocnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if metacnt > 0:
      metatotal += PgMeta.process_metadata('W', metacnt, PgOPT.PGOPT['emerol'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (metatotal + ADDCNT + MODCNT + reorder) > 0:
      PgLOG.pglog("{}/{} of {} Web file record{} added/modified for {}!".format(ADDCNT, MODCNT, ALLCNT , s, dsid), PgOPT.PGOPT['emllog'])
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""
   if dslocflags: PgArch.set_dataset_locflag(dsid, dslocflags.pop())

#
# archive help files
#
def archive_help_files():

   global ADDCNT, MODCNT, RETSTAT
   tname = 'hfile'
   dsid = PgOPT.params['DS']
   bucket = 'rda-data'
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0
   dflags = {}
   PgLOG.pglog("Archive {} Help file{} of {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   PgOPT.validate_multiple_values(tname, ALLCNT)
   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} file{} processed for Help archive".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']
   if 'HT' not in PgOPT.params:
      PgOPT.params['HT'] = ['D']*ALLCNT
      PgOPT.OPTS['HT'][2] |= 2
   if 'HF' not in PgOPT.params:
      PgOPT.params['HF'] = [None]*ALLCNT
      PgOPT.OPTS['HF'][2] |= 2
   if 'AF' not in PgOPT.params:
      PgOPT.params['AF'] = [None]*ALLCNT
      PgOPT.OPTS['AF'][2] |= 2
   if 'LC' not in PgOPT.params:
      PgOPT.params['LC'] = [None]*ALLCNT
      PgOPT.OPTS['LC'][2] |= 2
   if 'SZ' not in PgOPT.params:
      PgOPT.params['SZ'] = [0]*ALLCNT
      PgOPT.OPTS['SZ'][2] |= 2
   if 'MC' not in PgOPT.params: PgOPT.params['MC'] = [None]*ALLCNT
   reorder = errcnt = ADDCNT = MODCNT = bgcnt = acnt = ocnt = 0
   perrcnt = ALLCNT
   efiles = [1]*ALLCNT
   if PgSIG.PGSIG['BPROC'] > 1:
      afiles = [None]*ALLCNT
      lfiles = [None]*ALLCNT
      ofiles = [None]*ALLCNT
   override = OVERRIDE
   fnames = None
   while True:
      for i in range(bidx, ALLCNT):
         if PgSIG.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
            if PgLOG.PGLOG['DSCHECK']:
               PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
            if 'EM' in PgOPT.params:
               PgLOG.PGLOG['PRGMSG'] = "{}/{} of {} help file{} archived/processed".format(acnt, i, ALLCNT, s)
         lfile = locfile = PgOPT.params['LF'][i]
         if not (efiles[i] and locfile): continue
         efiles[i] = 0
         if not PgOPT.params['AF'][i]:
            PgOPT.params['AF'][i] = PgFile.local_archive_format(lfile)
         locflag = (PgOPT.params['LC'][i] if PgOPT.params['LC'][i] else '')
         if not locflag: locflag = PgOPT.params['LC'][i] = 'B'
         url = PgOPT.params['WU'][i] if 'WU' in PgOPT.params and PgOPT.params['WU'][i] else None
         if locflag == 'R' or url:
            PgLOG.pglog("{}: Set Help file on {} via Action -SH".format(lfile, (url if url else 'URL')), PgOPT.PGOPT['errlog'])
            PgOPT.params['LF'][i] = PgOPT.params['HF'][i] = None
            continue
         lsize = PgFile.local_file_size(locfile, 6, PgOPT.PGOPT['emerol'])
         if lsize <= 0:
            if lsize == -2:
               errcnt += 1
               efiles[i] = 1
            else:
               PgOPT.params['LF'][i] = PgOPT.params['HF'][i] = None
            continue
         oarch = harch = 1
         if locflag == 'O':
            harch = 0
         elif locflag == 'G':
            oarch = 0
         type = PgOPT.params['HT'][i]
         stype = PgOPT.HTYPE[type] if type in PgOPT.HTYPE else 'Help'
         hpath = PgOPT.HPATH[type] if type in PgOPT.HPATH else 'help'
         if not PgOPT.params['MC'][i]: PgOPT.params['MC'][i] = PgFile.get_md5sum(locfile)  # re-get MD5 Checksum
         if not (PgOPT.params['SZ'][i] and PgOPT.params['SZ'][i] == lsize):
            PgOPT.params['SZ'][i] = lsize
         if not PgOPT.params['HF'][i]: PgOPT.params['HF'][i] = get_archive_filename(lfile)
         hfile = PgOPT.params['HF'][i]
         afile = PgArch.get_help_path(i, hfile, 1, type)
         ofile = PgArch.get_object_path(hfile, dsid, hpath) if oarch else None
         typstr = "type = '{}'".format(type)
         pgrec = PgDBI.pgget(tname, "*", "hfile = '{}' AND {} AND {}".format(hfile, typstr, dcnd), PgOPT.PGOPT['extlog'])
         if pgrec and pgrec['locflag'] == 'R':
            url = pgrec['url']
            PgLOG.pglog("{}: Reset existing Help file on {} via Action -SH".format(lfile, (url if url else 'URL')), PgOPT.PGOPT['errlog'])
            PgOPT.params['LF'][i] = PgOPT.params['HF'][i] = None
            continue
         if not re.search(r'^/', locfile): locfile = PgLOG.join_paths(PgLOG.PGLOG['CURDIR'], locfile)
         hinfo = "{}-{}-{}".format(dsid, stype, hfile)

         PgLOG.pglog("{}: Archive Help file from {} ...".format(hfile, locfile),  PgLOG.WARNLG)
         if harch and locfile == afile: harch = 0

         chksum = PgOPT.params['MC'][i]
         if harch and not OVERRIDE:
            info = PgFile.check_local_file(afile, 1, PgOPT.PGOPT['emerol'])
            if info:
               if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                  PgLOG.pglog("Help-{}: Same-Checksum ARCHIVED at {}:{}".format(hinfo, info['date_modified'], info['time_modified']), PgOPT.PGOPT['emllog'])
                  harch = 0
               elif info['data_size'] == lsize:
                  PgLOG.pglog("Help-{}: Same-Size ARCHIVED at {}:{}".format(hinfo, info['date_modified'], info['time_modified']), PgOPT.PGOPT['wrnlog'])
                  harch = 0
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               continue
 
         if oarch:
            replace = 0
            info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
            if info:
               if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                  PgLOG.pglog("Object-{}-{}: Same-Checksum ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), PgOPT.PGOPT['emllog'])
                  oarch = 0
               elif info['data_size'] == lsize:
                  PgLOG.pglog("Object-{}-{}: Same-Size ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), PgOPT.PGOPT['wrnlog'])
                  oarch = 0
               else:
                  replace = 1
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['O'] = bucket
               continue

         if (harch + oarch) > 0:
            if harch:
               if not PgFile.local_copy_local(afile, lfile, PgOPT.PGOPT['emerol']|override):
                  errcnt += 1
                  efiles[i] = 1
                  continue
               acnt += 1
            if oarch:
               if replace: PgFile.delete_object_file(ofile, bucket)
               if not PgFile.local_copy_object(ofile, lfile, bucket, None, PgOPT.PGOPT['emerol']|override):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['O'] = bucket
                  continue
               ocnt += 1

            if PgLOG.PGLOG['DSCHECK']: chksize += lsize
            if PgSIG.PGSIG['BPROC'] > 1:
               afiles[i] = afile
               lfiles[i] = lfile
               ofiles[i] = ofile
               bgcnt += 1

         if PgSIG.PGSIG['BPROC'] < 2:
            if not fnames: fnames = PgOPT.get_field_keys(tname, None, "G")  # get setting fields if not yet
            info = None
            if harch:
               info = PgFile.check_local_file(afile, 1, PgOPT.PGOPT['emerol'])
            elif oarch:
               info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
            elif pgrec:
               info = get_file_origin_info(hfile, pgrec)
            elif locflag == 'O':
               info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
            hid = set_one_helpfile(i, pgrec, hfile, fnames, type, info)
            if not hid:
               PgOPT.params['LF'][i] = PgOPT.params['WF'][i] = None
               continue

      if errcnt == 0 or errcnt >= perrcnt or PgLOG.PGLOG['DSCHECK']: break
      perrcnt = errcnt
      PgLOG.pglog("Rearchive failed {}/{} Help file{} for {}".format(errcnt, ALLCNT. s, dsid), PgOPT.PGOPT['emllog'])
      errcnt = reset_errmsg(0)

   if errcnt:
      if 'CL' in PgOPT.params: del PgOPT.params['CL']
      RETSTAT = reset_errmsg(errcnt)
      for i in range(bidx, ALLCNT):
         if efiles[i]: PgOPT.params['HF'][i] = ''
      if PgLOG.PGLOG['DSCHECK']:
         PgFile.check_storage_dflags(dflags, PgLOG.PGLOG['DSCHECK'], PgOPT.PGOPT['emerol'])
   if bgcnt:
      PgSIG.check_background(None, 0, PgLOG.LOGWRN, 1)
      for i in range(ALLCNT):
         if afiles[i]:
            validate_gladearch(afiles[i], lfiles[i], i)
         if ofiles[i]:
            validate_objectarch(ofiles[i], lfiles[i], bucket, i)
   if acnt > 0:
      PgLOG.pglog("{} of {} Help file{} archived for {}".format(acnt, ALLCNT, s, PgOPT.params['DS']), PgOPT.PGOPT['emllog'])
   if ocnt > 0:
      PgLOG.pglog("{} of {} Object file{} archived for {}".format(ocnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (ADDCNT + MODCNT + reorder) > 0:
      PgLOG.pglog("{}/{} of {} Help file record{} added/modified for {}!".format(ADDCNT, MODCNT, ALLCNT , s, dsid), PgOPT.PGOPT['emllog'])
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# archive save files
#
def archive_saved_files():

   global ADDCNT, MODCNT, RETSTAT
   tname = 'sfile'
   dftloc = None
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0
   dflags = {}
   bucket = "rda-decsdata"    # object store bucket 
   PgLOG.pglog("Archive {} Saved file{} of {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   if 'ZD' in PgOPT.params or 'UZ' in PgOPT.params:
      PgArch.compress_localfile_list(PgOPT.PGOPT['CACT'], ALLCNT)
   if 'QF' in PgOPT.params: PgOPT.params['QF'] = PgArch.get_bid_numbers(PgOPT.params['QF'])

   PgOPT.validate_multiple_values(tname, ALLCNT)
   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} file{} processed for SAVED archive".format(bidx, ALLCNT , s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   if 'SF' not in PgOPT.params:
      PgOPT.params['SF'] = [None]*ALLCNT
      PgOPT.OPTS['SF'][2] |= 2
   if 'AF' not in PgOPT.params:
      PgOPT.params['AF'] = [None]*ALLCNT
      PgOPT.OPTS['AF'][2] |= 2
   if 'LC' not in PgOPT.params:
      PgOPT.params['LC'] = [None]*ALLCNT
      PgOPT.OPTS['LC'][2] |= 2
   if 'SZ' not in PgOPT.params:
      PgOPT.params['SZ'] = [0]*ALLCNT
      PgOPT.OPTS['SZ'][2] |= 2
   if 'MC' not in PgOPT.params: PgOPT.params['MC'] = [None]*ALLCNT
   reorder = errcnt = ADDCNT = MODCNT = bgcnt = acnt = ocnt = 0
   perrcnt = ALLCNT
   efiles = [1]*ALLCNT
   if PgSIG.PGSIG['BPROC'] > 1:
      afiles = [None]*ALLCNT
      ofiles = [None]*ALLCNT
      lfiles = [None]*ALLCNT
   fnames = None
   override = OVERRIDE
   if not override and 'GF' in PgOPT.params: override = PgLOG.OVRIDE

   while True:
      for i in range(bidx, ALLCNT):
         if PgSIG.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
            if PgLOG.PGLOG['DSCHECK']:
               PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
            if 'EM' in PgOPT.params:
               PgLOG.PGLOG['PRGMSG'] = "{}/{} of {} saved file{} archived/processed".format(acnt, i, ALLCNT, s)
         lfile = locfile = PgOPT.params['LF'][i]
         if not (efiles[i] and locfile): continue
         efiles[i] = 0
         if not PgOPT.params['AF'][i]:
            PgOPT.params['AF'][i] = PgFile.local_archive_format(lfile)
         lsize = PgFile.local_file_size(lfile, 6, PgOPT.PGOPT['emerol'])
         if lsize <= 0:
            if lsize == -2:
               errcnt += 1
               efiles[i] = 1
            else:
               PgOPT.params['LF'][i] = PgOPT.params['SF'][i] = None
            continue
         locflag = PgOPT.params['LC'][i]
         if not locflag or locflag == 'R':
            if not dftloc: dftloc = PgArch.get_dataset_locflag(dsid)
            locflag = PgOPT.params['LC'][i] = dftloc
         if locflag == 'C': PgLOG.pglog(lfile + ": Cannot Archive Saved File for CGD data", PgOPT.PGOPT['extlog'])
         if locflag == 'B':
            oarch = sarch = 1
         elif locflag == 'O':
            oarch = 1
            sarch = 0
         else:
            oarch = 0
            sarch = 1
         if not PgOPT.params['MC'][i]: PgOPT.params['MC'][i] = PgFile.get_md5sum(lfile)  # re-get MD5 Checksum
         if not (PgOPT.params['SZ'][i] and PgOPT.params['SZ'][i] == lsize):
            PgOPT.params['SZ'][i] = lsize
         if PgOPT.params['SF'][i]:
            sfile = PgOPT.params['SF'][i]
         else:
            sfile = PgOPT.params['SF'][i] = get_archive_filename(lfile)
         if 'ST' in PgOPT.params and PgOPT.params['ST'][i]:
            type = PgOPT.params['ST'][i]
            if PgOPT.PGOPT['SDTYP'].find(type) < 0:
               PgLOG.pglog("{}-{}: Invalid Saved file Type '{}' to Archive".format(dsid, sfile, s), PgOPT.PGOPT['extlog'])
               continue
         else:
            PgLOG.pglog("{}-{}: Miss Saved file Type to Archive".format(dsid, sfile), PgOPT.PGOPT['extlog'])
            continue
         afile = (PgArch.get_saved_path(i, sfile, 1, type) if sarch else None)
         sfile = PgArch.get_saved_path(i, sfile, 0, type)
         ofile = (PgLOG.join_paths(dsid, sfile) if oarch else None)
         pgrec = PgDBI.pgget(tname, "*", "{} AND sfile = '{}' AND type = '{}'".format(dcnd, sfile, type), PgOPT.PGOPT['extlog'])
         if not pgrec:
            pgrec = PgDBI.pgget(tname, "type", "{} AND sfile = '{}'".format(dcnd, sfile), PgOPT.PGOPT['extlog'])
            if pgrec:
               PgLOG.pglog("{}-{}: Fail to archive, Move Action -MV to change file Type from '{}' to '{}'".format(dsid, sfile, pgrec['type'], type), PgOPT.PGOPT['emlerr'])
               continue
         if pgrec and PgOPT.params['SF'][i] != sfile: PgOPT.params['SF'][i] = sfile
         sinfo = "{}-{}-{}".format(dsid, type, sfile)
         if not re.match(r'^/', locfile): locfile = PgLOG.join_paths(PgLOG.PGLOG['CURDIR'], locfile)
         if sarch and locfile == afile: sarch = 0

         PgLOG.pglog("{}: Archive Saved file from {} ...".format(sinfo, locfile),  PgLOG.WARNLG)
         vsnctl = 1 if pgrec and pgrec['vindex'] and pgrec['data_size'] else 0
         chksum = PgOPT.params['MC'][i]
         if sarch and (vsnctl or not override):
            info = PgFile.check_local_file(afile, 1, PgOPT.PGOPT['emerol'])
            if info:
               if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                  PgLOG.pglog("Saved-{}: Same-Checksum ARCHIVED at {}:{}".format(sinfo, info['date_modified'], info['time_modified']), PgOPT.PGOPT['emllog'])
                  sarch = 0
               elif info['data_size'] == lsize:
                  PgLOG.pglog("Saved-{}: Same-Size ARCHIVED at {}:{}".format(sinfo, info['date_modified'], info['time_modified']), PgOPT.PGOPT['wrnlog'])
                  sarch = 0
               elif vsnctl and not override:
                  PgLOG.pglog("Saved-{}: Cannot rearchive version controlled file".format(sinfo), PgOPT.PGOPT['extlog'])
                  PgOPT.params['SF'][i] = None
                  continue
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['G'] = PgLOG.PGLOG['DECSHOME']
               continue
         if oarch:
            replace = 0
            info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
            if info:
               if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                  PgLOG.pglog("Object-{}-{}: Same-Checksum ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), PgOPT.PGOPT['emllog'])
                  oarch = 0
               elif info['data_size'] == lsize:
                  PgLOG.pglog("Object-{}-{}: Same-Size ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), PgOPT.PGOPT['wrnlog'])
                  oarch = 0
               elif vsnctl and not override:
                  PgLOG.pglog("Object-{}-{}: Cannot rearchive version controlled file".format(bucket, ofile), PgOPT.PGOPT['extlog'])
                  PgOPT.params['SF'][i] = None
                  continue
               else:
                  replace = 1
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['O'] = bucket
               continue

         if (oarch + sarch) > 0:
            if sarch:
               if not PgFile.local_copy_local(afile, lfile, PgOPT.PGOPT['emerol']|override):
                  errcnt += 1
                  efiles[i] = 1
                  continue
               acnt += 1
            if oarch:
               if replace: PgFile.delete_object_file(ofile, bucket)
               if not PgFile.local_copy_object(ofile, lfile, bucket, None, PgOPT.PGOPT['emerol']|override):
                  errcnt += 1
                  efiles[i] = 1
                  continue
               ocnt += 1
            if PgLOG.PGLOG['DSCHECK']: chksize += lsize
            if PgSIG.PGSIG['BPROC'] > 1:
               afiles[i] = afile
               ofiles[i] = ofile
               lfiles[i] = lfile
               bgcnt += 1

         if PgSIG.PGSIG['BPROC'] < 2:
            if not fnames: fnames = PgOPT.get_field_keys(tname, None, "G")  # get setting fields if not yet
            info = None
            if sarch:
               info = PgFile.check_local_file(afile, 1, PgOPT.PGOPT['emerol'])
            elif oarch:
               info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
            elif pgrec:
               info = get_file_origin_info(sfile, pgrec)
            elif locflag == 'O':
               info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
            sid = set_one_savedfile(i, pgrec, sfile, fnames, type, info)
            if not sid:
               PgOPT.params['LF'][i] = PgOPT.params['SF'][i] = None
               continue

      if errcnt == 0 or errcnt >= perrcnt or PgLOG.PGLOG['DSCHECK']: break
      perrcnt = errcnt
      PgLOG.pglog("Rearchive failed {}/{} Saved file{} for {}!".format(errcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
      errcnt = reset_errmsg(0)

   if errcnt:
      if 'CL' in PgOPT.params: del PgOPT.params['CL']
      RETSTAT = reset_errmsg(errcnt)
      for i in range(bidx, ALLCNT):
         if efiles[i]: PgOPT.params['SF'][i] = ''
      if PgLOG.PGLOG['DSCHECK']:
         PgFile.check_storage_dflags(dflags, PgLOG.PGLOG['DSCHECK'], PgOPT.PGOPT['emerol'])
   if bgcnt:
      PgSIG.check_background(None, 0, PgLOG.LOGWRN, 1)
      for i in range(ALLCNT):
         if afiles[i]: validate_gladearch(afiles[i], lfiles[i], i)
         if ofiles[i]: validate_objectarch(ofiles[i], lfiles[i], bucket, i)
   if acnt > 0:
      PgLOG.pglog("{} of {} Saved file{} archived for {}".format(acnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if ocnt > 0:
      PgLOG.pglog("{] of {} Object file{} archived for {}".format(ocnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   PgLOG.pglog("{}/{} of {} Saved file record{} added/modified for {}!".format(ADDCNT, MODCNT, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (ADDCNT + MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# cross copy web files between glade and object store
#
def crosscopy_web_files(aname):

   global ALLCNT, MODCNT, RETSTAT
   tname = 'wfile'
   dsid = PgOPT.params['DS']
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0
   dflags = {}
   bucket = "rda-data"  # object store bucket
   PgLOG.pglog("Cross {} {} Web file{} of {} ...".format(aname, ALLCNT, s, dsid), PgLOG.WARNLG)

   PgOPT.validate_multiple_values(tname, ALLCNT)
   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} file{} processed for Web cross copy".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   reorder = errcnt = metatotal = metacnt = MODCNT = bgcnt = acnt = ocnt = 0
   perrcnt = ALLCNT
   efiles = [1]*ALLCNT
   PgOPT.params['LC'] = ['B']*ALLCNT
   if PgSIG.PGSIG['BPROC'] > 1:
      afiles = [None]*ALLCNT
      ofiles = [None]*ALLCNT
      warchs = [None]*ALLCNT
   fnames = None
   while True:
      for i in range(bidx, ALLCNT):
         if PgSIG.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
            if PgLOG.PGLOG['DSCHECK']:
               PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
            if metacnt >= PgOPT.PGOPT['RSMAX']:
               metatotal += PgMeta.process_metadata("W", metacnt, PgOPT.PGOPT['emerol'])
               metacnt = 0
            if 'EM' in PgOPT.params:
               PgLOG.PGLOG['PRGMSG'] = "{}/{} of {} web file{} archived/processed".format(acnt, i, ALLCNT, s)
         wfile = PgOPT.params['WF'][i]
         if not (efiles[i] and wfile): continue
         efiles[i] = 0
         type = PgOPT.params['WT'][i] if 'WT' in PgOPT.params else 'D'
         wfile = PgArch.get_web_path(i, wfile, 0, type)
         winfo = "{}-{}-{}".format(dsid, type, wfile)
         pgrec = PgSplit.pgget_wfile(dsid, "*", "wfile = '{}' AND type = '{}'".format(wfile, type), PgOPT.PGOPT['extlog'])
         if not pgrec:
            PgLOG.pglog("{}: Cannot Cross {} Web File not in RDADB".format(winfo, aname), PgOPT.PGOPT['errlog'])
            continue
         elif pgrec['locflag'] == 'C':
            PgLOG.pglog("{}: Cannot Cross {} Web File for CGD data".format(winfo, aname), PgOPT.PGOPT['extlog'])
         afile = PgArch.get_web_path(i, wfile, 1, type)
         ofile = PgLOG.join_paths(dsid, wfile)
         warch = oarch = 1
         PgLOG.pglog(winfo + ": Cross {} Web file ...".format(aname),  PgLOG.WARNLG)
         info = PgFile.check_local_file(afile, 0, PgOPT.PGOPT['emerol'])
         if info:
            warch = 0
         elif info is not None:
            errcnt += 1
            efiles[i] = 1
            dflags['G'] = PgLOG.PGLOG['DSDHOME']
            continue
         info = PgFile.check_object_file(ofile, bucket, 0, PgOPT.PGOPT['emerol'])
         if info:
            oarch = 0
         elif info is not None:
            errcnt += 1
            efiles[i] = 1
            dflags['O'] = bucket
            continue
         if warch and oarch:
            PgLOG.pglog(winfo + ": Cannot Cross {}, Neither Web Nor Object file Exists".format(aname), PgOPT.PGOPT['errlog'])
            continue
         elif (warch + oarch) == 0 and pgrec['locflag'] == 'B':
            PgLOG.pglog(winfo + ": No need Cross {}, Both Web & Object Exist".format(aname), PgOPT.PGOPT['wrnlog'])
            continue

         if warch:
            if not PgFile.object_copy_local(afile, ofile, bucket, PgOPT.PGOPT['emerol']|OVERRIDE):
               errcnt += 1
               efiles[i] = 1
               dflags['G'] = PgLOG.PGLOG['DSDHOME']
               continue
            if aname == 'Move':
               PgOPT.params['LC'][i] = 'G'
               PgFile.delete_object_file(ofile, bucket, PgOPT.PGOPT['extlog'])
            acnt += 1
               
         elif oarch:
            if not PgFile.local_copy_object(ofile, afile, bucket, None, PgOPT.PGOPT['emerol']|OVERRIDE):
               errcnt += 1
               efiles[i] = 1
               dflags['O'] = bucket
               continue
            if aname == 'Move':
               PgOPT.params['LC'][i] = 'O'
               PgFile.delete_local_file(afile, PgOPT.PGOPT['extlog'])
            ocnt += 1

         if PgSIG.PGSIG['BPROC'] > 1:
            afiles[i] = afile
            ofiles[i] = ofile
            warchs[i] = warch
            bgcnt += 1

         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']

         if PgSIG.PGSIG['BPROC'] < 2:
            if not fnames: fnames = PgOPT.get_field_keys(tname, None, "G")  # get setting fields if not yet
            info = get_file_origin_info(wfile, pgrec)
            wid = set_one_webfile(i, pgrec, wfile, fnames, type, info)
            if not wid:
               PgOPT.params['WF'][i] = None
               continue
            if 'GX' in PgOPT.params and PgOPT.PGOPT['GXTYP'].find(type) > -1:
               metacnt += PgMeta.record_meta_gather('W', dsid, wfile, PgOPT.params['DF'][i])
               PgMeta.cache_meta_tindex(dsid, wid, 'W')
            if pgrec['meta_link'] and pgrec['meta_link'] != 'N':
               if 'DX' in PgOPT.params or PgOPT.PGOPT['GXTYP'].find(type) < 0 and PgOPT.PGOPT['GXTYP'].find(pgrec['type']) > -1:
                  metacnt += PgMeta.record_meta_delete('W', dsid, wfile)
               elif 'GI' in PgOPT.params:
                  gindex = PgOPT.params['GI'][i]
                  if gindex != pgrec['gindex'] and (gindex or (PgOPT.OPTS['GI'][2]&2) == 0):
                     metacnt += PgMeta.record_meta_summary('W', dsid, gindex, pgrec['gindex'])

      if errcnt == 0 or errcnt >= perrcnt or PgLOG.PGLOG['DSCHECK']: break
      perrcnt = errcnt
      PgLOG.pglog("Recopy failed {}/{} Web file{} for {}".format(errcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
      errcnt = reset_errmsg(0)

   if errcnt:
      RETSTAT = reset_errmsg(errcnt)
      for i in range(bidx, ALLCNT):
         if efiles[i]: PgOPT.params['WF'][i] = ''
      if PgLOG.PGLOG['DSCHECK']:
         PgFile.check_storage_dflags(dflags, PgLOG.PGLOG['DSCHECK'], PgOPT.PGOPT['emerol'])
   if bgcnt:
      PgSIG.check_background(None, 0, PgLOG.LOGWRN, 1)
      for i in range(ALLCNT):
         if warchs[i]:
            validate_gladearch(afiles[i], "{}-{}".format(bucket, ofiles[i]), i)
         elif ofiles[i]:
            validate_objectarch(ofiles[i], afiles[i], bucket, i)
   astr = 'Moved' if aname == 'Move' else 'Copied'
   if acnt > 0: PgLOG.pglog("{} of {} Web file{} Cross {} for {}".format(acnt, ALLCNT, s, astr, dsid), PgOPT.PGOPT['emllog'])
   if ocnt > 0: PgLOG.pglog("{} of {} Object file{} Cross {} for {}".format(ocnt, ALLCNT, s, astr, dsid), PgOPT.PGOPT['emllog'])
   if metacnt > 0:
      metatotal += PgMeta.process_metadata('W', metacnt, PgOPT.PGOPT['emerol'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (metatotal + MODCNT + reorder) > 0:
      PgLOG.pglog("{} of {} Web file record{} modified for {}!".format(MODCNT, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# cross copy help files between glade and object store
#
def crosscopy_help_files(aname):

   global ALLCNT, MODCNT, RETSTAT
   tname = 'hfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0
   dflags = {}
   bucket = "rda-data"
   PgLOG.pglog("Cross {} {} Help file{} of {} ...".format(aname, ALLCNT, s, dsid), PgLOG.WARNLG)

   PgOPT.validate_multiple_values(tname, ALLCNT)
   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} file{} processed for HELP archive".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   reorder = errcnt = MODCNT = bgcnt = acnt = ocnt = 0
   perrcnt = ALLCNT
   efiles = [1]*ALLCNT
   PgOPT.params['LC'] = ['B']*ALLCNT
   if PgSIG.PGSIG['BPROC'] > 1:
      afiles = [None]*ALLCNT
      ofiles = [None]*ALLCNT
      harchs = [None]*ALLCNT
   fnames = None
   while True:
      for i in range(bidx, ALLCNT):
         if PgSIG.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
            if PgLOG.PGLOG['DSCHECK']:
               PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
            if 'EM' in PgOPT.params:
               PgLOG.PGLOG['PRGMSG'] = "{}/{} of {} help file{} archived/processed".format(acnt, i, ALLCNT, s)
         hfile = PgOPT.params['HF'][i]
         if not (efiles[i] and hfile): continue
         efiles[i] = 0
         if 'HT' in PgOPT.params and PgOPT.params['HT'][i]:
            type = PgOPT.params['HT'][i]
            if type not in PgOPT.HTYPE:
               PgLOG.pglog("{}-{}: Invalid Help file Type '{}' to Archive".format(dsid, hfile, type), PgOPT.PGOPT['emerol'])
               continue
         else:
            PgLOG.pglog("{}-{}: Miss Help file Type to Archive".format(dsid, hfile), PgOPT.PGOPT['errlog'])
            continue
         stype = PgOPT.HTYPE[type] if type in PgOPT.HTYPE else 'Help'
         hfile = PgArch.get_help_path(i, hfile, 0, type)
         afile = PgArch.get_help_path(i, hfile, 1, type)
         hpath = PgOPT.HPATH[type] if type in PgOPT.HPATH else 'help'
         ofile = PgArch.get_object_path(hfile, dsid, hpath)
         hinfo = "{}-{}-{}".format(dsid, type, hfile)
         pgrec = PgDBI.pgget(tname, "*", "{} and hfile = '{}' AND type = '{}'".format(dcnd, hfile, type), PgOPT.PGOPT['extlog'])
         if not pgrec:
            PgLOG.pglog(hinfo + ": Fail to Cross {} for Help file not in RDADB".format(aname), PgOPT.PGOPT['emlerr'])
            continue
         if pgrec['locflag'] == 'R':
            url = pgrec['url']
            if not url: url = 'URL'
            PgLOG.pglog("{}: Cannot Cross {} Help File on {}".format(hinfo, aname, url), PgOPT.PGOPT['emlerr'])
            continue
         if pgrec and PgOPT.params['HF'][i] != hfile: PgOPT.params['HF'][i] = hfile
         harch = oarch = 1
         PgLOG.pglog(hinfo + ": Cross {} Help file ...".format(aname),  PgLOG.WARNLG)
         info = PgFile.check_local_file(afile, 0, PgOPT.PGOPT['emerol'])
         if info:
            harch = 0
         elif info is not None:
            errcnt += 1
            efiles[i] = 1
            dflags['G'] = PgLOG.PGLOG['DECSHOME']
            continue
         info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
         if info:
            oarch = 0
         elif info is not None:
            errcnt += 1
            efiles[i] = 1
            dflags['O'] = bucket
            continue

         if harch and oarch:
            PgLOG.pglog(hinfo + ": Cannot Cross {} Help file, on Neither Glade Nor Object".format(aname), PgOPT.PGOPT['errlog'])
            continue
         elif not (harch or oarch) and pgrec['locflag'] == 'B':
            PgLOG.pglog(hinfo + ": No need Cross {} Help file, on Both Glade & Object".format(aname), PgOPT.PGOPT['wrnlog'])
            continue

         if harch:
            if not PgFile.object_copy_local(afile, ofile, bucket, PgOPT.PGOPT['emerol']|OVERRIDE):
               errcnt += 1
               efiles[i] = 1
               dflags['G'] = PgLOG.PGLOG['DECSHOME']
               continue
            if aname == 'Move':
               PgOPT.params['LC'][i] = 'G'
               PgFile.delete_object_file(ofile, bucket, PgOPT.PGOPT['extlog'])
            acnt += 1
         elif oarch:
            if not PgFile.local_copy_object(ofile, afile, bucket, None, PgOPT.PGOPT['emerol']|OVERRIDE):
               errcnt += 1
               efiles[i] = 1
               dflags['O'] = bucket
               continue
            if aname == 'Move':
               PgOPT.params['LC'][i] = 'O'
               PgFile.delete_local_file(afile, PgOPT.PGOPT['extlog'])
            ocnt += 1

         if PgSIG.PGSIG['BPROC'] > 1:
            afiles[i] = afile
            ofiles[i] = ofile
            harchs[i] = harch
            bgcnt += 1

         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']

         if PgSIG.PGSIG['BPROC'] < 2:
            if not fnames: fnames = PgOPT.get_field_keys(tname, None, "G")  # get setting fields if not yet
            info = get_file_origin_info(hfile, pgrec)
            hid = set_one_helpfile(i, pgrec, hfile, fnames, type, info)
            if not hid:
               PgOPT.params['HF'][i] = None
               continue

      if errcnt == 0 or errcnt >= perrcnt or PgLOG.PGLOG['DSCHECK']: break
      perrcnt = errcnt
      PgLOG.pglog("Re{} {} Help file{} for {}".format(aname, errcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
      errcnt = reset_errmsg(0)

   if errcnt:
      RETSTAT = reset_errmsg(errcnt)
      for i in range(bidx, ALLCNT):
         if efiles[i]: PgOPT.params['SF'][i] = ''
      if PgLOG.PGLOG['DSCHECK']:
         PgFile.check_storage_dflags(dflags, PgLOG.PGLOG['DSCHECK'], PgOPT.PGOPT['emerol'])
   if bgcnt:
      PgSIG.check_background(None, 0, PgLOG.LOGWRN, 1)
      for i in range(ALLCNT):
         if harchs[i]:
            validate_gladearch(afiles[i], "{}-{}".format(bucket, ofiles[i]), i)
         elif ofiles[i]:
            validate_objectarch(ofiles[i], afiles[i], bucket, i)
   astr = 'Moved' if aname == 'Move' else 'Copied'
   if acnt > 0: PgLOG.pglog("{} of {} Help file{} Cross {} for {}".format(acnt, ALLCNT, s, astr, dsid), PgOPT.PGOPT['emllog'])
   if ocnt > 0: PgLOG.pglog("{} of {} Object file{} Cross {} for {}".format(ocnt, ALLCNT, s, astr, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   PgLOG.pglog("{} of {} Help file record{} modified for {}!".format(MODCNT, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if 'ON' in PgOPT.params: reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# cross copy save files between glade and object store
#
def crosscopy_saved_files(aname):

   global ALLCNT, MODCNT, RETSTAT
   tname = 'sfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0
   dflags = {}
   bucket = "rda-decsdata"
   PgLOG.pglog("Cross {} {} Saved file{} of {} ...".format(aname, ALLCNT, s, dsid), PgLOG.WARNLG)

   PgOPT.validate_multiple_values(tname, ALLCNT)
   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} file{} processed for SAVED archive".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   reorder = errcnt = MODCNT = bgcnt = acnt = ocnt = 0
   perrcnt = ALLCNT
   efiles = [1]*ALLCNT
   PgOPT.params['LC'] = ['B']*ALLCNT
   if PgSIG.PGSIG['BPROC'] > 1:
      afiles = [None]*ALLCNT
      ofiles = [None]*ALLCNT
      sarchs = [None]*ALLCNT
   fnames = None
   while True:
      for i in range(bidx, ALLCNT):
         if PgSIG.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
            if PgLOG.PGLOG['DSCHECK']:
               PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
            if 'EM' in PgOPT.params:
               PgLOG.PGLOG['PRGMSG'] = "{}/{} of {} saved file{} archived/processed".format(acnt, i, ALLCNT, s)
         sfile = PgOPT.params['SF'][i]
         if not (efiles[i] and sfile): continue
         efiles[i] = 0
         if 'ST' in PgOPT.params and PgOPT.params['ST'][i]:
            type = PgOPT.params['ST'][i]
            if PgOPT.PGOPT['SDTYP'].find(type) < 0:
               PgLOG.pglog("{}-{}: Invalid Saved file Type '{}' to Archive".format(dsid, sfile, type), PgOPT.PGOPT['emerol'])
               continue
         else:
            PgLOG.pglog("{}-{}: Miss Saved file Type to Archive".format(dsid, sfile), PgOPT.PGOPT['errlog'])
            continue
         sfile = PgArch.get_saved_path(i, sfile, 0, type)
         afile = PgArch.get_saved_path(i, sfile, 1, type)
         ofile = PgLOG.join_paths(dsid, sfile)
         sinfo = "{}-{}-{}".format(dsid, type, sfile)
         pgrec = PgDBI.pgget(tname, "*", "{} and sfile = '{}' AND type = '{}'".format(dcnd, sfile, type), PgOPT.PGOPT['extlog'])
         if not pgrec:
            PgLOG.pglog("{}: Fail to Cross {} for Saved file not in RDADB".format(sinfo, aname), PgOPT.PGOPT['emlerr'])
            continue
         elif pgrec['locflag'] == 'C':
            PgLOG.pglog("{}: Fail to Cross {} Saved File for CGD data".format(sinfo, aname), PgOPT.PGOPT['extlog'])
         if pgrec and PgOPT.params['SF'][i] != sfile: PgOPT.params['SF'][i] = sfile
         sarch = oarch = 1
         PgLOG.pglog(sinfo + ": Cross {} Saved file ...".format(aname),  PgLOG.WARNLG)
         info = PgFile.check_local_file(afile, 0, PgOPT.PGOPT['emerol'])
         if info:
            sarch = 0
         elif info is not None:
            errcnt += 1
            efiles[i] = 1
            dflags['G'] = PgLOG.PGLOG['DECSHOME']
            continue
         info = PgFile.check_object_file(ofile, bucket, 1, PgOPT.PGOPT['emerol'])
         if info:
            oarch = 0
         elif info is not None:
            errcnt += 1
            efiles[i] = 1
            dflags['O'] = bucket
            continue

         if sarch and oarch:
            PgLOG.pglog(sinfo + ": Cannot Cross {}, Neither Saved Nor Object file Exists".format(aname), PgOPT.PGOPT['errlog'])
            continue
         elif not (sarch or oarch) and pgrec['locflag'] == 'B':
            PgLOG.pglog(sinfo + ": No need Cross {}, Both Saved & Object Exist".format(aname), PgOPT.PGOPT['wrnlog'])
            continue

         if sarch:
            if not PgFile.object_copy_local(afile, ofile, bucket, PgOPT.PGOPT['emerol']|OVERRIDE):
               errcnt += 1
               efiles[i] = 1
               dflags['G'] = PgLOG.PGLOG['DECSHOME']
               continue
            if aname == 'Move':
               PgOPT.params['LC'][i] = 'G'
               PgFile.delete_object_file(ofile, bucket, PgOPT.PGOPT['extlog'])
            acnt += 1
         elif oarch:
            if not PgFile.local_copy_object(ofile, afile, bucket, None, PgOPT.PGOPT['emerol']|OVERRIDE):
               errcnt += 1
               efiles[i] = 1
               dflags['O'] = bucket
               continue
            if aname == 'Move':
               PgOPT.params['LC'][i] = 'O'
               PgFile.delete_local_file(afile, PgOPT.PGOPT['extlog'])
            ocnt += 1

         if PgSIG.PGSIG['BPROC'] > 1:
            afiles[i] = afile
            ofiles[i] = ofile
            sarchs[i] = sarch
            bgcnt += 1

         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']

         if PgSIG.PGSIG['BPROC'] < 2:
            if not fnames: fnames = PgOPT.get_field_keys(tname, None, "G")  # get setting fields if not yet
            info = get_file_origin_info(sfile, pgrec)
            sid = set_one_savedfile(i, pgrec, sfile, fnames, type, info)
            if not sid:
               PgOPT.params['SF'][i] = None
               continue

      if errcnt == 0 or errcnt >= perrcnt or PgLOG.PGLOG['DSCHECK']: break
      perrcnt = errcnt
      PgLOG.pglog("Recopy {} Saved file{} for {}".format(errcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
      errcnt = reset_errmsg(0)

   if errcnt:
      RETSTAT = reset_errmsg(errcnt)
      for i in range(bidx, ALLCNT):
         if efiles[i]: PgOPT.params['SF'][i] = ''
      if PgLOG.PGLOG['DSCHECK']:
         PgFile.check_storage_dflags(dflags, PgLOG.PGLOG['DSCHECK'], PgOPT.PGOPT['emerol'])
   if bgcnt:
      PgSIG.check_background(None, 0, PgLOG.LOGWRN, 1)
      for i in range(ALLCNT):
         if sarchs[i]:
            validate_gladearch(afiles[i], "{}-{}".format(bucket, ofiles[i]), i)
         elif ofiles[i]:
            validate_objectarch(ofiles[i], afiles[i], bucket, i)
   astr = 'Moved' if aname == 'Move' else 'Copied'
   if acnt > 0: PgLOG.pglog("{} of {} Saved file{} Cross {} for {}".format(acnt, ALLCNT, s, astr, dsid), PgOPT.PGOPT['emllog'])
   if ocnt > 0: PgLOG.pglog("{} of {} Object file{} Cross {} for {}".format(ocnt, ALLCNT, s, astr, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   PgLOG.pglog("{} of {} Saved file record{} modified for {}!".format(MODCNT, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if 'ON' in PgOPT.params: reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# get backup file names in RDADB and on Quaser server for given dataset id
#
def get_backup_filenames(bfile, dsid):

   ms = re.match(r'^/{}/(.+)$'.format(dsid), bfile)
   if ms:
      qfile = bfile
      bfile = ms.group(1)
   else:
      qfile = "/{}/{}".format(dsid, bfile)
   
   return (bfile, qfile)

#
# archive a backup file for given wfiles/sfiles
#
def archive_backup_file():

   tname = 'bfile'
   endpoint = PgLOG.PGLOG['BACKUPEP']
   drpoint = PgLOG.PGLOG['DRDATAEP']
   dsid = PgOPT.params['DS']
   qtype = PgOPT.params['QT'][0]
   (bfile, qfile) = get_backup_filenames(PgOPT.params['QF'][0], dsid)
   dobackup = 0 if 'TO' in PgOPT.params else 1
   note = PgOPT.params['DE'][0] if 'DE' in PgOPT.params else None
   chkstat = False if re.search(r'_changed.tar$', bfile) else True

   pgbck = PgDBI.pgget(tname, '*', "dsid = '{}' AND bfile = '{}'".format(dsid, bfile), PgOPT.PGOPT['extlog'])
   if pgbck and not OVERRIDE and pgbck['checksum']:
      return PgLOG.pglog(bfile + ": file in RDADB, delete it or add Option -OE to backup again", PgOPT.PGOPT['extlog'])
   if dobackup and not OVERRIDE and PgFile.check_backup_file(qfile, endpoint, 0, PgOPT.PGOPT['extlog']):
      return PgLOG.pglog(qfile +": Backup file on Quasar, add Option -OE to override", PgOPT.PGOPT['extlog'])
   endpath = 'decsdata' if 'SF' in PgOPT.params else 'data'
   fromfile = "/{}/{}/{}/{}".format(endpath, endpoint, dsid, op.basename(bfile))

   tarfile = PgLOG.PGLOG['DSSDATA'] + fromfile
   PgFile.make_local_directory(op.dirname(tarfile), PgOPT.PGOPT['extlog'])
   if PgFile.check_local_file(tarfile, 0, PgOPT.PGOPT['extlog']):
      if not OVERRIDE:
         return PgLOG.pglog(fromfile + ": exists for Quasar backup, add Option -OE to override", PgOPT.PGOPT['extlog'])
      PgFile.delete_local_file(tarfile, PgOPT.PGOPT['extlog'])

   tfmt = 'TAR'

   ccnt = scnt = wcnt = 0
   ifcnt = len(PgOPT.params['IF']) if 'IF' in PgOPT.params else 0 
   if PgLOG.PGLOG['DSCHECK']:
      if 'SF' in PgOPT.params:
         ccnt = len(PgOPT.params['SF'])
      elif 'WF' in PgOPT.params:
         ccnt = len(PgOPT.params['WF'])
      ifidx = 1
      while ifidx < ifcnt:
         buf = PgLOG.pgsystem("wc -l " + PgOPT.params['WF'][ifidx], PgLOG.LOGWRN, 16)
         ms = re.match(r'^(\d+)', buf)
         if ms: ccnt += int(ms.group(1))
         ifidx += 1
      ccnt *= 3
      PgCMD.set_dscheck_fcount(ccnt, PgOPT.PGOPT['extlog'])

   tinfo = {'bid' : 0, 'size' : 0, 'cnt' : 0, 'afmt' : '', 'dfmt' : '', 'sids' : [], 'wids' : []}
   if pgbck: tinfo['bid'] = pgbck['bid']
   ifidx = 1
   while True:
      if 'SF' in PgOPT.params:
         scnt += tar_backup_savedfiles(tarfile, tinfo, ccnt, chkstat)
      elif 'WF' in PgOPT.params:
         wcnt += tar_backup_webfiles(tarfile, tinfo, ccnt, chkstat)
      if ifidx >= ifcnt: break  # no more input file to read
      PgOPT.params['DS'] = PgOPT.read_one_infile(PgOPT.params['IF'][ifidx])
      ifidx += 1

   info = PgFile.check_local_file(tarfile, 33)   # 1+32
   fsize = info['data_size'] if info else 0
   if fsize < tinfo['size']:
      PgLOG.pglog("{}: Backup file size {} is less than total file size {}".format(tarfile, fsize, tinfo['size']), PgOPT.PGOPT['extlog'])
   if fsize < PgLOG.PGLOG['ONEGBS']:
      PgLOG.pglog("{}: Backup file size {} is less than one GB".format(tarfile, fsize), PgOPT.PGOPT['extlog'])
   record = {'type' : qtype, 'data_format' : tinfo['dfmt'], 'data_size' : fsize,
             'uid' : PgOPT.PGOPT['UID'], 'checksum' : info['checksum'],
             'scount' : scnt, 'wcount' : wcnt}
   record['file_format'] = PgOPT.append_format_string(tinfo['afmt'], tfmt, 1)
   record['date_created'] = record['date_modified'] = info['date_modified']
   record['time_created'] = record['time_modified'] = info['time_modified']

   if dobackup:
      if qtype == 'D':
         dstat = PgFile.local_copy_backup(qfile, fromfile, drpoint, PgOPT.PGOPT['errlog']|OVERRIDE)
         if not dstat: PgLOG.pglog("{}: Error Quaser Drdata for {}".format(bfile, dsid), PgOPT.PGOPT['extlog'])
      else:
         dstat = -1
      bstat = PgFile.local_copy_backup(qfile, fromfile, endpoint, PgOPT.PGOPT['errlog']|OVERRIDE)
      if not bstat: PgLOG.pglog("{}: Error Quaser Backup for {}".format(bfile, dsid), PgOPT.PGOPT['extlog'])

      if dstat == PgLOG.FINISH: dstat = PgFile.check_globus_finished(qfile, drpoint, PgOPT.PGOPT['errlog']|PgLOG.NOWAIT)
      if bstat == PgLOG.FINISH: bstat = PgFile.check_globus_finished(qfile, endpoint, PgOPT.PGOPT['errlog']|PgLOG.NOWAIT)
      if dstat and bstat:
         PgFile.delete_local_file(tarfile, PgOPT.PGOPT['extlog'])
         msg = tarfile + ": local tar file is removed"
         PgLOG.pglog(msg, PgOPT.PGOPT['wrnlog'])
      else:
         msg = tarfile + ": backup action is not complete and local tar file is not removed"
         PgLOG.pglog(msg, PgOPT.PGOPT['extlog'])
      record['status' ] = 'A'
   else:
      record['status' ] = 'T'
   bid = set_one_backfile(0, pgbck, bfile, None, qtype, dsid, record)
   if not bid:   # should not happen
      PgLOG.pglog("{}: Error add Quaser Backup file name in RDADB for {}".format(bfile, dsid), PgOPT.PGOPT['extlog'])

   tcnt = tinfo['cnt']
   tsize = tinfo['size']
   brec = {'bid' : bid}
   if scnt:
      for sid in tinfo['sids']:
         tcnt += PgDBI.pgupdt("sfile", brec, "sid = {}".format(sid))
         if ccnt and tcnt%20 == 0: PgCMD.set_dscheck_dcount(tcnt, tsize, PgOPT.PGOPT['extlog'])
   if wcnt:
      for wid in tinfo['wids']:
         tcnt += PgSplit.pgupdt_wfile(dsid, brec, "wid = {}".format(wid))
         if ccnt and tcnt%20 == 0: PgCMD.set_dscheck_dcount(tcnt, tsize, PgOPT.PGOPT['extlog'])

   if ccnt: PgCMD.set_dscheck_dcount(ccnt, tsize, PgOPT.PGOPT['extlog'])
   if dobackup:
      msg = "{}/{} Web/Saved files backed up to {} on '{}'".format(wcnt, scnt, qfile, endpoint)
      if dstat > 0: msg += " and '{}'".format(drpoint)
   else:
      msg = "{}/{} Web/Saved files tar to {}".format(wcnt, scnt, tarfile)
   PgLOG.pglog(msg, PgOPT.PGOPT['wrnlog'])
        
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   PgDBI.reset_rdadb_version(dsid)

   PgFile.record_delete_directory(None, (PgOPT.params['DD'] if 'DD' in PgOPT.params else 0))

#
# tarring saved files to Quasar backup file
#
def tar_backup_savedfiles(tarfile, tinfo, ccnt, chkstat):

   scnt = len(PgOPT.params['SF'])
   dsid = PgOPT.params['DS']
   fcnd = "dsid = '{}' and sfile = ".format(dsid)
   s = 's' if scnt > 1 else ''
   PgLOG.pglog("tar {} Saved file{} of {} to {} ...".format(scnt, s, dsid, tarfile), PgLOG.WARNLG)
   PgOPT.validate_multiple_options(scnt, ["ST", 'DF', 'AF'])
   if 'ST' not in PgOPT.params:
      PgOPT.params['ST'] = [None]*scnt
      PgOPT.INOPTS['ST'] = 1
   if 'DF' not in PgOPT.params:
      PgOPT.params['DF'] = [None]*scnt
      PgOPT.INOPTS['DF'] = 1
   if 'AF' not in PgOPT.params:
      PgOPT.params['AF'] = [None]*scnt
      PgOPT.INOPTS['AF'] = 1
   dshome = "{}/{}".format(PgLOG.PGLOG['DECSHOME'], dsid)
   tarhome = "{}/{}/{}".format(PgLOG.PGLOG['DECSHOME'], PgLOG.PGLOG['BACKUPEP'], dsid)
   fields = "sid, sfile, dsid, type, data_size, data_format, file_format, bid, locflag, date_modified"
   tcnt = tinfo['cnt']
   tsize = tinfo['size']
   dfmt = tinfo['dfmt']
   afmt = tinfo['afmt']
   sids = tinfo['sids']
   topt = ('u' if tcnt else 'c')

   # loop through the saved files to validate and build tar commands
   tarcmds = [None]*scnt
   tmpfiles = [None]*scnt
   pdfmt = pafmt = None
   for i in range(scnt):
      if ccnt and tcnt%20 == 0: PgCMD.set_dscheck_dcount(tcnt, tsize, PgOPT.PGOPT['extlog'])
      sfile = PgOPT.params['SF'][i]
      stype = PgOPT.params['ST'][i]
      relative = 0 if re.match(r'^/', sfile) else 1
      if not stype:
         ms = re.match(r'^([{}])/((.+)$'.format(PgOPT.PGOPT['SDTYP']), sfile)
         if ms:
            stype = ms.group(1)
            sfile = ms.group(2)
      tcnd = " AND type = '{}'".format(stype) if stype else ''
      pgrec = PgDBI.pgget('sfile', fields, "{}'{}'{}".format(fcnd, sfile, tcnd), PgOPT.PGOPT['extlog'])
      if not pgrec:
         PgLOG.pglog("{}-{}: Saved file record not in RDADB".format(dsid, sfile), PgOPT.PGOPT['extlog'])
      elif chkstat and pgrec['bid'] and pgrec['bid'] != tinfo['bid']:
         PgGLBS.file_backup_status(pgrec, 0, PgOPT.PGOPT['extlog'])
      if not stype: stype = pgrec['type']
      if pgrec['locflag'] == 'O':
         tardir = tarhome
         savedfile = "{}/{}".format(tardir, sfile)
         ofile = PgLOG.join_paths(dsid, sfile)
         PgFile.object_copy_local(savedfile, ofile, 'rda-decsdata', PgOPT.PGOPT['wrnlog'])
         tmpfiles[i] = savedfile
      else:
         tardir = "{}/{}".format(dshome, stype)
         savedfile = "{}/{}".format(tardir, sfile) if relative else sfile
      if not op.isfile(savedfile):
         PgLOG.pglog(savedfile + ": Saved file not exists to backup", PgOPT.PGOPT['extlog'])
      if relative:
         tarcmds[i] = "tar -{}vf {} -C {} {}".format(topt, tarfile, tardir, sfile)
      else:
         tarcmds[i] = "tar -{}vf {} {}".format(topt, tarfile, sfile)
      topt = 'u'
      tcnt += 1
      sids.append(pgrec['sid'])
      tsize += pgrec['data_size']
      # get combined data format
      if PgOPT.params['DF'][i]: pgrec['data_format'] = PgOPT.params['DF'][i]
      if PgOPT.params['AF'][i]: pgrec['file_format'] = PgOPT.params['AF'][i]
      if pgrec['data_format'] and pgrec['data_format'] != pdfmt:
         pdfmt = pgrec['data_format']
         dfmt = PgOPT.append_format_string(dfmt, pdfmt)
      if pgrec['file_format'] and pgrec['file_format'] != pafmt:
         pafmt = pgrec['file_format']
         afmt = PgOPT.append_format_string(afmt, pafmt)

   # do tar actions
   for i in range(scnt):
      if ccnt and tcnt%20 == 0: PgCMD.set_dscheck_dcount(tcnt, tsize, PgOPT.PGOPT['extlog'])
      tcnt += PgLOG.pgsystem(tarcmds[i], PgOPT.PGOPT['extlog'], 5)
      if tmpfiles[i]: PgFile.delete_local_file(tmpfiles[i], PgOPT.PGOPT['extlog'])

   tinfo['cnt'] = tcnt
   tinfo['size'] = tsize
   tinfo['dfmt'] = dfmt
   tinfo['afmt'] = afmt
   tinfo['sids'] = sids

   return scnt

#
# tarring web files to Quasar backup file
#
def tar_backup_webfiles(tarfile, tinfo, ccnt, chkstat):

   wcnt = len(PgOPT.params['WF'])
   dsid = PgOPT.params['DS']
   fcnd = "wfile = "
   s = 's' if wcnt > 1 else ''
   PgLOG.pglog("tar {} Web file{} of {} to {} ...".format(wcnt, s, dsid, tarfile), PgLOG.WARNLG)
   PgOPT.validate_multiple_options(wcnt, ["WT", 'DF', 'AF'])
   if 'WT' not in PgOPT.params:
      PgOPT.params['WT'] = [None]*wcnt
      PgOPT.INOPTS['WT'] = 1
   if 'DF' not in PgOPT.params:
      PgOPT.params['DF'] = [None]*wcnt
      PgOPT.INOPTS['DF'] = 1
   if 'AF' not in PgOPT.params:
      PgOPT.params['AF'] = [None]*wcnt
      PgOPT.INOPTS['AF'] = 1
   dshome = "{}/{}".format(PgLOG.PGLOG['DSDHOME'], dsid)
   tarhome = "{}/{}/{}".format(PgLOG.PGLOG['DSDHOME'], PgLOG.PGLOG['BACKUPEP'], dsid)
   tcnt = tinfo['cnt']
   tsize = tinfo['size']
   dfmt = tinfo['dfmt']
   afmt = tinfo['afmt']
   wids = tinfo['wids']
   topt = ('u' if tcnt else 'c')

   # loop through web files to validate and build tar commands
   tarcmds = [None]*wcnt
   tmpfiles = [None]*wcnt
   pdfmt = pafmt = None
   for i in range(wcnt):
      if ccnt and (tcnt%20) == 0: PgCMD.set_dscheck_dcount(tcnt, tsize, PgOPT.PGOPT['extlog'])
      wfile = PgOPT.params['WF'][i]
      wtype = PgOPT.params['WT'][i]
      fcnd = "wfile = '{}'".format(wfile)
      relative = 0 if re.match(r'^/', wfile) else 1
      if wtype: fcnd += " AND type = '{}'".format(wtype)
      pgrec = PgSplit.pgget_wfile(dsid, '*', fcnd, PgOPT.PGOPT['extlog'])
      if not pgrec:
         PgLOG.pglog("{}-{}: Web file record not in RDADB".format(dsid, wfile), PgOPT.PGOPT['extlog'])
      elif chkstat and pgrec['bid'] and pgrec['bid'] != tinfo['bid']:
         PgGLBS.file_backup_status(pgrec, 0, PgOPT.PGOPT['extlog'])
      if pgrec['locflag'] == 'O':
         tardir = tarhome
         webfile = "{}/{}".format(tardir, wfile)
         ofile = PgLOG.join_paths(dsid, wfile)
         PgFile.object_copy_local(webfile, ofile, 'rda-data', PgOPT.PGOPT['wrnlog'])
         tmpfiles[i] = webfile
      else:
         tardir = dshome
         webfile = "{}/{}".format(tardir, wfile) if relative else wfile
      if not op.isfile(webfile):
         PgLOG.pglog(webfile + ": Web file not exists to backup", PgOPT.PGOPT['extlog'])
      if relative:
         tarcmds[i] = "tar -{}vf {} -C {} {}".format(topt, tarfile, tardir, wfile)
      else:
         tarcmds[i] = "tar -{}vf {} {}".format(topt, tarfile, wfile)
      topt = 'u'
      tcnt += 1
      wids.append(pgrec['wid'])
      tsize += pgrec['data_size']
      # get combined data format
      if PgOPT.params['DF'][i]: pgrec['data_format'] = PgOPT.params['DF'][i]
      if PgOPT.params['AF'][i]: pgrec['file_format'] = PgOPT.params['AF'][i]
      if pgrec['data_format'] and pgrec['data_format'] != pdfmt:
         pdfmt = pgrec['data_format']
         dfmt = PgOPT.append_format_string(dfmt, pdfmt)
      if pgrec['file_format'] and pgrec['file_format'] != pafmt:
         pafmt = pgrec['file_format']
         afmt = PgOPT.append_format_string(afmt, pafmt)

   # do tar actions
   for i in range(wcnt):
      if ccnt and tcnt%20 == 0: PgCMD.set_dscheck_dcount(tcnt, tsize, PgOPT.PGOPT['extlog'])
      tcnt += PgLOG.pgsystem(tarcmds[i], PgOPT.PGOPT['extlog'], 5)
      if tmpfiles[i]: PgFile.delete_local_file(tmpfiles[i], PgOPT.PGOPT['extlog'])

   tinfo['cnt'] = tcnt
   tinfo['size'] = tsize
   tinfo['dfmt'] = dfmt
   tinfo['afmt'] = afmt
   tinfo['wids'] = wids

   return wcnt

#
# retrieve backup tar files from Quasar servers
#
def retrieve_backup_files():

   tname = 'bfile'
   endpoint = PgLOG.PGLOG['BACKUPEP']
   dsid = PgOPT.params['DS']
   dcnt = bidx = chksize = 0
   s = 's' if ALLCNT > 1 else ''
   PgFile.check_block_path(PgLOG.PGLOG['CURDIR'], "Retrieve Backup file{}".format(s), PgOPT.PGOPT['extlog'])
   PgLOG.pglog("Retrieving {} Backup file{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Backup file{} downloaded".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{} of {} Backup file{} downloaded/processed".format(dcnt, i, ALLCNT, s)
      (bfile, qfile) = get_backup_filenames(PgOPT.params['QF'][i], dsid)
      pgrec = PgDBI.pgget(tname, "*", "bfile = '{}'".format(bfile), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("Backup-{}: is not in RDADB".format(bfile, PgLOG.PGLOG['MISSFILE']), PgLOG.LOGWRN)
         continue
      ms = re.match(r'^<(ds\d+\.\d+)_(\w)_\d+.txt>', pgrec['note'])
      if not ms:
         PgLOG.pglog("Backup-{}: Note field not formatted properly\n{}".format(bfile, pgrec['note']), PgLOG.LGEREX)
      fdsid = ms.group(1)
      ftype = ms.group(2)
      if fdsid != dsid: qfile = "/{}/{}".format(fdsid, bfile)
      info = PgFile.check_backup_file(qfile)
      if not info:
         PgLOG.pglog("Backup-{}: {}".format(qfile, PgLOG.PGLOG['MISSFILE']), PgLOG.LOGWRN)
         continue

      endpath = 'decsdata' if ftype == 'S' else 'data'
      todir = "/{}/{}/{}".format(endpath, endpoint, fdsid)
      tardir = "{}{}".format(PgLOG.PGLOG['DSSDATA'], todir)
      PgFile.make_local_directory(tardir, PgOPT.PGOPT['extlog'])
      tofile = "{}/{}".format(todir, op.basename(bfile))
      tarfile = "{}/{}".format(tardir, op.basename(bfile))
      TARFILES[pgrec['bid']] = tarfile
      tinfo = PgFile.check_local_file(tarfile, 0, PgOPT.PGOPT['extlog'])
      if tinfo:
         PgLOG.pglog(tarfile + ": tar file exists already", PgOPT.PGOPT['wrnlog'])
      elif PgFile.backup_copy_local(tofile, qfile, endpoint, PgOPT.PGOPT['extlog']):
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         dcnt += 1
         PgFile.set_local_mode(tarfile, 1, 0o664)

   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   PgLOG.pglog("{} of Quasar Backup file{} downloaded for {}".format(dcnt, ALLCNT, s, dsid), PgLOG.LOGWRN)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# untar backup file to retrieve web files
#
def restore_backup_webfiles():

   tname = 'wfile'
   dsid = PgOPT.params['DS']
   bucket = "rda-data"    # default object store bucket
   s = 's' if ALLCNT > 1 else ''
   wcnt = ocnt = bidx = chksize = 0
   dflags = {}
   PgLOG.pglog("Restore {} Web file{} of {} from backup ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Web file{} restored".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']
   if 'WT' not in PgOPT.params:
      PgOPT.params['WT'] = ['D']*ALLCNT
      PgOPT.OPTS['WT'][2] |= 2

   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{} of {} web file{} restored/processed".format(wcnt, i, ALLCNT, s)
      wfile = PgOPT.params['WF'][i]
      type = PgOPT.params['WT'][i]
      pgrec = PgSplit.pgget_wfile(dsid, "*", "wfile = '{}' AND type = '{}'".format(wfile, type), PgOPT.PGOPT['extlog'])
      if not pgrec:  # should not happen
         PgLOG.pglog(wfile + ": Web File Not in RDADB", PgOPT.PGOPT['extlog'])
      oarch = warch = 1
      if pgrec['locflag'] == 'O':
         warch = 0
      elif pgrec['locflag'] == 'G':
         oarch = 0
      ofile = (PgLOG.join_paths(dsid, wfile) if oarch else None)
      if warch:
         tardir = "{}/{}".format(PgLOG.PGLOG['DSDHOME'], dsid)
      else:
         tardir = "{}/{}/{}".format(PgLOG.PGLOG['DSDHOME'], PgLOG.PGLOG['BACKUPEP'], dsid)
      afile = "{}/{}".format(tardir, wfile)
      tmpfile = None if warch else afile
      ainfo = PgFile.check_local_file(afile, 0, PgOPT.PGOPT['extlog'])
      if ainfo:
         PgLOG.pglog(afile + ": File exists already", PgOPT.PGOPT['wrnlog'])
      else:
         tarfile = TARFILES[pgrec['bid']]
         ainfo = get_backup_member_file(pgrec, tarfile, tardir)
         tarcmd = "tar -xvf {} -C {} {}".format(tarfile, tardir, wfile)
         PgLOG.pgsystem(tarcmd, PgOPT.PGOPT['extlog'], 5)
         ainfo = PgFile.check_local_file(afile, 0, PgOPT.PGOPT['extlog'])
         if not ainfo:
            PgLOG.pglog("{}: Error untar File {}".format(tarfile, afile), PgOPT.PGOPT['wrnlog'])
         if warch: wcnt += 1
      if ainfo['data_size'] != pgrec['data_size']:
         PgLOG.pglog("{}: Different Restored/RDADB file sizes {}/{}".format(afile, ainfo['data_size'], pgrec['data_size']), PgOPT.PGOPT['wrnlog'])

      if oarch:
         oinfo = PgFile.check_object_file(ofile, bucket, 0, PgOPT.PGOPT['extlog'])
         if oinfo:
            PgLOG.pglog(ofile + ": Object file exists", PgOPT.PGOPT['wrnlog'])
         elif PgFile.local_copy_object(ofile, afile, bucket, None, PgOPT.PGOPT['extlog']):
            ocnt += 1

      if tmpfile: PgFile.delete_local_file(tmpfile, PgOPT.PGOPT['extlog'])

      if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']

   if wcnt > 0:
      PgLOG.pglog("{} of {} Web file{} restored for {}".format(wcnt, ALLCNT, s, PgOPT.params['DS']), PgOPT.PGOPT['emllog'])
   if ocnt > 0:
      PgLOG.pglog("{} of {} Object file{} restored for {}".format(ocnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# untar backup tarfile to get a single member file
#
def get_backup_member_file(pgrec, tarfile, tardir):

   mfile = wfile = pgrec['wfile']
   note = pgrec['note']
   while not re.search(r'{}<:>'.format(mfile), note):
      ms = re.search(r'>MV .* File (\S+) To .*{}'.format(mfile), note)
      if not ms: break
      mfile = ms.group(1)

   tarcmd = "tar -xvf {} -C {} {}".format(tarfile, tardir, mfile)
   PgLOG.pgsystem(tarcmd, PgOPT.PGOPT['extlog'], 5)
   afile = '{}/{}'.format(tardir, mfile)
   ainfo = PgFile.check_local_file(afile, 0, PgOPT.PGOPT['extlog'])

   if ainfo and mfile != wfile:
      nfile = '{}/{}'.format(tardir, wfile)
      PgFile.move_local_file(nfile, afile, PgOPT.PGOPT['extlog'])     
      ainfo = PgFile.check_local_file(nfile, 0, PgOPT.PGOPT['extlog'])

   return ainfo

#
# untar backup file to retrieve saved files
#
def restore_backup_savedfiles():

   tname = 'sfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   bucket = "rda-decsdata"    # default object store bucket
   s = 's' if ALLCNT > 1 else ''
   scnt = ocnt = bidx = chksize = 0
   dflags = {}
   PgLOG.pglog("Restore {} Saved file{} of {} from backup ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Saved file{} restored".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{} of {} saved file{} restored/processed".format(scnt, i, ALLCNT, s)
      sfile = PgOPT.params['SF'][i]
      type = PgOPT.params['ST'][i]
      pgrec = PgDBI.pgget(tname, "*", "sfile = '{}' AND type = '{}' AND {}".format(sfile, type, dcnd), PgOPT.PGOPT['extlog'])
      if not pgrec:  # should not happen
         PgLOG.pglog(sfile + ": Saved File Not in RDADB", PgOPT.PGOPT['extlog'])
      oarch = sarch = 1
      if pgrec['locflag'] == 'O':
         sarch = 0
      elif pgrec['locflag'] == 'G':
         oarch = 0
      ofile = (PgLOG.join_paths(dsid, sfile) if oarch else None)
      if sarch:
         tardir = "{}/{}".format(PgLOG.PGLOG['DECSHOME'], dsid)
      else:
         tardir = "{}/{}/{}".format(PgLOG.PGLOG['DECSHOME'], PgLOG.PGLOG['BACKUPEP'], dsid)
      afile = "{}/{}".format(tardir, sfile)
      tmpfile = None if sarch else afile
      ainfo = PgFile.check_local_file(afile, 0, PgOPT.PGOPT['extlog'])
      if ainfo:
         PgLOG.pglog(afile + ": File exists already", PgOPT.PGOPT['wrnlog'])
      else:
         tarfile = TARFILES[pgrec['bid']]
         ainfo = get_backup_member_file(pgrec, tarfile, tardir)
         tarcmd = "tar -xvf {} -C {} {}".format(tarfile, tardir, sfile)
         PgLOG.pgsystem(tarcmd, PgOPT.PGOPT['extlog'], 5)
         ainfo = PgFile.check_local_file(afile, 0, PgOPT.PGOPT['extlog'])
         if not ainfo:
            PgLOG.pglog("{}: Error untar File {}".format(tarfile, afile), PgOPT.PGOPT['wrnlog'])
         if sarch: scnt += 1
      if ainfo['data_size'] != pgrec['data_size']:
         PgLOG.pglog("{}: Different Restored/RDADB file sizes {}/{}".format(afile, ainfo['data_size'], pgrec['data_size']), PgOPT.PGOPT['wrnlog'])

      if oarch:
         oinfo = PgFile.check_object_file(ofile, bucket, 0, PgOPT.PGOPT['extlog'])
         if oinfo:
            PgLOG.pglog(ofile + ": Object file exists", PgOPT.PGOPT['wrnlog'])
         elif PgFile.local_copy_object(ofile, afile, bucket, None, PgOPT.PGOPT['extlog']):
            ocnt += 1

      if tmpfile: PgFile.delete_local_file(tmpfile, PgOPT.PGOPT['extlog'])

      if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']

   if scnt > 0:
      PgLOG.pglog("{} of {} Saved file{} restored for {}".format(scnt, ALLCNT, s, PgOPT.params['DS']), PgOPT.PGOPT['emllog'])
   if ocnt > 0:
      PgLOG.pglog("{} of {} Object file{} restored for {}".format(ocnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# cross copy quasar backup files between Globus endpoints rda-quasar and rda-quasar-drdata
#
def crosscopy_backup_files():

   global ALLCNT, MODCNT, RETSTAT
   tname = 'bfile'
   qtype = 'D'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0
   dflags = {}
   bpoint = 'rda-quasar'
   dpoint = 'rda-quasar-drdata'
   PgLOG.pglog("Cross Copy {} Quasar file{} of {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   PgOPT.validate_multiple_values(tname, ALLCNT)
   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} file{} processed for Quasar archive".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   reorder = errcnt = MODCNT = bgcnt = bcnt = dcnt = 0
   perrcnt = ALLCNT
   efiles = [1]*ALLCNT
   if PgSIG.PGSIG['BPROC'] > 1:
      qfiles = [None]*ALLCNT
      barchs = [None]*ALLCNT
   fnames = None
   while True:
      for i in range(bidx, ALLCNT):
         if PgSIG.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
            if PgLOG.PGLOG['DSCHECK']:
               PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
            if 'EM' in PgOPT.params:
               PgLOG.PGLOG['PRGMSG'] = "{}/{} of {} Quasar file{} archived/processed".format(bcnt, i, ALLCNT, s)
         bfile = PgOPT.params['QF'][i]
         if not (efiles[i] and bfile): continue
         efiles[i] = 0
         (bfile, qfile) = get_backup_filenames(bfile, dsid)
         binfo = "{}-{}".format(dsid, bfile)
         pgrec = PgDBI.pgget(tname, "*", "{} and bfile = '{}'".format(dcnd, bfile), PgOPT.PGOPT['extlog'])
         if not pgrec:
            PgLOG.pglog(binfo + ": Fail to Cross Copy for Quasar file not in RDADB", PgOPT.PGOPT['emlerr'])
            continue
         if pgrec and PgOPT.params['QF'][i] != bfile: PgOPT.params['QF'][i] = bfile
         barch = darch = 1
         PgLOG.pglog(binfo + ": Cross Copy Quasar file ...",  PgLOG.WARNLG)
         info = PgFile.check_backup_file(bfile, bpoint, 0, PgOPT.PGOPT['emerol'])
         if info:
            barch = 0
         elif info is not None:
            errcnt += 1
            efiles[i] = 1
            dflags['B'] = bpoint
            continue
         info = PgFile.check_backup_file(bfile, dpoint, 0, PgOPT.PGOPT['emerol'])
         if info:
            darch = 0
         elif info is not None:
            errcnt += 1
            efiles[i] = 1
            dflags['D'] = dpoint
            continue

         if barch and darch:
            PgLOG.pglog(binfo + ": Cannot Cross Copy, Neither Backup Nor Drdata file Exists", PgOPT.PGOPT['errlog'])
            continue
         elif not (barch or darch) and pgrec['type'] == 'D':
            PgLOG.pglog(binfo + ": No need Cross Copy, Both Backup & Drdata Exist", PgOPT.PGOPT['wrnlog'])
            continue

         if barch:
            if not PgFile.endpoint_copy_endpoint(qfile, qfile, bpoint, dpoint, PgOPT.PGOPT['emerol']|OVERRIDE):
               errcnt += 1
               efiles[i] = 1
               dflags['B'] = bpoint
               continue
            bcnt += 1
         elif darch:
            if not PgFile.endpoint_copy_endpoint(qfile, qfile, dpoint, bpoint, PgOPT.PGOPT['emerol']|OVERRIDE):
               errcnt += 1
               efiles[i] = 1
               dflags['D'] = dpoint
               continue
            dcnt += 1

         if PgSIG.PGSIG['BPROC'] > 1:
            qfiles[i] = qfile
            barchs[i] = barch
            bgcnt += 1

         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']

         if PgSIG.PGSIG['BPROC'] < 2:
            if not fnames: fnames = PgOPT.get_field_keys(tname)  # get setting fields if not yet
            bid = set_one_backfile(i, pgrec, bfile, fnames, qtype)
            if not bid:
               PgOPT.params['QF'][i] = None
               continue

      if errcnt == 0 or errcnt >= perrcnt or PgLOG.PGLOG['DSCHECK']: break
      perrcnt = errcnt
      PgLOG.pglog("Recopy {} Quasar file{} for {}".format(errcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
      errcnt = reset_errmsg(0)

   if errcnt:
      RETSTAT = reset_errmsg(errcnt)
      for i in range(bidx, ALLCNT):
         if efiles[i]: PgOPT.params['QF'][i] = ''
      if PgLOG.PGLOG['DSCHECK']:
         PgFile.check_storage_dflags(dflags, PgLOG.PGLOG['DSCHECK'], PgOPT.PGOPT['emerol'])
   if bgcnt:
      PgSIG.check_background(None, 0, PgLOG.LOGWRN, 1)
      for i in range(ALLCNT):
         if barchs[i]:
            validate_backarch(qfiles[i], "{}-{}".format(bpoint, qfiles[i]), i)
         elif qfiles[i]:
            validate_backarch(qfiles[i], "{}-{}".format(dpoint, qfiles[i]), i)
   if bcnt > 0: PgLOG.pglog("{} of {} Backup file{} Cross Copied for {}".format(bcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if dcnt > 0: PgLOG.pglog("{} of {} Drdata file{} Cross Copied for {}".format(dcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   PgLOG.pglog("{} of {} Quasar Backup file record{} modified for {}!".format(MODCNT, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if 'ON' in PgOPT.params: reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# validate a webfile archive, fatal is error
#
def validate_gladearch(file, ofile, i):

   info = PgFile.check_local_file(file, 6, PgOPT.PGOPT['errlog'])
   if not info:
      PgLOG.pglog("Error archiving {} to {}".format(ofile, file), PgLOG.LGEREX)
   elif PgOPT.OPTS['SZ'][2]&2 and PgOPT.params['SZ'] and info['data_size'] != PgOPT.params['SZ'][i]:
      PgLOG.pglog("Wrong Sizes: ({}){}/({}){}".format(file, info['data_size'], ofile, PgOPT.params['SZ'][i]), PgLOG.LGEREX)
   else:
      PgFile.set_local_mode(file, 1, PgLOG.PGLOG['FILEMODE'], info['mode'], info['logname'], PgOPT.PGOPT['errlog'])

#
# validate an object store file archive, fatal is error
#
def validate_objectarch(file, ofile, bucket, i):

   info = PgFile.check_object_file(file, bucket, 0, PgOPT.PGOPT['errlog'])
   if not info:
      PgLOG.pglog("Error archiving {} to {}-{}".format(ofile, bucket, file), PgLOG.LGEREX)
   elif PgOPT.OPTS['SZ'][2]&2 and PgOPT.params['SZ'] and info['data_size'] != PgOPT.params['SZ'][i]:
      PgLOG.pglog("Wrong Sizes: ({}-{}){}/({}){}".format(bucket, file, info['data_size'], ofile, PgOPT.params['SZ'][i]), PgLOG.LGEREX)

#
# validate an object store file archive, fatal is error
#
def validate_backarch(file, ofile, endpoint, i):

   info = PgFile.check_backup_file(file, endpoint, 0, PgOPT.PGOPT['errlog'])
   if not info:
      PgLOG.pglog("Error archiving {} to {}-{}".format(ofile, endpoint, file), PgLOG.LGEREX)
   elif PgOPT.OPTS['SZ'][2]&2 and PgOPT.params['SZ'] and info['data_size'] != PgOPT.params['SZ'][i]:
      PgLOG.pglog("Wrong Sizes: ({}-{}){}/({}){}".format(endpoint, file, info['data_size'], ofile, PgOPT.params['SZ'][i]), PgLOG.LGEREX)

#
# get dataset info
#
def get_dataset_info():

   tname = 'dataset'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get dataset info for {} ...".format(dsid), PgLOG.WARNLG)

   fnames = PgOPT.params['FN'] if 'FN' in PgOPT.params else ''
   kvalues = PgOPT.params['KV'] if 'KV' in PgOPT.params else []
   getkeys = 1 if (kvalues or re.match(r'^all$', fnames, re.I)) else 0
   if not getkeys or fnames:
      PgOPT.OUTPUT.write("{}{}{}\n".format(PgOPT.OPTS['DS'][1], PgOPT.params['ES'], dsid))
      if PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GA'][0]:
         PgOPT.OUTPUT.write("[{}]\n".format(tname.upper()))   # get all action
      fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['dsall'])
      pgrec = PgDBI.pgget(tname, "*", dcnd, PgLOG.LGEREX)
      PgOPT.print_row_format(pgrec, fnames, hash)

   view_keyvalues(dsid, kvalues, getkeys)
   if fnames and 'PE' in PgOPT.params: get_period_info()
   if 'WN' in PgOPT.params: PgArch.view_filenumber(dsid, 0)

   return 1   # get one dataset record

#
# get dataset period information
#
def get_period_info():
   
   tname = "dsperiod"
   dsid = PgOPT.params['DS']
   hash = PgOPT.TBLHASH[tname]
   fnames = PgOPT.PGOPT[tname]
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "G"
   condition = PgArch.get_condition(tname) + PgOPT.get_order_string(onames, tname) + ", dorder"
   pgrecs = PgDBI.pgmget(tname, "*", condition, PgLOG.LGEREX)
   if pgrecs:
      lens = PgUtil.all_column_widths(pgrecs, fnames, hash) if 'FO' in PgOPT.params else None
      PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      s = 's' if cnt  > 1 else ''
      PgLOG.pglog("{} period record{} retrieved for {}".format(cnt, s, dsid), PgLOG.LOGWRN)

#
# add a new or modify an existing datatset record into RDADB
#
def set_dataset_info(include = None):

   tname = 'dataset'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   PgLOG.pglog("Set {} info of {} ...".format(tname, dsid), PgLOG.WARNLG)

   mcnt = acnt = pcnt = kcnt = 0
   fnames = PgOPT.get_field_keys(tname, include)
   if fnames: # set dataset record
      pgrec = PgDBI.pgget(tname, PgOPT.get_string_fields(fnames, tname), dcnd, PgLOG.LGEREX)
      record = PgOPT.build_record(fnames, pgrec, tname, 0)
      if record:
         if 'backflag' in record and record['backflag'] == 'P': record['backflag'] = 'N'
         if 'locflag' in record and record['locflag'] in 'BR': record['locflag'] = 'G'
         if pgrec:
            record['date_change'] = PgUtil.curdate()
            if PgDBI.pgupdt(tname, record, dcnd, PgLOG.LGEREX):
               mcnt += 1
               if 'use_rdadb' in record and re.search(r'^[PYW]$', record['use_rdadb']):
                  PgOPT.params['WN'] = 6
         else:
            record['dsid'] = dsid
            record['date_change'] = record['date_create'] = PgUtil.curdate()
            if not record['use_rdadb']: record['use_rdadb'] = 'Y'
            acnt += PgDBI.pgadd(tname, record, PgLOG.LGEREX)

   if acnt == 0: pcnt = set_period_info(dcnd)   # set dsperiod record
   kvalues = PgOPT.params['KV'] if 'KV' in PgOPT.params else []
   kcnt = set_keyvalues(dsid, kvalues)

   if (pcnt + kcnt + mcnt + acnt) == 0:
      if not include: PgLOG.pglog("No change of dataset record for {}!".format(dsid), PgLOG.LOGWRN)   
   else:
      if acnt:
         PgLOG.pglog("Dataset record added for {}!".format(dsid),  PgLOG.LOGWRN)
      else:
         PgLOG.pglog("Dataset record modified for {}!".format(dsid),  PgLOG.LOGWRN)
      if pcnt + mcnt:
         PgDBI.reset_rdadb_version(dsid)

#
# add new or modify existing period record into RDADB
#
def set_period_info(dcnd):

   tname = 'dsperiod'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   dorder = 1
   allcnt = (len(PgOPT.params['ED']) if 'ED' in PgOPT.params else
             (len(PgOPT.params['BD']) if 'BD' in PgOPT.params else
              (len(PgOPT.params['BT']) if 'BT' in PgOPT.params else
               (len(PgOPT.params['ET']) if 'ET' in PgOPT.params else 0))))
   if not allcnt: return 0

   s = 's' if allcnt > 1 else ''
   PgLOG.pglog("Set {} period{} for {} ...".format(allcnt, s, dsid), PgLOG.WARNLG)
   fnames = PgOPT.get_field_keys(tname, None, "G")
   if not fnames: return 0
   PgOPT.validate_multiple_values(tname, allcnt, fnames)
   pcnt = 0
   for i in range(allcnt):
      gindex = PgOPT.params['GI'][i] if 'GI' in PgOPT.params else 0
      if gindex == 0:
         tmpcnd = "{} AND gindex = {} AND dorder = {}".format(dcnd, gindex, dorder)
         dorder += 1
      else:
         tmpcnd = "{} AND gindex = {}".format(dcnd, gindex)
      pgrec = PgDBI.pgget(tname, PgOPT.get_string_fields(fnames, tname), tmpcnd, PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("No Period for {}\nAdd it via metadata editor".format(tmpcnd), PgLOG.LOGWRN)
         continue
      record = PgOPT.build_record(fnames, pgrec, tname, i)
      if record:
         sdpcmd = "sdp -d {} -g {}".format(dsid[2:], gindex)
         if 'date_start' in record: sdpcmd += " -bd " + record['date_start']
         if 'date_end' in record: sdpcmd += " -ed " + record['date_end']
         if 'time_start' in record: sdpcmd += " -bt " + record['time_start']
         if 'time_end' in record: sdpcmd += " -et " + record['time_end']
         if PgLOG.pgsystem(sdpcmd): pcnt += 1

   if pcnt > 0: PgLOG.pglog("{} of {} period{} modified for {}!".format(pcnt, allcnt, s, dsid), PgLOG.LOGWRN)
   return pcnt

#
# delete group information for given dataset and index list
#
def delete_group_info():

   tname = 'dsgroup'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Delete {} group{} from {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   delcnt = webcnt = savedcnt = prdcnt = chdcnt = 0
   record = {}
   record['gindex'] = record['tindex'] = 0
   for gindex in PgOPT.params['GI']:
      gcnd = "gindex = {}".format(gindex)
      cnd = "{} AND {}".format(dcnd, gcnd)
      webcnt = PgSplit.pgget_wfile(dsid, '', gcnd, PgLOG.LGEREX)
      savedcnt = PgDBI.pgget("sfile", '', cnd, PgLOG.LGEREX)
      prdcnt = PgDBI.pgget("dsperiod", "", cnd, PgLOG.LGEREX)
      chdcnt = PgDBI.pgget(tname, "", "{} AND pindex = {}".format(dcnd, gindex), PgLOG.LGEREX)
      if (webcnt + savedcnt + prdcnt + chdcnt) > 0:
         ss = 's' if (webcnt + savedcnt + prdcnt + chdcnt) else ''
         PgLOG.pglog("Can not delete GroupIndex {}, due to".format(gindex), PgLOG.LOGWRN)
         PgLOG.pglog("{}/{}/{}/{} reocrd{} of ".format(webcnt, savedcnt, prdcnt, chdcnt, ss) +
                     "WebFile/SavedFile/GroupPeriod/SubGroup still in RDADB for the group", PgLOG.LOGWRN)
         continue
      delcnt += PgDBI.pgdel(tname, cnd, PgLOG.LGEREX)

   PgLOG.pglog("{} of {} group{} deleted from {}".format(delcnt, ALLCNT, s, dsid), PgLOG.LOGWRN)
   if delcnt > 0: PgDBI.reset_rdadb_version(dsid)

#
# get group information
#
def get_group_info():

   tname = "dsgroup"
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get group info of {} from RDADB ...".format(dsid), PgLOG.WARNLG)

   lens = oflds = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   if 'RG' in PgOPT.params and 'GI' in PgOPT.params: PgArch.get_subgroups("GG")
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['gpall'])
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "I"
   qnames = fnames
   if fnames.find('Y') > -1 and fnames.find('X') < 0: qnames += 'X'
   if 'WN' in PgOPT.params and fnames.find('I') < 0: qnames += 'I'
   qnames += PgOPT.append_order_fields(onames, qnames, tname, "Y")
   condition = PgArch.get_condition(tname)
   if 'ON' in PgOPT.params and ('OB' in PgOPT.params or re.search(r'Y', onames, re.I)):
      oflds = PgOPT.append_order_fields(onames, None, tname)
   else:
      condition += PgOPT.get_order_string(onames, tname)
   pgrecs = PgDBI.pgmget(tname, PgOPT.get_string_fields(qnames, tname, None, "Y"), condition, PgLOG.LGEREX)

   if PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GA'][0]:
      PgOPT.OUTPUT.write("[{}]\n".format(tname.upper()))   # get all action
   else:
      PgOPT.OUTPUT.write("{}{}{}\n".format(PgOPT.OPTS['DS'][1], PgOPT.params['ES'], dsid))

   if pgrecs:
      if fnames.find('Y') > -1: pgrecs['pname'] = PgArch.group_index_to_id(pgrecs['pindex'])
      if 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs, fnames, hash)
      if oflds: pgrecs = PgUtil.sorthash(pgrecs, oflds, hash, PgOPT.params['OB'])
   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{} group record{} retrieved".format(cnt, s), PgLOG.LOGWRN)
      if 'WN' in PgOPT.params: PgArch.view_filenumber(dsid, pgrecs['gindex'], cnt)
   else:
      PgLOG.pglog("No group found for " + condition, PgLOG.LOGWRN)

#
# add or modify group information
#
def set_group_info():

   if ALLCNT == 0 or PgOPT.params['GI'][0] == 0: return  # skip group index 0
   tname = 'dsgroup'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Set {} group{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   if 'PI' in PgOPT.params or 'PN' in PgOPT.params: PgOPT.validate_groups(1)
   fnames = PgOPT.get_field_keys(tname, None, 'Y')
   if not fnames: return PgLOG.pglog("Nothing to set for group!", PgLOG.LOGWRN)

   PgOPT.validate_multiple_values(tname, ALLCNT, fnames)
   fields = PgOPT.get_string_fields(fnames, tname) + ", level, gidx"
   tcnt = addcnt = modcnt = 0
   for i in range(ALLCNT):
      gindex = PgOPT.params['GI'][i]
      if gindex == 0: continue # skip group index 0
      pgrec = PgDBI.pgget(tname, fields, "{} AND gindex = {}".format(dcnd, gindex), PgLOG.LGEREX)
      record = PgOPT.build_record(fnames, pgrec, tname, i)
      if record:
         level = 1
         pindex = record['pindex'] if 'pindex' in record and record['pindex'] else 0
         if pindex:
            if abs(pindex) >= abs(gindex):
               PgLOG.pglog("{}-{}: Parent Group Index {} must be smaller than current Index!".format(dsid, gindex, pindex), PgLOG.LGEREX)
            prec = PgDBI.pgget(tname, 'grptype', "{} AND gindex = {}".format(dcnd, pindex), PgLOG.LGEREX)
            if not prec:
               PgLOG.pglog("{}-{}: Parent Group Index {} not on file!".format(dsid, gindex, pindex), PgLOG.LGEREX)
            if prec['grptype'] == 'I':
               if 'grptype' not in record:
                  if not pgrec: record['grptype'] = 'I'
               elif record['grptype'] == 'P':
                  PgLOG.pglog("{}-{}: cannot set Public for Parent Group {} is Internal".format(dsid, gindex, pindex), PgLOG.LGEREX)
            level = PgMeta.get_group_levels(dsid, pindex, level + 1)
            if pgrec:
               PgOPT.params['WN'] = 6
               CHGGRPS[record['pindex']] = 1
               CHGGRPS[pgrec['pindex']] = 1
         if not pgrec or level != pgrec['level']: record['level'] = level

         if pgrec:
            modcnt += PgDBI.pgupdt(tname, record, "gidx = {}".format(pgrec['gidx']), PgLOG.LGEREX)
            if pindex: tcnt += PgMeta.reset_top_group_index(dsid, gindex, 6)
            if 'grptype' in record:
               PgOPT.params['WN'] = 6
               CHGGRPS[gindex] = 1
               if record['grptype'] == 'I': set_for_internal_group(gindex)
         else:
            record['dsid'] = dsid
            record['gindex'] = gindex
            addcnt += PgDBI.pgadd(tname, record, PgLOG.LGEREX)

   PgLOG.pglog("{}/{} of {} group{} added/modified for {}!".format(addcnt, modcnt, ALLCNT, s, dsid), PgLOG.LOGWRN)
   if (addcnt + modcnt + tcnt) > 0: PgDBI.reset_rdadb_version(dsid)

#
# set subgroups and files to Internal for given group index
#
def set_for_internal_group(gindex):

   tname = 'dsgroup'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   grp = "{}-{}".format(dsid, gindex)
   gcnd = "gindex = {} AND status = 'P'".format(gindex)
   cnd = "{} AND {}".format(dcnd, gcnd)

   pgrecs = PgSplit.pgget_wfile(dsid, 'wfile, type', gcnd, PgOPT.PGOPT['extlog'])
   cnt = len(pgrecs['wfile']) if pgrecs else 0
   srec = {'status' : 'I'}
   if cnt:
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{}: set {} web file{} to Internal".format(grp, cnt, s), PgOPT.PGOPT['wrnlog'])
      if PgSplit.pgupdt_wfile(dsid, srec, gcnd, PgOPT.PGOPT['extlog']):
         for i in range(cnt):
            PgArch.change_wfile_mode(dsid, pgrecs['wfile'][i], pgrecs['type'][i], 'P', 'I')

   cnt = PgDBI.pgget('sfile', '', cnd, PgOPT.PGOPT['extlog'])
   if cnt:
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{}: set {} saved file{} to Internal".format(grp, cnt, s), PgOPT.PGOPT['wrnlog'])
      PgDBI.pgupdt("sfile", srec, cnd, PgOPT.PGOPT['extlog'])

   cnd = "{} AND pindex = {} AND grptype = 'P'".format(dcnd, gindex)
   pgrecs = PgDBI.pgmget(tname, 'gindex', cnd, PgOPT.PGOPT['extlog'])
   cnt = (len(pgrecs['gindex']) if pgrecs else 0)
   if cnt:
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{}: set {} subgroup{} to Internal".format(grp, cnt, s), PgOPT.PGOPT['wrnlog'])
      PgDBI.pgexec("UPDATE dsgroup SET grptype = 'I' WHERE " + cnd, PgOPT.PGOPT['extlog'])
      for i in range(cnt):
         set_for_internal_group(pgrecs['gindex'][i])

#
# get WEB file information
#
def get_webfile_info():

   tname = "wfile"
   dsid = PgOPT.params['DS']
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get Web file info of {} from RDADB ...".format(dsid), PgLOG.WARNLG)

   lens = oflds = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   if 'RG' in PgOPT.params and 'GI' in PgOPT.params: PgArch.get_subgroups("GW")
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['wfall'])
   if 'QF' in PgOPT.params and fnames.find('B') < 0: fnames += 'B'
   if 'QT' in PgOPT.params:
      if fnames.find('B') < 0: fnames += 'B'
      if fnames.find('Q') < 0: fnames += 'Q'
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "ITO"
   qnames = fnames
   if 'TT' in PgOPT.params and fnames.find('S') < 0: qnames , 'S'
   if 'RN' in PgOPT.params:
      if fnames.find('I') < 0: qnames += 'I'
      if fnames.find('T') < 0: qnames += 'T'
   qnames += PgOPT.append_order_fields(onames, qnames, tname)
   condition = PgArch.get_condition(tname)
   if 'ON' in PgOPT.params and ('OB' in PgOPT.params or re.search(r'(B|P)', onames, re.I)):
      oflds = PgOPT.append_order_fields(onames, None, tname)
   else:
      condition += PgOPT.get_order_string(onames, tname)

   if PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GA'][0]:
      PgOPT.OUTPUT.write("[WEBFILE]\n")   # get all action
   else:
      PgOPT.OUTPUT.write("{}{}{}\n".format(PgOPT.OPTS['DS'][1], PgOPT.params['ES'], dsid))

   fields = PgOPT.get_string_fields(qnames, tname)
   if qnames.find('Q') > -1:
      tjoin = "LEFT JOIN bfile ON wfile.bid = bfile.bid"
      pgrecs = PgSplit.pgmget_wfile_join(dsid, tjoin, fields, condition, PgLOG.LGEREX)
   else:
      pgrecs = PgSplit.pgmget_wfile(dsid, fields, condition, PgLOG.LGEREX)

   if pgrecs:
      if 'bid' in pgrecs: pgrecs['bid'] = PgArch.get_quasar_backfiles(pgrecs['bid'])
      if 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs, fnames, hash)
      if oflds: pgrecs = PgUtil.sorthash(pgrecs, oflds, hash, PgOPT.params['OB'])

   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      if 'RN' in PgOPT.params:
         if tname in pgrecs:
            pgrecs[tname] = PgArch.get_relative_names(pgrecs[tname], pgrecs['gindex'], pgrecs['type'])

      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      if 'TT' in PgOPT.params: PgArch.print_statistics(pgrecs['data_size'])
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{} Web file record{} retrieved".format(cnt, s), PgLOG.LOGWRN)
   else:
      PgLOG.pglog("No Web file found for " + condition, PgLOG.LOGWRN)

#
# get Help file information
#
def get_helpfile_info():

   tjoin = tname = "hfile"
   dsid = PgOPT.params['DS']
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get Help file info of {} from RDADB ...".format(dsid), PgLOG.WARNLG)

   lens = oflds = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['hfall'])
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "TO"
   qnames = fnames
   if 'TT' in PgOPT.params and fnames.find('S') < 0: qnames , 'S'
   qnames += PgOPT.append_order_fields(onames, qnames, tname)
   condition = PgArch.get_condition(tname)
   if 'ON' in PgOPT.params and 'OB' in PgOPT.params:
      oflds = PgOPT.append_order_fields(onames, None, tname)
   else:
      condition += PgOPT.get_order_string(onames, tname)

   if PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GA'][0]:
      PgOPT.OUTPUT.write("[HELPFILE]\n")   # get all action
   else:
      PgOPT.OUTPUT.write("{}{}{}\n".format(PgOPT.OPTS['DS'][1], PgOPT.params['ES'], dsid))

   pgrecs = PgDBI.pgmget(tjoin, PgOPT.get_string_fields(qnames, tname), condition, PgLOG.LGEREX)
   if pgrecs:
      if 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs, fnames, hash)
      if oflds: pgrecs = PgUtil.sorthash(pgrecs, oflds, hash, PgOPT.params['OB'])

   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      if 'TT' in PgOPT.params: PgArch.print_statistics(pgrecs['data_size'])
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{} Help file record{} retrieved".format(cnt, s), PgLOG.LOGWRN)
   else:
      PgLOG.pglog("No Help file found for " + condition, PgLOG.LOGWRN)

#
# get Saved file information
#
def get_savedfile_info():

   tjoin = tname = "sfile"
   dojoin = 0
   dsid = PgOPT.params['DS']
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get Saved file info of {} from RDADB ...".format(dsid), PgLOG.WARNLG)

   lens = oflds = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   if 'RG' in PgOPT.params and 'GI' in PgOPT.params: PgArch.get_subgroups("GS")
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['sfall'])
   if 'QF' in PgOPT.params or 'QT' in PgOPT.params:
      if fnames.find('B') < 0: fnames += 'B'
      if fnames.find('Q') < 0: fnames += 'Q'
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "ITO"
   qnames = fnames
   if 'TT' in PgOPT.params and fnames.find('S') < 0: qnames += 'S'
   if 'RN' in PgOPT.params:
      if fnames.find('I') < 0: qnames += 'I'
      if fnames.find('T') < 0: qnames += 'T'
   qnames += PgOPT.append_order_fields(onames, qnames, tname)
   if qnames.find('Q') > -1: dojoin = 1
   condition = PgArch.get_condition(tname, None, None, dojoin)
   if 'ON' in PgOPT.params and ('OB' in PgOPT.params or re.search(r'(B|P)', onames, re.I)):
      oflds = PgOPT.append_order_fields(onames, None, tname)
   else:
      condition += PgOPT.get_order_string(onames, tname)

   if PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GA'][0]:
      PgOPT.OUTPUT.write("[SAVEDFILE]\n")   # get all action
   else:
      PgOPT.OUTPUT.write("{}{}{}\n".format(PgOPT.OPTS['DS'][1], PgOPT.params['ES'], dsid))

   if dojoin: tjoin += " LEFT JOIN bfile ON sfile.bid = bfile.bid"
   pgrecs = PgDBI.pgmget(tjoin, PgOPT.get_string_fields(qnames, tname), condition, PgLOG.LGEREX)
   if pgrecs:
      if 'bid' in pgrecs: pgrecs['bid'] = PgArch.get_quasar_backfiles(pgrecs['bid'])
      if 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs, fnames, hash)
      if oflds: pgrecs = PgUtil.sorthash(pgrecs, oflds, hash, PgOPT.params['OB'])
   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      if 'RN' in PgOPT.params:
         if tname in pgrecs:
            pgrecs[tname] = PgArch.get_relative_names(pgrecs[tname], pgrecs['gindex'], pgrecs['type'], 1)
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      if 'TT' in PgOPT.params: PgArch.print_statistics(pgrecs['data_size'])
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{} Saved file record{} retrieved".format(cnt, s), PgLOG.LOGWRN)
   else:
      PgLOG.pglog("No Saved file found for " + condition, PgLOG.LOGWRN)

#
# get Quasar Backup file information
#
def get_backfile_info():

   tname = "bfile"
   dsid = PgOPT.params['DS']
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get Quasar Backup file info of {} from RDADB ...".format(dsid), PgLOG.WARNLG)

   lens = oflds = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['bfall'])
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "O"
   qnames = fnames
   if 'TT' in PgOPT.params and fnames.find('S') < 0: qnames += 'S'
   qnames += PgOPT.append_order_fields(onames, qnames, tname)
   condition = PgArch.get_condition(tname)
   if 'ON' in PgOPT.params and 'OB' in PgOPT.params:
      oflds = PgOPT.append_order_fields(onames, None, tname)
   else:
      condition += PgOPT.get_order_string(onames, tname)

   if PgOPT.PGOPT['ACTS'] == PgOPT.OPTS['GA'][0]:
      PgOPT.OUTPUT.write("[BACKFILE]\n")   # get all action
   else:
      PgOPT.OUTPUT.write("{}{}{}\n".format(PgOPT.OPTS['DS'][1], PgOPT.params['ES'], dsid))

   pgrecs = PgDBI.pgmget(tname, PgOPT.get_string_fields(qnames, tname), condition, PgLOG.LGEREX)
   if pgrecs:
      if 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs, fnames, hash)
      if oflds: pgrecs = PgUtil.sorthash(pgrecs, oflds, hash, PgOPT.params['OB'])
   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      if 'TT' in PgOPT.params: PgArch.print_statistics(pgrecs['data_size'])
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{} Quasar Backup file record{} retrieved".format(cnt, s), PgLOG.LOGWRN)
   else:
      PgLOG.pglog("No Quasar Backup file found for " + condition, PgLOG.LOGWRN)

#
# add or modify WEB file information
#
def set_webfile_info(include = None):

   global MODCNT, ADDCNT
   bidx = 0
   dftloc = None
   tname = 'wfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Setting {} Web file record{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   if 'MC' not in PgOPT.params: # create place hold for MD5 cehcksum
      PgOPT.params['MC'] = [None]*ALLCNT
      if 'SC' not in PgOPT.params: PgOPT.OPTS['MC'][2] |= 2
   if 'LC' not in PgOPT.params:
      PgOPT.params['LC'] = [None]*ALLCNT
      PgOPT.OPTS['LC'][2] |= 2

   if 'QF' in PgOPT.params: PgOPT.params['QF'] = PgArch.get_bid_numbers(PgOPT.params['QF'])

   fnames = PgOPT.get_field_keys(tname, include, "Q")
   if not fnames:
      if not include: PgLOG.pglog("Nothing to set for Web file!", PgOPT.PGOPT['emlerr'])
      return

   PgOPT.validate_multiple_values(tname, ALLCNT, fnames)
   PgOPT.validate_multiple_options(ALLCNT, ["WP"])
   if 'RO' in PgOPT.params and 'DO' not in PgOPT.params:
      if 'O' not in fnames: fnames += 'O'
      PgOPT.params['DO'] = [0]*ALLCNT

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} web file record{} processed for set".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)

   reorder = metatotal = metacnt = ADDCNT = MODCNT = 0
   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.add_dscheck_dcount(20, 0, PgOPT.PGOPT['extlog'])
         if metacnt >= PgOPT.PGOPT['RSMAX']:
            metatotal += PgMeta.process_metadata("W", metacnt, PgOPT.PGOPT['emerol'])
            metacnt = 0
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{} of {} {} Web file record{} added/modified/processed".format(ADDCNT, MODCNT, i, ALLCNT, dsid, s)
      if not PgOPT.params['WF'][i]: continue
      if 'WT' in PgOPT.params and PgOPT.params['WT'][i]:
         type = PgOPT.params['WT'][i]
         deftype = 0
      else:
         type = 'D'
         deftype = 1

      # validate web file
      file = PgArch.get_web_path(i, PgOPT.params['WF'][i], 0, type)
      pgrec = PgSplit.pgget_wfile(dsid, "*", "wfile = '{}'".format(file), PgLOG.LGEREX)
      olocflag = pgrec['locflag'] if pgrec else ''
      locflag = PgOPT.params['LC'][i]
      if not locflag:
         if not dftloc: dftloc = PgArch.get_dataset_locflag(dsid)
         locflag = PgOPT.params['LC'][i] = olocflag if olocflag else dftloc
      if olocflag and locflag != olocflag:
         PgLOG.pglog("{}-{}: Cannot reset Web file Location Flag {} to {}".format(dsid, file, olocflag, locflag), PgOPT.PGOPT['errlog'])
         continue
      if not PgOPT.params['MC'][i] and ('SC' in PgOPT.params or not (pgrec and pgrec['checksum'])):
         if locflag != 'O':
            ofile = PgArch.get_web_path(i, PgOPT.params['WF'][i], 1, type)
            PgOPT.params['MC'][i] = PgFile.get_md5sum(ofile)
         elif 'SC' in PgOPT.params:
            PgLOG.pglog("{}-{}: Cannot set MD5 checksum for web file on Object Store only".format(dsid, file), PgOPT.PGOPT['errlog'])
            continue
      if pgrec and PgOPT.params['WF'][i] != file: PgOPT.params['WF'][i] = file
      if pgrec and pgrec['type'] and deftype and type != pgrec['type']: type = pgrec['type']
      info = None
      if locflag == 'O' and not pgrec:
         info = PgFile.check_object_file(PgLOG.join_paths(dsid, file), 'rda-data', 1, PgOPT.PGOPT['emlerr'])
      wid = set_one_webfile(i, pgrec, file, fnames, type, info)
      if not wid: continue
      wfile = PgArch.get_web_path(i, PgOPT.params['WF'][i], 0, type)
      if 'GX' in PgOPT.params and PgOPT.PGOPT['GXTYP'].find(type) > -1:
         fmt = PgOPT.params['DF'][i] if 'DF' in PgOPT.params else (pgrec['data_format'] if pgrec else None)
         metacnt += PgMeta.record_meta_gather('W', dsid, wfile, fmt)
         PgMeta.cache_meta_tindex(dsid, wid, 'W')
      if pgrec and pgrec['meta_link'] and pgrec['meta_link'] != 'N':
         if 'DX' in PgOPT.params or PgOPT.PGOPT['GXTYP'].find(type) < 0 and PgOPT.PGOPT['GXTYP'].find(pgrec['type']) > -1:
            metacnt += PgMeta.record_meta_delete('W', dsid, wfile)
         elif 'GI' in PgOPT.params:
            gindex = PgOPT.params['GI'][i]
            if gindex != pgrec['gindex'] and (gindex or (PgOPT.OPTS['GI'][2]&2) == 0):
               metacnt += PgMeta.record_meta_summary('W', dsid, gindex, pgrec['gindex'])

   PgLOG.pglog("{}/{} of {} Web file record{} added/modified for {}!".format(ADDCNT, MODCNT, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if metacnt > 0:
      metatotal += PgMeta.process_metadata('W', metacnt, PgOPT.PGOPT['emerol'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, 0, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (metatotal + ADDCNT + MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ''

#
# add or modify one Web file record
#
def set_one_webfile(i, pgrec, file, flds, type, info = None, ndsid = None, sact = 0):

   global ADDCNT, MODCNT
   tname = 'wfile'
   gindex = (PgOPT.params['GI'][i] if 'GI' in PgOPT.params and PgOPT.OPTS['GI'][2]&2 == 0 else (pgrec['gindex'] if pgrec else 0))
   dsid = ndsid if ndsid else PgOPT.params['DS']
   wid = pgrec['wid'] if pgrec else 0
   if not type: type = 'D'
   if 'RO' in PgOPT.params and 'DO' in PgOPT.params:
      PgOPT.params['DO'][i] = PgArch.get_next_disp_order(dsid, gindex)
   record = PgOPT.build_record(flds, pgrec, tname, i)
   if pgrec and (pgrec['status'] == 'D' or PgOPT.PGOPT['ACTS']&PgOPT.OPTS['AW'][0]):
      record['uid'] = PgOPT.PGOPT['UID']
      if not info:
         info = PgFile.check_local_file(PgArch.get_web_path(i, file, 1, type),
                                        1, PgOPT.PGOPT['emerol'])
      if info:
         record['data_size'] = info['data_size']
         record['date_modified'] = info['date_modified']
         record['time_modified'] = info['time_modified']
         record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
         record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
      else:
         record['date_modified'] = PgUtil.curdate()
   if record:
      ccnt = fcnt = 0
      mlink = 0
      if 'meta_link' in record:
         del record['meta_link']
         mlink = 1
      if 'locflag' in record and record['locflag'] == 'R': record['locflag'] = 'G'
      if not ('type' in record or pgrec and pgrec['type'] == type): record['type'] = type
      if not ('vindex' in record or (pgrec and pgrec['vindex'])):
         if dsid not in VINDEX: VINDEX[dsid] = PgOPT.get_version_index(dsid, PgOPT.PGOPT['extlog'])
         if VINDEX[dsid]: record['vindex'] = VINDEX[dsid]
      if 'status' not in record or record['status'] == 'P':
         if gindex and PgArch.get_group_type(dsid, gindex) == 'I':
            if 'status' in record:
               PgLOG.pglog("{}-{}: Keep file status Internal for Internal group {}".format(dsid, file, gindex), PgOPT.PGOPT['wrnlog'])
            record['status'] = 'I'
      if 'checksum' in record: PgLOG.pglog("md5({}-{})={}".format(dsid, file, record['checksum']), PgLOG.LOGWRN)
      if 'gindex' in record and record['gindex']: record['tindex'] = PgMeta.get_top_gindex(dsid, record['gindex'])
      if pgrec:
         if pgrec['wfile'] != file: record['wfile'] = file
         if 'bid' in record and record['bid'] and pgrec['bid'] and record['bid'] != pgrec['bid']:
            return PgLOG.pglog("{}: Cannot change link to backup ID ({}/{})".format(file, record['bid'], pgrec['bid']), PgOPT.PGOPT['emlerr'])
         if 'data_format' in record and not record['data_format']:
            del record['data_format']
            if not record: return 0
         if pgrec['status'] == 'D':
            if not ('status' in record and record['status']): record['status'] = 'P'
         ccnt = PgMeta.record_webfile_changes(dsid, gindex, record, pgrec)
         if PgSplit.pgupdt_wfile_dsid(dsid, pgrec['dsid'], record, pgrec['wid'], PgLOG.LGEREX):
            MODCNT += 1
            fcnt = 1
            if 'status' in record: PgArch.change_wfile_mode(dsid, file, type, pgrec['status'], record['status'])
         if mlink or pgrec['meta_link'] and pgrec['meta_link'] == 'Y': PgMeta.set_meta_link(dsid, file)
      else:
         if record['wfile'] != file: record['wfile'] = file
         record['uid'] = PgOPT.PGOPT['UID']
         if 'status' not in record: record['status'] = 'P'
         if not info:
            info = PgFile.check_local_file(PgArch.get_web_path(i, file, 1, type), 
                                           1, PgOPT.PGOPT['emerol'])
         if info:
            if not info['isfile']: return PgLOG.pglog("file: is a directory", PgOPT.PGOPT['emlerr'])
            record['data_size'] = info['data_size']
            record['date_modified'] = info['date_modified']
            record['time_modified'] = info['time_modified']
            record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
            record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
         else:
            return PgLOG.pglog("{}-{}: {}".format(type, file, PgLOG.PGLOG['MISSFILE']), PgOPT.PGOPT['emlerr'])
         if 'disp_order' not in record:
            record['disp_order'] = PgArch.get_next_disp_order(dsid, gindex, tname, type)
         ccnt = PgMeta.record_webfile_changes(dsid, gindex, record)
         wid = PgSplit.pgadd_wfile(dsid, record, PgLOG.LGEREX|PgLOG.AUTOID)
         if wid:
            ADDCNT += 1
            fcnt = 1
      if ccnt:
         if not sact: sact = 4
         PgMeta.save_filenumber(None, sact, 1, fcnt)

   return wid

#
# add or modify Help file information
#
def set_helpfile_info():

   global MODCNT, ADDCNT
   bidx = 0
   tname = 'hfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Setting {} Help file record{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   if 'MC' not in PgOPT.params: # create place hold for MD5 cehcksum
      PgOPT.params['MC'] = [None]*ALLCNT
      if 'SC' not in PgOPT.params: PgOPT.OPTS['MC'][2] |= 2

   fnames = PgOPT.get_field_keys(tname)
   if not fnames: return PgLOG.pglog("Nothing to set for Help file!", PgOPT.PGOPT['emlerr'])

   PgOPT.validate_multiple_values(tname, ALLCNT, fnames)
   if 'RO' in PgOPT.params and 'DO' not in PgOPT.params:
      if 'O' not in fnames: fnames += 'O'
      PgOPT.params['DO'] = [0]*ALLCNT

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} help file record{} processed for set".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)

   reorder = ADDCNT = MODCNT = 0
   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.add_dscheck_dcount(20, 0, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{} of {} {} Help file record{} added/modified/processed".format(ADDCNT, MODCNT, i, ALLCNT, dsid, s)
      hfile = PgOPT.params['HF'][i]
      if not hfile: continue
      type = PgOPT.params['HT'][i]
      if PgOPT.PGOPT['HFTYP'].find(type) < 0:
         PgLOG.pglog("{}-{}: Unknown Help File Type {} to set file information".format(dsid, hfile, type), PgOPT.PGOPT['errlog'])
         continue

      # validate help file
      pgrec = PgDBI.pgget(tname, "*", "hfile = '{}' AND type = '{}' AND {}".format(hfile, type, dcnd), PgLOG.LGEREX)
      olocflag = pgrec['locflag'] if pgrec else ''
      locflag = PgOPT.params['LC'][i] if 'LC' in PgOPT.params else olocflag
      if olocflag and locflag != olocflag:
         PgLOG.pglog("{}-{}: Cannot reset Help file Location Flag {} to {}".format(dsid, hfile, olocflag, locflag), PgOPT.PGOPT['errlog'])
         continue
      getmc = 0 if 'WU' in PgOPT.params or (pgrec and pgrec['url']) else 1
      if getmc and not PgOPT.params['MC'][i] and ('SC' in PgOPT.params or not (pgrec and pgrec['checksum'])):
         ofile = PgArch.get_help_path(i, hfile, 1, type)
         PgOPT.params['MC'][i] = PgFile.get_md5sum(ofile)
      set_one_helpfile(i, pgrec, hfile, fnames, type)

   PgLOG.pglog("{}/{} of {} Help file record{} added/modified for {}!".format(ADDCNT, MODCNT, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']: PgCMD.set_dscheck_dcount(ALLCNT, 0, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params: reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (ADDCNT + MODCNT + reorder) > 0: PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ''

#
# add or modify one help file record
#
def set_one_helpfile(i, pgrec, file, flds, type, info = None, ndsid = None):

   global ADDCNT, MODCNT
   tname = 'hfile'
   dsid = ndsid if ndsid else PgOPT.params['DS']
   hid = pgrec['hid'] if pgrec else 0
   if 'RO' in PgOPT.params and 'DO' in PgOPT.params:
      PgOPT.params['DO'][i] = PgArch.get_next_disp_order(dsid)
   record = PgOPT.build_record(flds, pgrec, tname, i)
   if pgrec and (pgrec['status'] == 'D' or PgOPT.PGOPT['ACTS']&PgOPT.OPTS['AH'][0]):
      record['uid'] = PgOPT.PGOPT['UID']
      if not info:
         info = PgFile.check_local_file(PgArch.get_help_path(i, file, 1, type), 1, PgOPT.PGOPT['emerol'])
      if info:
         record['data_size'] = info['data_size']
         record['date_modified'] = info['date_modified']
         record['time_modified'] = info['time_modified']
         record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
         record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
      else:
         record['date_modified'] = PgUtil.curdate()
   if ndsid: record['dsid'] = ndsid
   if record:
      ccnt = fcnt = 0
      mlink = 0
      if not ('type' in record or pgrec and pgrec['type'] == type): record['type'] = type
      if 'checksum' in record: PgLOG.pglog("md5({}-{})={}".format(dsid, file, record['checksum']), PgLOG.LOGWRN)
      if 'url' in record:
         record['locflag'] = 'R'
         record['date_modified'] = PgUtil.curdate()
      if pgrec:
         if pgrec['hfile'] != file: record['hfile'] = file
         if not ndsid:
            if not pgrec['dsid'] or pgrec['dsid'] == PgLOG.PGLOG['DEFDSID']:
               record['dsid'] = dsid
            elif dsid != pgrec['dsid']:
               return PgLOG.pglog("{}-{}: in {}, Move to {} first".format(type, file, pgrec['dsid'], dsid), PgOPT.PGOPT['emlerr'])
         if 'data_format' in record and not record['data_format']:
            del record['data_format']
            if not record: return 0
         if PgDBI.pgupdt(tname, record, "hid = {}".format(pgrec['hid']), PgLOG.LGEREX):
            MODCNT += 1
            fcnt = 1
      else:
         if record['hfile'] != file: record['hfile'] = file
         record['dsid'] = dsid
         record['uid'] = PgOPT.PGOPT['UID']
         if 'status' not in record: record['status'] = 'P'
         if not (info or 'url' in record):
            info = PgFile.check_local_file(PgArch.get_help_path(i, file, 1, type), 1, PgOPT.PGOPT['emerol'])
            if info:
               if not info['isfile']: return PgLOG.pglog("file: is a directory", PgOPT.PGOPT['emlerr'])
               record['data_size'] = info['data_size']
               record['date_modified'] = info['date_modified']
               record['time_modified'] = info['time_modified']
               record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
               record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
            else:
               return PgLOG.pglog("{}-{}: {}".format(type, file, PgLOG.PGLOG['MISSFILE']), PgOPT.PGOPT['emlerr'])
         if 'disp_order' not in record:
            record['disp_order'] = PgArch.get_next_disp_order(dsid, 0, tname, type)
         hid = PgDBI.pgadd(tname, record, PgLOG.LGEREX|PgLOG.AUTOID)
         if hid:
            ADDCNT += 1
            fcnt = 1

   return hid

#
# add or modify save file information
#
def set_savedfile_info():

   global ADDCNT, MODCNT
   bidx = 0
   dftloc = None
   tname = 'sfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Setting {} Saved file record{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   if 'MC' not in PgOPT.params: # create place hold for MD5 cehcksum
      PgOPT.params['MC'] = [None]*ALLCNT
      if 'SC' not in PgOPT.params: PgOPT.OPTS['MC'][2] |= 2
   if 'LC' not in PgOPT.params:
      PgOPT.params['LC'] = [None]*ALLCNT
      PgOPT.OPTS['LC'][2] |= 2

   if 'QF' in PgOPT.params: PgOPT.params['QF'] = PgArch.get_bid_numbers(PgOPT.params['QF'])

   fnames = PgOPT.get_field_keys(tname, None, "Q")
   if not fnames: return PgLOG.pglog("Nothing to set for Saved file!", PgOPT.PGOPT['emlerr'])

   PgOPT.validate_multiple_values(tname, ALLCNT, fnames)
   PgOPT.validate_multiple_options(ALLCNT, ["SP"])
   if 'RO' in PgOPT.params and 'DO' not in PgOPT.params:
      if 'O' not in fnames: fnames += 'O'
      PgOPT.params['DO'] = [0]*ALLCNT

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Saved file record{} processed for set".format(bidx, ALLCNT), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 8)

   reorder = ADDCNT = MODCNT = 0
   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.add_dscheck_dcount(20, 0, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{} of {} {} Saved file record{} added/modified/processed".format(ADDCNT, MODCNT, i, ALLCNT, dsid, s)
      file = PgOPT.params['SF'][i]
      if not file: continue
      if 'ST' in PgOPT.params and PgOPT.params['ST'][i]:
         type = PgOPT.params['ST'][i]
         if PgOPT.PGOPT['SDTYP'].find(type) < 0:
            PgLOG.pglog("{}-{}: Invalid Saved file Type '{}' to set".format(dsid, file, type), PgOPT.PGOPT['emerol'])
            continue
      else:
         PgLOG.pglog("{}-{}: Miss Saved file Type to Set".format(dsid, file), PgOPT.PGOPT['errlog'])
         continue
      typstr = "type = '{}'".format(type)

      # validate saved file
      file = PgArch.get_saved_path(i, file, 0, type)
      pgrec = PgDBI.pgget(tname, "*", "{} AND sfile = '{}' AND {}".format(dcnd, file, typstr), PgLOG.LGEREX)
      if not pgrec:
         pgrec = PgDBI.pgget("sfile", "type", "{} AND sfile = '{}'".format(dcnd, file), PgOPT.PGOPT['extlog'])
         if pgrec:
            PgLOG.pglog("{}-{}: Fail to set, Move Action -MV to change file Type from '{}' to '{}'".format(dsid, file, pgrec['type'], type), PgOPT.PGOPT['emlerr'])
            continue
      olocflag = pgrec['locflag'] if pgrec else ''
      locflag = PgOPT.params['LC'][i]
      if not locflag:
         if not dftloc: dftloc = PgArch.get_dataset_locflag(dsid)
         locflag = PgOPT.params['LC'][i] = dftloc
      if olocflag and locflag != olocflag:
         PgLOG.pglog("{}-{}: Cannot reset Saved file Location Flag '{}' to '{}'".format(dsid, file, olocflag, locflag), PgOPT.PGOPT['errlog'])
         continue
      if not PgOPT.params['MC'][i] and ('SC' in PgOPT.params or not (pgrec and pgrec['checksum'])):
         if locflag != 'O':
            ofile = PgArch.get_saved_path(i, PgOPT.params['SF'][i], 1, type)
            PgOPT.params['MC'][i] = PgFile.get_md5sum(ofile)
         elif 'SC' in PgOPT.params:
            PgLOG.pglog("{}-{}: Cannot set MD5 checksum for saved file on Object Store only".format(dsid, file), PgOPT.PGOPT['errlog'])
            continue
      if pgrec and PgOPT.params['SF'][i] != file: PgOPT.params['SF'][i] = file
      info = None
      if locflag == 'O' and not pgrec:
         info = PgFile.check_object_file(PgLOG.join_paths(dsid, file), 'rda-decsdata', 1, PgOPT.PGOPT['emlerr'])
      set_one_savedfile(i, pgrec, file, fnames, type, info)

   PgLOG.pglog("{}/{} of {} Saved file record{} added/modified for {}!".format(ADDCNT, MODCNT, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, 0, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (ADDCNT + MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ''

#
# add or modify one saved file record
#
def set_one_savedfile(i, pgrec, file, flds, type, info = None, ndsid = None, sact = 0):

   global ADDCNT, MODCNT
   tname = 'sfile'
   gindex = (PgOPT.params['GI'][i] if 'GI' in PgOPT.params and PgOPT.OPTS['GI'][2]&2 == 0 else (pgrec['gindex'] if pgrec else 0))
   dsid = ndsid if ndsid else PgOPT.params['DS']
   sid = pgrec['sid'] if pgrec else 0
   if not type: type = 'P'
   if 'RO' in PgOPT.params and 'DO' in PgOPT.params:
      PgOPT.params['DO'][i] = PgArch.get_next_disp_order(dsid, gindex)
   record = PgOPT.build_record(flds, pgrec, tname, i)
   if pgrec and PgOPT.PGOPT['ACTS']&PgOPT.OPTS['AS'][0]:
      record['uid'] = PgOPT.PGOPT['UID']
      if not info:
         info = PgFile.check_local_file(PgArch.get_saved_path(i, file, 1, type), 1, PgOPT.PGOPT['emerol'])
      if info:
         record['data_size'] = info['data_size']
         record['date_modified'] = info['date_modified']
         record['time_modified'] = info['time_modified']
         record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
         record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
      else:
         record['date_modified'] = PgUtil.curdate()
   if ndsid: record['dsid'] = ndsid
   if record:
      ccnt = fcnt = 0
      if 'locflag' in record and record['locflag'] == 'R': record['locflag'] = 'G'
      if not ('type' in record or (pgrec and pgrec['type'] == type)): record['type'] = type
      if not ('vindex' in record or pgrec and pgrec['vindex']):
         if dsid not in VINDEX: VINDEX[dsid] = PgOPT.get_version_index(dsid, PgOPT.PGOPT['extlog'])
         if VINDEX[dsid]: record['vindex'] = VINDEX[dsid]
      if 'status' not in record or record['status'] == 'P':
         if gindex and PgArch.get_group_type(dsid, gindex) == 'I':
            if 'status' in record:
               PgLOG.pglog("{}-{}: Keep file status Internal for Internal group {}".format(dsid, file, gindex), PgOPT.PGOPT['wrnlog'])
            record['status'] = 'I'
      if 'checksum' in record and record['checksum']: PgLOG.pglog("md5({}-{})={}".format(dsid, file, record['checksum']), PgLOG.LOGWRN)
      if 'gindex' in record and record['gindex']: record['tindex'] = PgMeta.get_top_gindex(dsid, record['gindex'])
      if pgrec:
         if pgrec['sfile'] != file: record['sfile'] = file
         if not ndsid:
            if not pgrec['dsid'] or pgrec['dsid'] == PgLOG.PGLOG['DEFDSID']:
               record['dsid'] = dsid
            elif dsid != pgrec['dsid']:
               return PgLOG.pglog("{}-{}: in {}, Move to {} first".format(type, file, pgrec['dsid'], dsid), PgOPT.PGOPT['emlerr'])
         if 'bid' in record and record['bid'] and pgrec['bid'] and record['bid'] != pgrec['bid']:
            return PgLOG.pglog("{}: Cannot change link to backup ID ({}/{})".format(file, record['bid'], pgrec['bid']), PgOPT.PGOPT['emlerr'])
         if 'data_format' in record and not record['data_format']:
            del record['data_format']
            if not record: return 0
         if pgrec['status'] == 'D':
            if 'status' not in record: record['status'] = 'P'
         ccnt = PgMeta.record_savedfile_changes(dsid, gindex, record, pgrec)
         if PgDBI.pgupdt(tname, record, "sid = {}".format(pgrec['sid']), PgLOG.LGEREX):
            MODCNT += 1
            fcnt = 1
      else:
         if record['sfile'] != file: record['sfile'] = file
         record['dsid'] = dsid
         record['uid'] = PgOPT.PGOPT['UID']
         if 'status' not in record: record['status'] = 'P'
         if not info:
            info = PgFile.check_local_file(PgArch.get_saved_path(i, file, 1, type), 1, PgOPT.PGOPT['emerol'])
         if info:
            if not info['isfile']: return PgLOG.pglog(file + ": is a directory", PgOPT.PGOPT['emlerr'])
            record['data_size'] = info['data_size']
            record['date_modified'] = info['date_modified']
            record['time_modified'] = info['time_modified']
            record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
            record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
         else:
            return PgLOG.pglog("{}-{}: {}".format(type, file, PgLOG.PGLOG['MISSFILE']), PgOPT.PGOPT['emlerr'])
         if 'disp_order' not in record:
            record['disp_order'] = PgArch.get_next_disp_order(dsid, gindex, tname, type)
         ccnt = PgMeta.record_savedfile_changes(dsid, gindex, record)
         sid = PgDBI.pgadd("sfile", record, PgLOG.LGEREX|PgLOG.AUTOID)
         if sid:
            ADDCNT += 1
            fcnt = 1
      if ccnt:
         if not sact: sact = 8
         PgMeta.save_filenumber(None, sact, 1, fcnt)

   return sid

#
# add or modify Quasar backup file information
#
def set_backfile_info():

   global ADDCNT, MODCNT
   bidx = zipped = 0
   tname = 'bfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Setting {} Backup file record{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   fnames = PgOPT.get_field_keys(tname, None)
   if not fnames: return PgLOG.pglog("Nothing to set for Backup file!", PgOPT.PGOPT['emlerr'])

   PgOPT.validate_multiple_values(tname, ALLCNT, fnames)
   if 'RO' in PgOPT.params and 'DO' not in PgOPT.params:
      if 'O' not in fnames: fnames += 'O'
      PgOPT.params['DO'] = [0]*ALLCNT

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Backup file record{} processed for set".format(bidx, ALLCNT), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return

   reorder = ADDCNT = MODCNT = 0
   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.add_dscheck_dcount(20, 0, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{} of {} {} Backup file record{} added/modified/processed".format(ADDCNT, MODCNT, i, ALLCNT, dsid, s)
      file = PgOPT.params['QF'][i]
      if not file: continue
      if 'QT' in PgOPT.params and PgOPT.params['QT'][i]:
         type = PgOPT.params['QT'][i]
         if 'BD'.find(type) < 0:
            PgLOG.pglog("{}-{}: Invalid Backup file Type '{}' to set".format(dsid, file, type), PgOPT.PGOPT['emerol'])
            continue
      else:
         PgLOG.pglog("{}-{}: Miss Backup file Type to Set".format(dsid, file), PgOPT.PGOPT['errlog'])
         continue
      typstr = "type = '{}'".format(type)

      # validate saved file
      pgrec = PgDBI.pgget(tname, "*", "{} AND bfile = '{}' AND {}".format(dcnd, file, typstr), PgLOG.LGEREX)
      if not pgrec:
         pgrec = PgDBI.pgget("sfile", "type", "{} AND bfile = '{}'".format(dcnd, file), PgOPT.PGOPT['extlog'])
         if pgrec:
            PgLOG.pglog("{}-{}: Fail to set, Move Action -MV to change file Type from '{}' to '{}'".format(dsid, file, pgrec['type'], type), PgOPT.PGOPT['emlerr'])
            continue
      set_one_backfile(i, pgrec, file, fnames, type)

   PgLOG.pglog("{}/{} of {} Backup file record{} added/modified for {}!".format(ADDCNT, MODCNT, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, 0, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (ADDCNT + MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ''

#
# add or modify one Quasar backup file record
#
def set_one_backfile(i, pgrec, file, flds, type, ndsid = None, record = None):

   global ADDCNT, MODCNT
   tname = 'bfile'
   dsid = ndsid if ndsid else PgOPT.params['DS']
   bid = pgrec['bid'] if pgrec else 0
   if 'RO' in PgOPT.params and 'DO' in PgOPT.params:
      PgOPT.params['DO'][i] = PgArch.get_next_disp_order(dsid)
   if not record: record = PgOPT.build_record(flds, pgrec, tname, i)
   if ndsid: record['dsid'] = ndsid
   if record:
      if pgrec:
         if 'data_format' in record and not record['data_format']:
            del record['data_format']
            if not record: return 0
         if pgrec['status'] == 'D':
            if 'status' not in record: record['status'] = 'A'
         if PgDBI.pgupdt(tname, record, "bid = {}".format(bid), PgLOG.LGEREX):
            MODCNT += 1
      else:
         if tname not in record or record[tname] != file: record[tname] = file
         record['dsid'] = dsid
         record['uid'] = PgOPT.PGOPT['UID']
         if 'status' not in record: record['status'] = 'A'
         info = PgFile.check_backup_file("/{}/{}".format(dsid, file), 'rda-quasar', 1, PgOPT.PGOPT['emerol'])
         if info:
            if not info['isfile']: return PgLOG.pglog(file + ": is a directory", PgOPT.PGOPT['emlerr'])
            record['data_size'] = info['data_size']
            record['date_created'] = record['date_modified'] = info['date_modified']
            record['time_created'] = record['time_modified'] = info['time_modified']
         else:
            return PgLOG.pglog("{}-{}: {}".format(type, file, PgLOG.PGLOG['MISSFILE']), PgOPT.PGOPT['emlerr'])
         if 'disp_order' not in record:
            record['disp_order'] = PgArch.get_next_disp_order(dsid, 0, tname)
         bid = PgDBI.pgadd(tname, record, PgLOG.LGEREX|PgLOG.AUTOID)
         if bid:
            ADDCNT += 1

   return bid

#
# moving WEB files tochange file paths/names, and/or from one dataset to another
#
def move_web_files():

   global RETSTAT, MODCNT
   tname = 'wfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   tmpds = tmpgs = None
   bidx = chksize = 0
   bucket = "rda-data"
   rcnt = len(PgLOG.PGLOG['WEBHOSTS'])
   s = 's' if ALLCNT > 1 else ''

   if 'OD' not in PgOPT.params:
      PgOPT.params['OD'] = dsid
   elif PgOPT.params['OD'] != dsid:
      tmpds = dsid
   if 'GI' not in PgOPT.params:
      if 'OG' in PgOPT.params:
         PgOPT.params['GI'] = PgOPT.params['OG']
         PgArch.validate_groups()
   elif 'OG' not in PgOPT.params:
        if 'GI' in PgOPT.params: PgOPT.params['OG'] = PgOPT.params['GI']
   elif 'OG' in PgOPT.params != PgOPT.params['GI']:
      tmpgs = PgOPT.params['GI']
   if tmpds:
      PgLOG.pglog("Move {} Web file{} from {} to {} ...".format(ALLCNT, s, PgOPT.params['OD'], dsid), PgLOG.WARNLG)
   else:
      PgLOG.pglog("Move {} Web file{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)
   PgOPT.validate_multiple_options(ALLCNT, ["WT", "OT", "OG", "RF"])
   if 'RF' not in PgOPT.params: PgOPT.params['RF'] = PgOPT.params['WF']
   if 'OT' not in PgOPT.params and 'WT' in PgOPT.params: PgOPT.params['OT'] = PgOPT.params['WT']
   if tmpds: PgOPT.params['DS'] = PgOPT.params['OD']
   if tmpgs: PgOPT.params['GI'] = PgOPT.params['OG']
   aolds = [None]*ALLCNT
   wolds = [None]*ALLCNT
   oolds = [None]*ALLCNT
   tolds = [None]*ALLCNT
   for i in range(ALLCNT):
      type = PgOPT.params['OT'][i] if 'OT' in PgOPT.params and PgOPT.params['OT'][i] else 'D'
      aolds[i] = PgArch.get_web_path(i, PgOPT.params['RF'][i], 5, type)
      wolds[i] = PgArch.get_web_path(i, PgOPT.params['RF'][i], 4, type)
      oolds[i] = PgArch.get_object_path(wolds[i], PgOPT.params['DS'])
      tolds[i] = type

   if tmpds: PgOPT.params['DS'] = tmpds
   if tmpgs: PgOPT.params['GI'] = tmpgs
   PgArch.cache_group_info(ALLCNT, 1)
   init = 1 if (tmpds or tmpgs) else 0
   anews = [None]*ALLCNT
   wnews = [None]*ALLCNT
   onews = [None]*ALLCNT
   tnews = [None]*ALLCNT
   for i in range(ALLCNT):
      type = PgOPT.params['WT'][i] if 'WT' in PgOPT.params and PgOPT.params['WT'][i] else 'D'
      anews[i] = PgArch.get_web_path(i, PgOPT.params['WF'][i], 5, type, init)
      wnews[i] = PgArch.get_web_path(i, PgOPT.params['WF'][i], 4, type)
      onews[i] = PgArch.get_object_path(wnews[i], dsid)
      tnews[i] = type
      init = 0

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Web file{} processed for move".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   fnames = "FIT"
   PgOPT.validate_multiple_values(tname, ALLCNT, fnames)
   reorder = metatotal = metacnt = wcnt = ocnt = MODCNT = 0
   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if metacnt > PgOPT.PGOPT['RSMAX']:
            metatotal += PgMeta.process_metadata("W", metacnt, PgOPT.PGOPT['emerol'])
            metacnt = 0
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{} of {} Web file{} moved/record-modified/processed".format(wcnt, MODCNT, i, ALLCNT, s)
      type = tolds[i]
      pgrec = PgSplit.pgget_wfile(PgOPT.params['OD'], "*", "wfile = '{}' AND type = '{}'".format(wolds[i], type), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("{}: Type '{}' Web File not in RDADB for {}".format(wolds[i], type, PgOPT.params['OD']), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['status'] == 'D':
         PgLOG.pglog("{}: Type '{}' Web File is not active in RDADB for {}".format(wolds[i], type, dsid), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['dsid'] != PgOPT.params['OD']:
         PgLOG.pglog("{}: Web File is actually in {}".format(wolds[i], pgrec['dsid']), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['vindex'] and tmpds:
         PgLOG.pglog(wolds[i] + ": cannot move version controlled Web file to a different dataset", PgOPT.PGOPT['emlerr'])
         continue
      elif tmpds and PgDBI.pgget("dsvrsn" , "", "{} AND status = 'A'".format(dcnd), PgOPT.PGOPT['extlog']):
         PgLOG.pglog("{}: cannot move Web file to version controlled dataset {}".format(wnews[i], dsid), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['locflag'] == 'C':
         PgLOG.pglog(wolds[i] + ": Cannot move Web File for CGD data", PgOPT.PGOPT['extlog'])

      newrec = PgSplit.pgget_wfile(dsid, "*", "wfile = '{}' AND type = '{}'".format(wnews[i], tnews[i]), PgLOG.LGEREX)
      if newrec and newrec['status'] != 'D':
         PgLOG.pglog("{}: cannot move to existing file {} of {}".format(wolds[i], newrec['wfile'], newrec['dsid']), PgOPT.PGOPT['emlerr'])
         continue
      if (pgrec['gindex'] and 'GI' not in PgOPT.params and
          not PgDBI.pgget("dsgroup", "", "{} and gindex = {}".format(dcnd, pgrec['gindex']), PgOPT.PGOPT['extlog'])):
         PgLOG.pglog("Group Index {} is not in RDADB for {}\n".format(pgrec['gindex'], dsid) +
                     "Specify Original/New group index via options -OG/-GI", PgOPT.PGOPT['extlog'])
      omove = wmove = 1
      if pgrec['locflag'] == 'O':
         wmove = 0
      elif pgrec['locflag'] == 'G':
         omove = 0
      if wmove and aolds[i] != anews[i]:
         ret = PgFile.move_local_file(anews[i], aolds[i], PgOPT.PGOPT['emerol']|OVERRIDE)
         if not ret:
            RETSTAT = 1
            continue
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         wcnt += 1
         set_web_move(pgrec)

      if omove and oolds[i] != onews[i]:
         if not PgFile.move_object_file(onews[i], oolds[i], bucket, bucket,  PgOPT.PGOPT['emerol']|OVERRIDE):
            RETSTAT = 1
            continue
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         ocnt += 1
      if newrec: PgSplit.pgdel_wfile(dsid, "wid = {}".format(newrec['wid']), PgOPT.PGOPT['extlog'])
      if not set_one_webfile(i, pgrec, wnews[i], fnames, tnews[i], None, tmpds): continue
      if pgrec['bid']: PgArch.save_move_info(pgrec['bid'], wolds[i], type, 'W', PgOPT.params['OD'], wnews[i], tnews[i], 'W', dsid)
      if PgOPT.PGOPT['GXTYP'].find(type) > -1 and pgrec['meta_link'] and pgrec['meta_link'] != 'N':
         if tmpds or wolds[i] != wnews[i]:
            metacnt += PgMeta.record_meta_move('W', PgOPT.params['OD'], dsid, wolds[i], wnews[i])
         elif 'GI' in PgOPT.params:
            gindex = PgOPT.params['GI'][i]
            if gindex != pgrec['gindex'] and (gindex or (PgOPT.OPTS['GI'][2]&2) == 0):
               metacnt += PgMeta.record_meta_summary('W', dsid, gindex, pgrec['gindex'])

   PgLOG.pglog("{}/{}/{}, Disk/Object/Record, of {} Web file{} moved".format(wcnt, ocnt, MODCNT, ALLCNT, s), PgOPT.PGOPT['emllog'])
   if metacnt > 0:
      metatotal += PgMeta.process_metadata('W', metacnt, PgOPT.PGOPT['emerol'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, 0, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (metatotal + MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
      if 'OD' in PgOPT.params and PgOPT.params['OD'] != dsid:
         PgDBI.reset_rdadb_version(PgOPT.params['OD'])
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ''

#
# moving Help files to change file paths/names, and/or from one dataset to another
#
def move_help_files():

   global RETSTAT, MODCNT
   tname = 'hfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   tmpds = None
   bidx = chksize = 0
   bucket = "rda-data"
   rcnt = len(PgLOG.PGLOG['WEBHOSTS'])
   s = 's' if ALLCNT > 1 else ''

   if 'OD' not in PgOPT.params:
      PgOPT.params['OD'] = dsid
   elif PgOPT.params['OD'] != dsid:
      tmpds = dsid
   if tmpds:
      PgLOG.pglog("Move {} Help file{} from {} to {} ...".format(ALLCNT, s, PgOPT.params['OD'], dsid), PgLOG.WARNLG)
   else:
      PgLOG.pglog("Move {} Help file{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)
   PgOPT.validate_multiple_options(ALLCNT, ["HT", "OT", "RF"])
   if 'RF' not in PgOPT.params: PgOPT.params['RF'] = PgOPT.params['HF']
   if 'OT' not in PgOPT.params and 'HT' in PgOPT.params: PgOPT.params['OT'] = PgOPT.params['HT']
   if tmpds: PgOPT.params['DS'] = PgOPT.params['OD']
   aolds = [None]*ALLCNT
   holds = [None]*ALLCNT
   oolds = [None]*ALLCNT
   tolds = [None]*ALLCNT
   for i in range(ALLCNT):
      type = PgOPT.params['OT'][i] if 'OT' in PgOPT.params and PgOPT.params['OT'][i] else 'D'
      stype = PgOPT.HTYPE[type] if type in PgOPT.HTYPE else 'Help'
      hpath = PgOPT.HPATH[type] if type in PgOPT.HPATH else 'help'
      aolds[i] = PgArch.get_help_path(i, PgOPT.params['RF'][i], 1, type)
      holds[i] = PgArch.get_help_path(i, PgOPT.params['RF'][i], 0, type)
      oolds[i] = PgArch.get_object_path(holds[i], PgOPT.params['DS'], hpath)
      tolds[i] = type

   init = 1 if tmpds else 0
   if tmpds: PgOPT.params['DS'] = tmpds
   anews = [None]*ALLCNT
   hnews = [None]*ALLCNT
   onews = [None]*ALLCNT
   tnews = [None]*ALLCNT
   for i in range(ALLCNT):
      type = PgOPT.params['HT'][i] if 'HT' in PgOPT.params and PgOPT.params['HT'][i] else 'D'
      stype = PgOPT.HTYPE[type] if type in PgOPT.HTYPE else 'Help'
      hpath = PgOPT.HPATH[type] if type in PgOPT.HPATH else 'help'
      anews[i] = PgArch.get_help_path(i, PgOPT.params['HF'][i], 1, type, init)
      hnews[i] = PgArch.get_help_path(i, PgOPT.params['HF'][i], 0, type)
      onews[i] = PgArch.get_object_path(hnews[i], dsid, hpath)
      tnews[i] = type
      init = 0

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Help file{} processed for move".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   fnames = "FT"
   PgOPT.validate_multiple_values(tname, ALLCNT, fnames)
   reorder = hcnt = ocnt = MODCNT = 0
   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{} of {} Help file{} moved/record-modified/processed".format(hcnt, MODCNT, i, ALLCNT, s)
      type = tolds[i]
      pgrec = PgDBI.pgget("hfile", "*", "hfile = '{}' AND type = '{}' AND dsid = '{}'".format(holds[i], type, PgOPT.params['OD']), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("{}: Type '{}' Help File not in RDADB for {}".format(holds[i], type, PgOPT.params['OD']), PgOPT.PGOPT['emlerr'])
         continue
      newrec = PgDBI.pgget(tname, "*", "hfile = '{}' AND type = '{}' AND {}".format(hnews[i], tnews[i], dcnd), PgLOG.LGEREX)
      if newrec:
         PgLOG.pglog("{}: cannot move to existing file {} of {}".format(holds[i], newrec['hfile'], newrec['dsid']), PgOPT.PGOPT['emlerr'])
         continue
      if pgrec['url']:
         PgLOG.pglog("{}-{}: Cannot move Help file on remote URL".format(holds[i], pgrec['url']), PgOPT.PGOPT['errlog'])
         PgOPT.params['LF'][i] = PgOPT.params['HF'][i] = None
         continue
      omove = hmove = 1
      if pgrec['locflag'] == 'O':
         hmove = 0
      elif pgrec['locflag'] == 'G':
         omove = 0
      if hmove and aolds[i] != anews[i]:
         ret = PgFile.move_local_file(anews[i], aolds[i], PgOPT.PGOPT['emerol']|OVERRIDE)
         if not ret:
            RETSTAT = 1
            continue
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         hcnt += 1

      if omove and oolds[i] != onews[i]:
         if not PgFile.move_object_file(onews[i], oolds[i], bucket, bucket, PgOPT.PGOPT['emerol']|OVERRIDE):
            RETSTAT = 1
            continue
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         ocnt += 1

      if not set_one_helpfile(i, pgrec, hnews[i], fnames, tnews[i], None, tmpds): continue

   PgLOG.pglog("{}/{}/{}/{}, Disk/Object/Record, of {} Help file{} moved".format(hcnt, ocnt, MODCNT, ALLCNT, s), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']: PgCMD.set_dscheck_dcount(ALLCNT, 0, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params: reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
      if 'OD' in PgOPT.params and PgOPT.params['OD'] != dsid:
         PgDBI.reset_rdadb_version(PgOPT.params['OD'])
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ''

#
# moving Saved files to Web files, both on glade and object store
#
def saved_to_web_files():

   global RETSTAT, MODCNT, ADDCNT
   tname = 'wfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   frombucket = "rda-decsdata"
   tobucket = "rda-data"
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0
   dslocflags = set()

   tmpds = tmpgs = None
   if 'OD' not in PgOPT.params:
      PgOPT.params['OD'] = dsid
   elif PgOPT.params['OD'] != dsid:
      tmpds = dsid
   if 'GI' not in PgOPT.params:
      if 'OG' in PgOPT.params:
         PgOPT.params['GI'] = PgOPT.params['OG']
         PgArch.validate_groups()
   elif 'OG' not in PgOPT.params:
        if 'GI' in PgOPT.params: PgOPT.params['OG'] = PgOPT.params['GI']
   elif 'OG' in PgOPT.params != PgOPT.params['GI']:
      tmpgs = PgOPT.params['GI']
   if tmpds:
      PgLOG.pglog("Move {} Saved to Web file{} from {} to {} ...".format(ALLCNT, s, PgOPT.params['OD'], dsid), PgLOG.WARNLG)
   else:
      PgLOG.pglog("Move {} Saved to Web file{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   PgOPT.validate_multiple_options(ALLCNT, ["WT", "OT", "OG", "RF"])
   if 'RF' not in PgOPT.params: PgOPT.params['RF'] = (PgOPT.params['SF'] if 'SF' in PgOPT.params else PgOPT.params['WF'])
   if 'OT' not in PgOPT.params and 'ST' in PgOPT.params: PgOPT.params['OT'] = PgOPT.params['ST']
   if tmpds: PgOPT.params['DS'] = PgOPT.params['OD']
   if tmpgs: PgOPT.params['GI'] = PgOPT.params['OG']
   aolds = [None]*ALLCNT
   solds = [None]*ALLCNT
   oolds = [None]*ALLCNT
   tolds = [None]*ALLCNT
   for i in range(ALLCNT):
      type = PgOPT.params['OT'][i] if 'OT' in PgOPT.params and PgOPT.params['OT'][i] else 'V'
      aolds[i] = PgArch.get_saved_path(i, PgOPT.params['RF'][i], 5, type)
      solds[i] = PgArch.get_saved_path(i, PgOPT.params['RF'][i], 4, type)
      oolds[i] = PgLOG.join_paths(PgOPT.params['DS'], solds[i])
      tolds[i] = type
   if tmpds: PgOPT.params['DS'] = tmpds
   if tmpgs: PgOPT.params['GI'] = tmpgs
   if 'WF' not in PgOPT.params: PgOPT.params['WF'] = (PgOPT.params['SF'] if 'SF' in PgOPT.params else PgOPT.params['RF'])
   PgArch.cache_group_info(ALLCNT, 1)
   init = 1 if (tmpds or tmpgs) else 0
   anews = [None]*ALLCNT
   wnews = [None]*ALLCNT
   onews = [None]*ALLCNT
   tnews = [None]*ALLCNT
   for i in range(ALLCNT):
      type = PgOPT.params['WT'][i] if 'WT' in PgOPT.params and PgOPT.params['WT'][i] else 'D'
      anews[i] = PgArch.get_web_path(i, PgOPT.params['WF'][i], 5, type, init)
      wnews[i] = PgArch.get_web_path(i, PgOPT.params['WF'][i], 4, type)
      onews[i] = PgLOG.join_paths(dsid, wnews[i])
      tnews[i] = type
      init = 0

   PgOPT.validate_multiple_values(tname, ALLCNT)
   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Saved to Web file{} processed for move".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   if 'QF' in PgOPT.params:
      PgOPT.params['QF'] = PgArch.get_bid_numbers(PgOPT.params['QF'])
   else:
      PgOPT.params['QF'] = [0]*ALLCNT
      PgOPT.OPTS['QF'][2] |= 2
   if 'GI' not in PgOPT.params: PgOPT.params['GI'] = [0]*ALLCNT
   if 'SZ' not in PgOPT.params: PgOPT.params['SZ'] = [0]*ALLCNT
   if 'DF' not in PgOPT.params: PgOPT.params['DF'] = [None]*ALLCNT
   if 'AF' not in PgOPT.params: PgOPT.params['AF'] = [None]*ALLCNT
   if 'LC' not in PgOPT.params: PgOPT.params['LC'] = [None]*ALLCNT
   if 'MC' not in PgOPT.params: PgOPT.params['MC'] = [None]*ALLCNT
   if 'DE' not in PgOPT.params: PgOPT.params['DE'] = [None]*ALLCNT
   fnames = None
   reorder = metatotal = metacnt = wcnt = ocnt = dcnt = ADDCNT = MODCNT = 0
   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if metacnt > PgOPT.PGOPT['RSMAX']:
            metatotal += PgMeta.process_metadata("W", metacnt, PgOPT.PGOPT['emerol'])
            metacnt = 0
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{}/{}/{}, Disk/Object/RecordDelete/RecordAdd/Proccessed, of {} Saved file{} moved".format(wcnt, ocnt, dcnt, ADDCNT, i, ALLCNT, s)
      type = tolds[i]
      pgrec = PgDBI.pgget("sfile", "*", "sfile = '{}' AND dsid = '{}' AND type = '{}'".format(solds[i], PgOPT.params['OD'], type), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("{}: Type '{}' Saved File not in RDADB for {}".format(solds[i], type, PgOPT.params['OD']), PgOPT.PGOPT['emlerr'])
         continue
      if tnews[i] == 'O' or tnews[i] == 'S':
         PgLOG.pglog("{}: Cannot move Saved File to Web Type '{}' in {}".format(solds[i], tnews[i], dsid), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['locflag'] == 'C':
         PgLOG.pglog(solds[i] + ": Cannot move Saved File to Web file for CGD data", PgOPT.PGOPT['extlog'])

      newrec = PgSplit.pgget_wfile(dsid, "*", "wfile = '{}'".format(wnews[i]), PgLOG.LGEREX)
      if newrec and newrec['status'] != 'D':
         PgLOG.pglog("{}: cannot move Saved to existing Web file {} of {}".format(solds[i], newrec['wfile'], newrec['dsid']), PgOPT.PGOPT['emlerr'])
         continue
      if (pgrec['gindex'] and not ('GI' in PgOPT.params and PgOPT.params['GI'][i]) and
          not PgDBI.pgget("dsgroup", "", "{} and gindex = {}".format(dcnd, pgrec['gindex']), PgOPT.PGOPT['extlog'])):
         PgLOG.pglog("Group Index {} is not in RDADB for {}\n".format(pgrec['gindex'], dsid) +
                     "Specify Original/New group index via options -OG/-GI", PgOPT.PGOPT['extlog'])
      ofrom = sfrom = omove = wmove = 1
      locflag = pgrec['locflag']
      if locflag == 'O':
         sfrom = 0
      elif locflag == 'G':
         ofrom = 0
      if not PgOPT.params['LC'][i] or PgOPT.params['LC'][i] == 'R':
         PgOPT.params['LC'][i] = locflag
      else:
         locflag = PgOPT.params['LC'][i]
      if locflag == 'O':
         wmove = 0
         dslocflags.add('O')
      elif locflag == 'G':
         omove = 0
         dslocflags.add('G')
      if wmove:
         if sfrom:
            stat = PgFile.move_local_file(anews[i], aolds[i], PgOPT.PGOPT['emerol']|OVERRIDE)
            sfrom = 0
         else:
            stat = PgFile.object_copy_local(anews[i], oolds[i], frombucket, PgOPT.PGOPT['emerol']|OVERRIDE)
         if not stat:
            RETSTAT = 1
            continue
         wcnt += 1
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
      if omove:
         if sfrom:
            stat = PgFile.local_copy_object(onews[i], aolds[i], tobucket, None, PgOPT.PGOPT['emerol']|OVERRIDE)
         elif wmove:
            stat = PgFile.local_copy_object(onews[i], anews[i], tobucket, None, PgOPT.PGOPT['emerol']|OVERRIDE)
         else:
            stat = PgFile.move_object_file(onews[i], oolds[i], tobucket, frombucket, PgOPT.PGOPT['emerol']|OVERRIDE)
            ofrom = 0
         if not stat:
            RETSTAT = 1
            continue
         ocnt += 1
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
      if sfrom: PgFile.delete_local_file(aolds[i], PgOPT.PGOPT['emerol'])
      if ofrom: PgFile.delete_object_file(oolds[i], frombucket, PgOPT.PGOPT['emerol'])

      if pgrec['bid'] and not PgOPT.params['QF'][i]: PgOPT.params['QF'][i] = pgrec['bid']
      if pgrec['gindex'] and not PgOPT.params['GI'][i]: PgOPT.params['GI'][i] = pgrec['gindex']
      if pgrec['data_size'] and not PgOPT.params['SZ'][i]: PgOPT.params['SZ'][i] = pgrec['data_size']
      if pgrec['data_format'] and not PgOPT.params['DF'][i]: PgOPT.params['DF'][i] = pgrec['data_format']
      if pgrec['file_format'] and not PgOPT.params['AF'][i]: PgOPT.params['AF'][i] = pgrec['file_format']
      if pgrec['checksum'] and not PgOPT.params['MC'][i]: PgOPT.params['MC'][i] = pgrec['checksum']
      if pgrec['note'] and not PgOPT.params['DE'][i]: PgOPT.params['DE'][i] = pgrec['note']
      if not fnames: fnames = PgOPT.get_field_keys(tname, None, "G")
      PgMeta.record_filenumber(pgrec['dsid'], pgrec['gindex'], 8, pgrec['type'], -1, -pgrec['data_size'])
      dcnt += PgSplit.pgdel_sfile("sid = {}".format(pgrec['sid']), PgOPT.PGOPT['extlog'])
      info = get_file_origin_info(wnews[i], pgrec)
      wid = set_one_webfile(i, newrec, wnews[i], fnames, tnews[i], info, dsid, 12)
      if not wid: continue
      if pgrec['bid']: PgArch.save_move_info(pgrec['bid'], solds[i], type, 'S', PgOPT.params['OD'], wnews[i], tnews[i], 'W', dsid)
      if 'GX' in PgOPT.params and PgOPT.PGOPT['GXTYP'].find(type) > -1:
         metacnt += PgMeta.record_meta_gather('W', dsid, wnews[i], PgOPT.params['DF'][i])
         PgMeta.cache_meta_tindex(dsid, wid, 'W')

   PgLOG.pglog("{}/{}/{}/{}, Disk/Object/RecordDelete/RecordAdd, of {} Saved file{} moved".format(wcnt, ocnt, dcnt, ADDCNT, ALLCNT, s), PgOPT.PGOPT['emllog'])
   if metacnt > 0:
      metatotal += PgMeta.process_metadata('W', metacnt, PgOPT.PGOPT['emerol'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, 0, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (metatotal + reorder + dcnt + ADDCNT + MODCNT) > 0:
      PgDBI.reset_rdadb_version(dsid)
      if 'OD' in PgOPT.params and PgOPT.params['OD'] != dsid: PgDBI.reset_rdadb_version(PgOPT.params['OD'])
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""
   if dslocflags: PgArch.set_dataset_locflag(dsid, dslocflags.pop())

#
# delete Web files from a given dataset
#
def delete_web_files():

   global RETSTAT
   tname = 'wfile'
   dsid = PgOPT.params['DS']
   bucket = "rda-data"
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0
   rcnt = len(PgLOG.PGLOG['WEBHOSTS'])
   PgLOG.pglog("Delete {} Web file{} from {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   reorder = metatotal = metacnt = dcnt = mcnt = ocnt = wcnt = 0
   PgArch.cache_group_info(ALLCNT, 0)
   PgOPT.validate_multiple_options(ALLCNT, ["WT", 'VI', 'QF', 'LC'])

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Web file{} processed for delete".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if metacnt >= PgOPT.PGOPT['RSMAX']:
            metatotal += PgMeta.process_meta_delete("W", PgOPT.PGOPT['emerol'])
            metacnt = 0
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{}/{}/{}, Disk/Object/DeleteRecord/ModifyRecord/Proccessed,of {} Web file{} deleted".format(wcnt, ocnt, dcnt, mcnt, i, ALLCNT, s)
      if 'WT' in PgOPT.params and PgOPT.params['WT'][i]:
         type = PgOPT.params['WT'][i]
      else:
         type = 'D'
      wfile = PgArch.get_web_path(i, PgOPT.params['WF'][i], 0, type)

      pgrec = PgSplit.pgget_wfile(dsid, "*", "wfile = '{}' AND type = '{}'".format(wfile, type), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("{}: type '{}' Web file is not in RDADB".format(wfile, type), PgOPT.PGOPT['errlog'])
         continue

      odel = wdel = oflag = wflag = 1
      locflag = pgrec['locflag']
      if locflag == 'O':
         wflag = 0
      elif locflag == 'G':
         oflag = 0
      elif locflag == 'C':
         wflag = oflag = 0
      if 'LC' in PgOPT.params and PgOPT.params['LC'][i]: locflag = PgOPT.params['LC'][i]
      if locflag == 'O':
         wdel = 0
      elif locflag == 'G':
         odel = 0
      elif locflag == 'C':
         wdel = odel = 0

      if (wflag+oflag) == (wdel+odel):
         vindex = PgOPT.params['VI'][i] if 'VI' in PgOPT.params else pgrec['vindex']
         if vindex:
            PgLOG.pglog(wfile + ": Web file is version controlled, add option -vi 0 to force delete", PgOPT.PGOPT['errlog'])
            continue
         bid = PgOPT.params['QF'][i] if 'QF' in PgOPT.params else pgrec['bid']
         if bid:
            PgLOG.pglog(wfile + ": Web file is Quasar backed up, add option -qf '' to force delete", PgOPT.PGOPT['errlog'])
            continue
         if PgOPT.PGOPT['GXTYP'].find(type) > -1 and ('DX' in PgOPT.params or pgrec and pgrec['meta_link'] and pgrec['meta_link'] != 'N'):
            metacnt += PgMeta.record_meta_delete('W', dsid, wfile)

      if wdel:
         afile = PgArch.get_web_path(i, PgOPT.params['WF'][i], 1, type)
         fcnt = PgFile.delete_local_file(afile, PgOPT.PGOPT['emerol'])
         if fcnt:
            wcnt += 1
            wflag = 0
            if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         elif PgFile.check_local_file(afile) is None:
            wflag = 0
      if odel:
         ofile = PgArch.get_object_path(wfile, dsid)
         if PgFile.delete_object_file(ofile, bucket, PgOPT.PGOPT['emerol']):
            ocnt += 1
            oflag = 0
            if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         elif PgFile.check_object_file(ofile, bucket) is None:
            oflag = 0
      wcnd = "wid = {}".format(pgrec['wid'])
      if (oflag + wflag) > 0:
         locflag = "O" if oflag else "G"
         lrec = {'locflag' : locflag}
         mcnt += PgSplit.pgupdt_wfile(dsid, lrec, wcnd, PgLOG.LGEREX)
      else:
         ccnt = PgMeta.record_filenumber(dsid, pgrec['gindex'], 4, (pgrec['type'] if pgrec['status'] == 'P' else ''), -1, -pgrec['data_size'])
         fcnt = PgSplit.pgdel_wfile(dsid, wcnd, PgLOG.LGEREX)
         if fcnt: dcnt += fcnt
         if ccnt: PgMeta.save_filenumber(dsid, 4, 1, fcnt)

   if (wcnt + ocnt + dcnt + mcnt) > 0:
      PgLOG.pglog("{}/{}/{}/{}, Disk/Object/DeleteRecord/ModifyRecord, of {} Web file{} deleted for {}".format(wcnt, ocnt, dcnt, mcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if metacnt > 0:
      metatotal += PgMeta.process_meta_delete('W', PgOPT.PGOPT['emerol'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (metatotal + dcnt + mcnt + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# delete Help files from a given dataset
#
def delete_help_files():

   global RETSTAT
   tname = 'hfile'
   dsid = PgOPT.params['DS']
   bucket = "rda-data"
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0
   rcnt = len(PgLOG.PGLOG['WEBHOSTS'])
   PgLOG.pglog("Delete {} Help file{} from {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   reorder = dcnt = hcnt = ocnt = mcnt = 0
   PgArch.cache_group_info(ALLCNT, 0)
   PgOPT.validate_multiple_options(ALLCNT, ["HT", 'LC'])

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Help file{} processed for delete".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{}/{}/{}, Disk/Record/Proccessed,of {} Help file{} deleted".format(hcnt, dcnt, mcnt, i, ALLCNT, s)
      hfile = PgOPT.params['HF'][i]
      if 'HT' in PgOPT.params and PgOPT.params['HT'][i]:
         type = PgOPT.params['HT'][i]
      else:
         PgLOG.pglog("{}: Specify Help file Type via Option -HT to delete".format(hfile), PgOPT.PGOPT['errlog'])
         continue
      stype = PgOPT.HTYPE[type] if type in PgOPT.HTYPE else 'Help'
      hpath = PgOPT.HPATH[type] if type in PgOPT.HPATH else 'help'
      pgrec = PgDBI.pgget(tname, "*", "hfile = '{}' AND type = '{}' AND {}".format(hfile, type, dcnd), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("{}: {} file is not in RDADB".format(hfile, stype), PgOPT.PGOPT['errlog'])
         continue

      if not type: type = pgrec['type']
      odel = hdel = oflag = hflag = 1
      locflag = pgrec['locflag']
      if locflag == 'O':
         hflag = 0
      elif locflag == 'G':
         oflag = 0
      if 'LC' in PgOPT.params and PgOPT.params['LC'][i]: locflag = PgOPT.params['LC'][i]
      if locflag == 'O':
         hdel = 0
      elif locflag == 'G':
         odel = 0

      if hdel:
         afile = PgArch.get_help_path(i, hfile, 1, type)
         if pgrec['url']:
            fcnt = 1
         else:
            fcnt = 0
            for j in range(rcnt):
               fcnt += PgFile.delete_local_file(afile, PgOPT.PGOPT['emerol'])
         if fcnt:
            hcnt += 1
            hflag = 0
            if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         elif PgFile.check_local_file(afile) is None:
            hflag = 0

      if odel:
         ofile = PgArch.get_object_path(hfile, dsid, hpath)
         if PgFile.delete_object_file(ofile, bucket, PgOPT.PGOPT['emerol']):
            ocnt += 1
            oflag = 0
            if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         elif PgFile.check_object_file(ofile, bucket) is None:
            oflag = 0
      if (oflag + hflag) > 0:
         locflag = "O" if oflag else "G"
         mcnt += PgDBI.pgexec("UPDATE hfile SET locflag = '{}' WHERE hid = {}".format(locflag, pgrec['hid']), PgLOG.LGEREX)
      else:
         dcnt += PgDBI.pgdel(tname, "hid = {}".format(pgrec['hid']), PgLOG.LGEREX)

   if (hcnt + ocnt + dcnt + mcnt) > 0:
      PgLOG.pglog("{}/{}/{}/{}, Disk/Object/DeleteRecord/ModifyRecord, of {} Web file{} deleted for {}".format(hcnt, ocnt, dcnt, mcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])

   if PgLOG.PGLOG['DSCHECK']: PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params: reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (dcnt + reorder) > 0: PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# moving Saved files to change file paths/names, and/or from one dataset to another
#
def move_saved_files():

   global RETSTAT, MODCNT
   tname = 'sfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   bucket = "rda-decsdata"
   s = 's' if ALLCNT > 1 else ''
   bidx = chksize = 0

   tmpds = tmpgs = None
   if 'OD' not in PgOPT.params:
      PgOPT.params['OD'] = dsid
   elif PgOPT.params['OD'] != dsid:
      tmpds = dsid
   if 'GI' not in PgOPT.params:
      if 'OG' in PgOPT.params:
         PgOPT.params['GI'] = PgOPT.params['OG']
         PgArch.validate_groups()
   elif 'OG' not in PgOPT.params:
        if 'GI' in PgOPT.params: PgOPT.params['OG'] = PgOPT.params['GI']
   elif 'OG' in PgOPT.params != PgOPT.params['GI']:
      tmpgs = PgOPT.params['GI']

   if tmpds:
      PgLOG.pglog("Move {} Saved file{} from {} to {} ...".format(ALLCNT, s, PgOPT.params['OD'], dsid), PgLOG.WARNLG)
   else:
      PgLOG.pglog("Move {} Saved file{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   PgOPT.validate_multiple_options(ALLCNT, ["ST", "OT", "OG", "RF"])
   if 'RF' not in PgOPT.params: PgOPT.params['RF'] = PgOPT.params['SF']
   if 'OT' not in PgOPT.params and 'ST' in PgOPT.params: PgOPT.params['OT'] = PgOPT.params['ST']
   if tmpds: PgOPT.params['DS'] = PgOPT.params['OD']
   if tmpgs: PgOPT.params['GI'] = PgOPT.params['OG']
   aolds = [None]*ALLCNT
   solds = [None]*ALLCNT
   oolds = [None]*ALLCNT
   tolds = [None]*ALLCNT
   for i in range(ALLCNT):
      type = PgOPT.params['OT'][i] if 'OT' in PgOPT.params and PgOPT.params['OT'][i] else 'P'
      aolds[i] = PgArch.get_saved_path(i, PgOPT.params['RF'][i], 5, type)
      solds[i] = PgArch.get_saved_path(i, PgOPT.params['RF'][i], 4, type)
      oolds[i] = PgArch.get_object_path(solds[i], PgOPT.params['DS'])
      tolds[i] = type
   if tmpds: PgOPT.params['DS'] = tmpds
   if tmpgs: PgOPT.params['GI'] = tmpgs
   PgArch.cache_group_info(ALLCNT, 1)
   init = 1 if (tmpds or tmpgs) else 0
   anews = [None]*ALLCNT
   snews = [None]*ALLCNT
   onews = [None]*ALLCNT
   tnews = [None]*ALLCNT
   for i in range(ALLCNT):
      type = PgOPT.params['ST'][i] if 'ST' in PgOPT.params and PgOPT.params['ST'][i] else 'P'
      anews[i] = PgArch.get_saved_path(i, PgOPT.params['SF'][i], 5, type, init)
      snews[i] = PgArch.get_saved_path(i, PgOPT.params['SF'][i], 4, type)
      onews[i] = PgArch.get_object_path(snews[i], dsid)
      tnews[i] = type
      init = 0

   fnames = "FIT"
   PgOPT.validate_multiple_values(tname, ALLCNT, fnames)
   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Saved file{} moved".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   reorder = MODCNT = scnt = ocnt = 0
   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{}/{}, Disk/Object/Record/Proccessed, of {}ALLCNT Saved files moved".format(scnt, ocnt, MODCNT, i, ALLCNT, s)
      type = tolds[i]
      pgrec = PgDBI.pgget(tname, "*", "sfile = '{}' AND dsid = '{}' AND type = '{}'".format(solds[i], PgOPT.params['OD'], type), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("{}: Type '{}' Saved File not in RDADB for {}".format(solds[i], type, PgOPT.params['OD']), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['dsid'] != PgOPT.params['OD']:
         PgLOG.pglog("{}: Saved File is actually in {}".format(solds[i], pgrec['dsid']), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['vindex'] and tmpds:
         PgLOG.pglog(solds[i] + ": cannot move version controlled Saved file to a different dataset", PgOPT.PGOPT['emlerr'])
         continue
      elif tmpds and PgDBI.pgget("dsvrsn" , "", "{} AND status = 'A'".format(dcnd), PgOPT.PGOPT['extlog']):
         PgLOG.pglog("{}: cannot move Saved file to version controlled dataset {}".format(snews[i], dsid), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['locflag'] == 'C':
         PgLOG.pglog(solds[i] + ": Cannot move Saved File for CGD data", PgOPT.PGOPT['extlog'])

      newrec = PgDBI.pgget(tname, "*", "sfile = '{}' AND {} AND type = '{}'".format(snews[i], dcnd, tnews[i]), PgLOG.LGEREX)
      if newrec:
         PgLOG.pglog("{}: cannot move to existing file {} of {}".format(solds[i], newrec['sfile'], newrec['dsid']), PgOPT.PGOPT['emlerr'])
         continue
      if (pgrec['gindex'] and not PgOPT.params['GI'] and
          not PgDBI.pgget("dsgroup", "", "{} and gindex = {}".format(dcnd, pgrec['gindex']), PgOPT.PGOPT['extlog'])):
         PgLOG.pglog("Group Index {} is not in RDADB for {}\n".format(pgrec['gindex'], dsid) +
                     "Specify Original/New group index via options -OG/-GI", PgOPT.PGOPT['extlog'])
      omove = smove = 1
      if pgrec['locflag'] == 'O':
         smove = 0
      elif pgrec['locflag'] == 'G':
         omove = 0
      if smove and aolds[i] != anews[i]:
         if not PgFile.move_local_file(anews[i], aolds[i], PgOPT.PGOPT['emerol']|OVERRIDE):
            RETSTAT = 1
            continue
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         scnt += 1
      if omove and oolds[i] != onews[i]:
         if not PgFile.move_object_file(onews[i], oolds[i], bucket, bucket, PgOPT.PGOPT['emerol']|OVERRIDE):
            RETSTAT = 1
            continue
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         ocnt += 1
      set_one_savedfile(i, pgrec, snews[i], fnames, tnews[i], None, tmpds)
      if pgrec['bid']: PgArch.save_move_info(pgrec['bid'], solds[i], type, 'S', PgOPT.params['OD'], snews[i], tnews[i], 'S', dsid)

   PgLOG.pglog("{}/{}/{}, Disk/Object/Record, of {} Saved file{} moved".format(scnt, ocnt, MODCNT, ALLCNT, s), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
      if 'OD' in PgOPT.params and PgOPT.params['OD'] != dsid:
         PgDBI.reset_rdadb_version(PgOPT.params['OD'])
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""  

#
# moving Quasar backup files from one dataset to another
#
def move_backup_files():

   global RETSTAT, MODCNT
   s = 's' if ALLCNT > 1 else ''
   tname = 'bfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   bkend = "rda-quasar"
   drend = 'rda-quasar-drdata'
   bidx = chksize = 0

   tmpds = None
   if 'QT' not in PgOPT.params: PgLOG.pglog("Miss File Type per -QT to move Quasar Backup files", PgOPT.PGOPT['extlog'])
   if 'OD' not in PgOPT.params:
      PgOPT.params['OD'] = dsid
   elif PgOPT.params['OD'] != dsid:
      tmpds = dsid
   if tmpds:
      PgLOG.pglog("Move {} Backup file{} from {} to {} ...".format(ALLCNT, s, PgOPT.params['OD'], dsid), PgLOG.WARNLG)
   else:
      PgLOG.pglog("Move {} Backup file{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   PgOPT.validate_multiple_options(ALLCNT, ["QT", "OT", "RF"])
   if 'RF' not in PgOPT.params: PgOPT.params['RF'] = PgOPT.params['QF']
   if 'OT' not in PgOPT.params and 'QT' in PgOPT.params: PgOPT.params['OT'] = PgOPT.params['QT']
   if tmpds: PgOPT.params['DS'] = PgOPT.params['OD']
   qolds = [None]*ALLCNT
   bolds = [None]*ALLCNT
   tolds = [None]*ALLCNT
   for i in range(ALLCNT):
      bolds[i] = PgOPT.params['RF'][i]
      qolds[i] = "/{}/{}".format(PgOPT.params['OD'], bolds[i])
      tolds[i] = PgOPT.params['OT'][i]
   if tmpds: PgOPT.params['DS'] = tmpds
   qnews = [None]*ALLCNT
   bnews = [None]*ALLCNT
   tnews = [None]*ALLCNT
   for i in range(ALLCNT):
      bnews[i] = PgOPT.params['QF'][i]
      qnews[i] = "/{}/{}".format(dsid, bnews[i])
      tnews[i] = PgOPT.params['QT'][i]
 
   fnames = "FT"
   PgOPT.validate_multiple_values(tname, ALLCNT, fnames)
   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Backup file{} moved".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   reorder = MODCNT = bcnt = dcnt = 0
   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{}/{}, Quasar/Drdata/Record/Proccessed, of {}ALLCNT Backup files moved".format(bcnt, dcnt, MODCNT, i, ALLCNT, s)
      type = tolds[i]
      ntype = tnews[i]
      pgrec = PgDBI.pgget(tname, "*", "bfile = '{}' AND dsid = '{}' AND type = '{}'".format(bolds[i], PgOPT.params['OD'], type), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("{}: Type '{}' Backup File not in RDADB for {}".format(bolds[i], type, PgOPT.params['OD']), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['dsid'] != PgOPT.params['OD']:
         PgLOG.pglog("{}: Backup File is actually in {}".format(bolds[i], pgrec['dsid']), PgOPT.PGOPT['emlerr'])
         continue
      elif type != ntype:
         PgLOG.pglog("{}: Type '{}' Backup File cannot be moved to '{}'".format(bolds[i], type, ntype), PgOPT.PGOPT['emlerr'])
         continue
      newrec = PgDBI.pgget(tname, "*", "bfile = '{}' AND {} AND type = '{}'".format(bnews[i], dcnd, ntype), PgLOG.LGEREX)
      if newrec:
         PgLOG.pglog("{}: cannot move to existing file {} of {}".format(bolds[i], newrec['bfile'], newrec['dsid']), PgOPT.PGOPT['emlerr'])
         continue
      dmove = bmove = 1
      if type == 'B': dmove = 0
      if qolds[i] != qnews[i]:
         if bmove:
            if not PgFile.move_backup_file(qnews[i], qolds[i], bkend, PgOPT.PGOPT['emerol']|OVERRIDE):
               RETSTAT = 1
               continue
            if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            bcnt += 1
         if dmove:
            if not PgFile.move_backup_file(qnews[i], qolds[i], drend, PgOPT.PGOPT['emerol']|OVERRIDE):
               RETSTAT = 1
               continue
            if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            dcnt += 1
      set_one_backfile(i, pgrec, bnews[i], fnames, tnews[i], tmpds)

   PgLOG.pglog("{}/{}/{}, Quasar/Drdata/Record, of {} Backup file{} moved".format(bcnt, dcnt, MODCNT, ALLCNT, s), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (MODCNT + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
      if 'OD' in PgOPT.params and PgOPT.params['OD'] != dsid:
         PgDBI.reset_rdadb_version(PgOPT.params['OD'])
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""  

#
# moving Web files to Saved files, for files both on glade and object store
#
def web_to_saved_files():

   global RETSTAT, MODCNT, ADDCNT
   s = 's' if ALLCNT > 1 else ''
   tname = 'sfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   frombucket = "rda-data"
   tobucket = "rda-decsdata"
   bidx = chksize = 0

   tmpds = tmpgs = None
   if 'OD' not in PgOPT.params:
      PgOPT.params['OD'] = dsid
   elif PgOPT.params['OD'] != dsid:
      tmpds = dsid
   if 'GI' not in PgOPT.params:
      if 'OG' in PgOPT.params:
         PgOPT.params['GI'] = PgOPT.params['OG']
         PgArch.validate_groups()
   elif 'OG' not in PgOPT.params:
        if 'GI' in PgOPT.params: PgOPT.params['OG'] = PgOPT.params['GI']
   elif 'OG' in PgOPT.params != PgOPT.params['GI']:
      tmpgs = PgOPT.params['GI']
   if tmpds:
      PgLOG.pglog("Move {} Web to Saved file{} from {} to {} ...".format(ALLCNT, s, PgOPT.params['OD'], dsid), PgLOG.WARNLG)
   else:
      PgLOG.pglog("Move {} Web to Saved file{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   if 'RF' not in PgOPT.params: PgOPT.params['RF'] = (PgOPT.params['WF'] if 'WF' in PgOPT.params else PgOPT.params['SF'])
   if 'OT' not in PgOPT.params and 'WT' in PgOPT.params: PgOPT.params['OT'] = PgOPT.params['WT']
   PgOPT.validate_multiple_options(ALLCNT, ["ST", "OT", "OG", "RF"])

   if tmpds: PgOPT.params['DS'] = PgOPT.params['OD']
   if tmpgs: PgOPT.params['GI'] = PgOPT.params['OG']
   aolds = [None]*ALLCNT
   wolds = [None]*ALLCNT
   oolds = [None]*ALLCNT
   tolds = [None]*ALLCNT
   for i in range(ALLCNT):
      type = PgOPT.params['OT'][i] if 'OT' in PgOPT.params and PgOPT.params['OT'][i] else 'D'
      aolds[i] = PgArch.get_web_path(i, PgOPT.params['RF'][i], 5, type)
      wolds[i] = PgArch.get_web_path(i, PgOPT.params['RF'][i], 4, type)
      oolds[i] = PgLOG.join_paths(PgOPT.params['DS'], wolds[i])
      tolds[i] = type
   if tmpds: PgOPT.params['DS'] = tmpds
   if tmpgs: PgOPT.params['GI'] = tmpgs
   if 'SF' not in PgOPT.params: PgOPT.params['SF'] = (PgOPT.params['WF'] if 'WF' in PgOPT.params else PgOPT.params['RF'])
   PgArch.cache_group_info(ALLCNT, 1)
   init = 1 if (tmpds or tmpgs) else 0
   anews = [None]*ALLCNT
   snews = [None]*ALLCNT
   onews = [None]*ALLCNT
   tnews = [None]*ALLCNT
   for i in range(ALLCNT):
      type = PgOPT.params['ST'][i] if 'ST' in PgOPT.params and PgOPT.params['ST'][i] else 'V'
      anews[i] = PgArch.get_saved_path(i, PgOPT.params['SF'][i], 5, type, init)
      snews[i] = PgArch.get_saved_path(i, PgOPT.params['SF'][i], 4, type)
      onews[i] = PgLOG.join_paths(dsid, snews[i])
      tnews[i] = type
      init = 0
   PgOPT.validate_multiple_values(tname, ALLCNT)

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("bidx of ALLCNT Web to Saved files processed for move", PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 4)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   if 'QF' in PgOPT.params:
      PgOPT.params['QF'] = PgArch.get_bid_numbers(PgOPT.params['QF'])
   else:
      PgOPT.params['QF'] = [0]*ALLCNT
      PgOPT.OPTS['QF'][2] |= 2
   if 'GI' not in PgOPT.params: PgOPT.params['GI'] = [0]*ALLCNT
   if 'SZ' not in PgOPT.params: PgOPT.params['SZ'] = [0]*ALLCNT
   if 'VI' not in PgOPT.params: PgOPT.params['VI'] = [0]*ALLCNT
   if 'DF' not in PgOPT.params: PgOPT.params['DF'] = [None]*ALLCNT
   if 'AF' not in PgOPT.params: PgOPT.params['AF'] = [None]*ALLCNT
   if 'LC' not in PgOPT.params: PgOPT.params['LC'] = [None]*ALLCNT
   if 'MC' not in PgOPT.params: PgOPT.params['MC'] = [None]*ALLCNT
   if 'DE' not in PgOPT.params: PgOPT.params['DE'] = [None]*ALLCNT
   fnames = None
   metatotal = metacnt = reorder = wcnt = ocnt = dcnt = ADDCNT = MODCNT = 0
   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if metacnt >= PgOPT.PGOPT['RSMAX']:
            metatotal += PgMeta.process_meta_delete("W", PgOPT.PGOPT['emerol'])
            metacnt = 0
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{}/{}/{}, Disk/Object/RecordDeleted/RecordAdded/Proccessed, of {}ALLCNT Web file{} moved".format(wcnt, ocnt, dcnt, ADDCNT, i, ALLCNT, s)
      type = tolds[i]
      pgrec = PgSplit.pgget_wfile(PgOPT.params['OD'], "*", "wfile = '{}' AND type = '{}'".format(wolds[i], type), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("{}: Type '{}' Web File not in RDADB for {}".format(wolds[i], type, PgOPT.params['OD']), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['status'] == 'D':
         PgLOG.pglog("{}: Type '{}' Web File is not active in RDADB for {}".format(wolds[i], type, dsid), PgOPT.PGOPT['emlerr'])
         continue
      elif pgrec['locflag'] == 'C':
         PgLOG.pglog(wolds[i] + ": Cannot move Web File to Saved file for CGD data", PgOPT.PGOPT['extlog'])

      newrec = PgDBI.pgget(tname, "*", "sfile = '{}' AND {}".format(snews[i], dcnd), PgLOG.LGEREX)
      if newrec:
         PgLOG.pglog("{}: cannot move Web to existing Saved file {} of {}".format(wolds[i], newrec['sfile'], newrec['dsid']), PgOPT.PGOPT['emlerr'])
         continue
      if (pgrec['gindex'] and not ('GI' in PgOPT.params and PgOPT.params['GI'][i]) and
          not PgDBI.pgget("dsgroup", "", "{} and gindex = {}".format(dcnd, pgrec['gindex']), PgOPT.PGOPT['extlog'])):
         PgLOG.pglog("Group Index {} is not in RDADB for {}\n".format(pgrec['gindex'], dsid) +
                     "Specify Original/New group index via options -OG/-GI", PgOPT.PGOPT['extlog'])
      ofrom = wfrom = omove = smove = 1
      locflag = pgrec['locflag']
      if locflag == 'O':
         wfrom = 0
      elif locflag == 'G':
         ofrom = 0
      if not PgOPT.params['LC'][i] or PgOPT.params['LC'][i] == 'R':
         PgOPT.params['LC'][i] = locflag
      else:
         locflag = PgOPT.params['LC'][i]
      if locflag == 'O':
         smove = 0
      elif locflag == 'G':
         omove = 0
      if smove:
         if wfrom:
            stat = PgFile.move_local_file(anews[i], aolds[i], PgOPT.PGOPT['emerol']|OVERRIDE)
            wfrom = 0
         else:
            stat = PgFile.object_copy_local(anews[i], oolds[i], frombucket, PgOPT.PGOPT['emerol']|OVERRIDE)
         if not stat:
            RETSTAT = 1
            continue
         wcnt += 1
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
      if omove:
         if wfrom:
            stat = PgFile.local_copy_object(onews[i], aolds[i], tobucket, None, PgOPT.PGOPT['emerol']|OVERRIDE)
         elif smove:
            stat = PgFile.local_copy_object(onews[i], anews[i], tobucket, None, PgOPT.PGOPT['emerol']|OVERRIDE)
         else:
            stat = PgFile.move_object_file(onews[i], oolds[i], tobucket, frombucket, PgOPT.PGOPT['emerol']|OVERRIDE)
            ofrom = 0
         if not stat:
            RETSTAT = 1
            continue
         ocnt += 1
         if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
      if wfrom: PgFile.delete_local_file(aolds[i], PgOPT.PGOPT['emerol'])
      if ofrom: PgFile.delete_object_file(oolds[i], frombucket, PgOPT.PGOPT['emerol'])

      if PgOPT.PGOPT['GXTYP'].find(type) > -1 and ('DX' in PgOPT.params or pgrec and pgrec['meta_link'] and pgrec['meta_link'] != 'N' and 'KM' not in PgOPT.params):
         metacnt += PgMeta.record_meta_delete('W', dsid, pgrec['wfile'])
      if pgrec['bid'] and not PgOPT.params['QF'][i]: PgOPT.params['QF'][i] = pgrec['bid']
      if pgrec['gindex'] and not PgOPT.params['GI'][i]: PgOPT.params['GI'][i] = pgrec['gindex']
      if pgrec['vindex'] and not PgOPT.params['VI'][i]: PgOPT.params['VI'][i] = pgrec['vindex']
      if pgrec['data_size'] and not PgOPT.params['SZ'][i]: PgOPT.params['SZ'][i] = pgrec['data_size']
      if pgrec['data_format'] and not PgOPT.params['DF'][i]: PgOPT.params['DF'][i] = pgrec['data_format']
      if pgrec['file_format'] and not PgOPT.params['AF'][i]: PgOPT.params['AF'][i] = pgrec['file_format']
      if pgrec['checksum'] and not PgOPT.params['MC'][i]: PgOPT.params['MC'][i] = pgrec['checksum']
      if pgrec['note'] and not PgOPT.params['DE'][i]: PgOPT.params['DE'][i] = pgrec['note']
      if not fnames: fnames = PgOPT.get_field_keys(tname, None, "G")
      PgMeta.record_filenumber(dsid, pgrec['gindex'], 4, (pgrec['type'] if pgrec['status'] == 'P' else ''), -1, -pgrec['data_size'])
      dcnt += PgSplit.pgdel_wfile(dsid, "wid = {}".format(pgrec['wid']), PgLOG.LGEREX)
      info = get_file_origin_info(snews[i], pgrec)
      set_one_savedfile(i, None, snews[i], fnames, tnews[i], info, dsid, 12)
      if pgrec['bid']: PgArch.save_move_info(pgrec['bid'], wolds[i], type, 'W', PgOPT.params['OD'], snews[i], tnews[i], 'S', dsid)

   PgLOG.pglog("{}/{}/{}/{}, Disk/Object/RecordDelete/RecordAdd, of {} Web file{} moved".format(wcnt, ocnt, dcnt, ADDCNT, ALLCNT, s), PgOPT.PGOPT['emllog'])
   if metacnt > 0:
      metatotal += PgMeta.process_meta_delete('W', PgOPT.PGOPT['emerol'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], 'sfile')
   if (metatotal + reorder + ADDCNT + MODCNT + dcnt) > 0:
      PgDBI.reset_rdadb_version(dsid)
      if 'OD' in PgOPT.params and PgOPT.params['OD'] != dsid:
         PgDBI.reset_rdadb_version(PgOPT.params['OD'])
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""  

#
# get date/time/size info from given file record
#
def get_file_origin_info(fname, pgrec):

   info = {'isfile' : 1, 'data_size' : pgrec['data_size']}
   info['fname'] = op.basename(fname)
   info['date_modified'] = pgrec['date_modified']
   info['time_modified'] = pgrec['time_modified']
   info['date_created'] = pgrec['date_created']
   info['time_created'] = pgrec['time_created']

   return info

#
# delete saved files from a given dataset
#
def delete_saved_files():

   tname = 'sfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   bucket = "rda-decsdata"
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Delete {} Saved file{} from {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   bidx = chksize = reorder = scnt = ocnt = dcnt = 0
   PgArch.cache_group_info(ALLCNT, 0)
   PgOPT.validate_multiple_options(ALLCNT, ["ST", 'VI', 'QF', 'LC'])

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Saved file{} processed for delete".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         set_rearchive_filenumber(dsid, bidx, ALLCNT, 8)
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{}/i, Disk/Object/Record/processed, of {} Saved file{} deleted".format(scnt, ocnt, dcnt, i, ALLCNT, s)

      sfile = PgOPT.params['SF'][i]
      if 'ST' in PgOPT.params and PgOPT.params['ST'][i]:
         type = PgOPT.params['ST'][i]
      else:
         PgLOG.pglog("{}-{}: Miss Saved file Type to Delete".format(dsid, sfile), PgOPT.PGOPT['emlerr'])
         continue
      sfile = PgArch.get_saved_path(i, sfile, 0, type)
      pgrec = PgDBI.pgget(tname, "*", "sfile = '{}' AND type = '{}' AND {}".format(sfile, type, dcnd), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("{}-{}: Type '{}' Saved file is not in RDADB".format(dsid, sfile, type), PgOPT.PGOPT['errlog'])
         continue

      odel = sdel = oflag = sflag = 1
      locflag = pgrec['locflag']
      if locflag == 'O':
         sflag = 0
      elif locflag == 'G':
         oflag = 0
      elif locflag == 'C':
         sflag = oflag = 0
      if 'LC' in PgOPT.params and PgOPT.params['LC'][i]: locflag = PgOPT.params['LC'][i]
      if locflag == 'O':
         sdel = 0
      elif locflag == 'G':
         odel = 0
      elif locflag == 'C':
         sdel = odel = 0
      if (sflag+oflag) == (sdel+odel):
         vindex = PgOPT.params['VI'][i] if 'VI' in PgOPT.params else pgrec['vindex']
         if vindex:
            PgLOG.pglog(sfile + ": Saved file is version controlled, add option -vi 0 to force delete", PgOPT.PGOPT['errlog'])
            continue
         bid = PgOPT.params['QF'][i] if 'QF' in PgOPT.params else pgrec['bid']
         if bid:
            PgLOG.pglog(sfile + ": Saved file is Quasar backed up, add option -qf '' to force delete", PgOPT.PGOPT['errlog'])
            continue

      if sdel:
         afile = PgArch.get_saved_path(i, PgOPT.params['SF'][i], 1, type)
         if PgFile.delete_local_file(afile, PgOPT.PGOPT['emerol']):
            scnt += 1
            sflag = 0
            if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         elif PgFile.check_local_file(afile) is None:
            sflag = 0
      if odel:
         ofile = PgLOG.join_paths(dsid, sfile)
         if PgFile.delete_object_file(ofile, bucket, PgOPT.PGOPT['emerol']):
            ocnt += 1
            oflag = 0
            if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         elif PgFile.check_object_file(ofile, bucket) is None:
            oflag = 0
      if (oflag + sflag) > 0:
         locflag = "O" if oflag else "G"
         PgDBI.pgexec("UPDATE sfile SET locflag = '{}' WHERE sid = {}".format(locflag, pgrec['sid']), PgLOG.LGEREX)
      else:
         ccnt = PgMeta.record_filenumber(dsid, pgrec['gindex'], 8, 'P', -1, -pgrec['data_size'])
         fcnt = PgSplit.pgdel_sfile("sid = {}".format(pgrec['sid']), PgLOG.LGEREX)
         if fcnt: dcnt += fcnt
         if ccnt: PgMeta.save_filenumber(dsid, 8, 1, fcnt)

   if (scnt + ocnt + dcnt) > 0:
      PgLOG.pglog("{}/{}/{}, Disk/Object/Record, of {} Saved file{} deleted for {}".format(scnt, ocnt, dcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (dcnt + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# delete Quasar backup files from a given dataset
#
def delete_backup_files():

   tname = 'bfile'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   brec = {'bid' : 0}
   bkend = "rda-quasar"
   drend = "rda-quasar-drdata"
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Delete {} Backup file{} from {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   bidx = chksize = reorder = bcnt = dcnt = delcnt = scnt = wcnt = 0
   PgOPT.validate_multiple_options(ALLCNT, ["QT"])

   if PgLOG.PGLOG['DSCHECK']:
      bidx = PgCMD.set_dscheck_fcount(ALLCNT, PgOPT.PGOPT['extlog'])
      if bidx > 0:
         PgLOG.pglog("{} of {} Backup file{} processed for delete".format(bidx, ALLCNT, s), PgOPT.PGOPT['emllog'])
         if bidx == ALLCNT: return
         chksize = PgLOG.PGLOG['DSCHECK']['size']

   for i in range(bidx, ALLCNT):
      if i > bidx and ((i-bidx)%20) == 0:
         if PgLOG.PGLOG['DSCHECK']:
            PgCMD.set_dscheck_dcount(i, chksize, PgOPT.PGOPT['extlog'])
         if 'EM' in PgOPT.params:
            PgLOG.PGLOG['PRGMSG'] = "{}/{}/{}/i, Quasar/Drdata/Record/processed, of {} Backup file{} deleted".format(bcnt, dcnt, delcnt, i, ALLCNT, s)

      (bfile, qfile) = get_backup_filenames(PgOPT.params['QF'][i], dsid)
      if 'QT' in PgOPT.params and PgOPT.params['QT'][i]:
         type = PgOPT.params['QT'][i]
      else:
         PgLOG.pglog("{}-{}: Miss backup file Type to Delete".format(dsid, bfile), PgOPT.PGOPT['emlerr'])
         continue
      pgrec = PgDBI.pgget(tname, "*", "bfile = '{}' AND type = '{}' AND {}".format(bfile, type, dcnd), PgLOG.LGEREX)
      if not pgrec:
         PgLOG.pglog("{}-{}: Type '{}' Backup file is not in RDADB".format(dsid, bfile, type), PgOPT.PGOPT['errlog'])
         continue
      bdel = ddel = 1
      if type == 'B': ddel = 0
      if bdel:
         if PgFile.delete_backup_file(qfile, bkend, PgOPT.PGOPT['emerol']):
            bcnt += 1
            if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']
      if ddel:
         if PgFile.delete_backup_file(qfile, drend, PgOPT.PGOPT['emerol']):
            dcnt += 1
            if PgLOG.PGLOG['DSCHECK']: chksize += pgrec['data_size']

      bcnd = "bid = {}".format(pgrec['bid'])
      if pgrec['scount']:
         scnt += PgDBI.pgupdt("sfile", brec, bcnd, PgLOG.LGEREX)
      if pgrec['wcount']:
         wcnt += PgSplit.pgupdt_wfile_dsids(dsid, pgrec['dsids'], brec, bcnd, PgLOG.LGEREX)

      fcnt = PgDBI.pgdel(tname, bcnd, PgLOG.LGEREX)
      if fcnt: delcnt += fcnt

   if (bcnt + dcnt + delcnt) > 0:
      PgLOG.pglog("{}/{}/{}, Quasar/Drdata/Record, of {} Backup file{} deleted for {}".format(bcnt, dcnt, delcnt, ALLCNT, s, dsid), PgOPT.PGOPT['emllog'])
   if (scnt + wcnt) > 0:
      PgLOG.pglog("{}/{} associated Web/Saved files cleaned for {}".format(wcnt, scnt, dsid), PgOPT.PGOPT['emllog'])
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_dcount(ALLCNT, chksize, PgOPT.PGOPT['extlog'])
   if 'ON' in PgOPT.params:
      reorder = PgArch.reorder_files(dsid, PgOPT.params['ON'], tname)
   if (delcnt + reorder) > 0:
      PgDBI.reset_rdadb_version(dsid)
   if 'EM' in PgOPT.params: PgLOG.PGLOG['PRGMSG'] = ""

#
# change existing group indices to new indices.  
#
def change_group_index():

   tname = 'dsgroup'
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Change {} group{} for {} ...".format(ALLCNT, s, dsid), PgLOG.WARNLG)

   # cache info of original groups
   tgroups = [1]*ALLCNT
   mlrecs = [None]*ALLCNT
   for i in range(ALLCNT):
      gindex = PgOPT.params['GI'][i]
      if gindex == 0:
         PgLOG.pglog("Error Change {} to GIndex 0".format(PgOPT.params['OG'][i]), PgOPT.PGOPT['extlog'])
      if PgDBI.pgget(tname, "", "{} AND gindex = {}".format(dcnd, gindex), PgOPT.PGOPT['extlog']):
         PgLOG.pglog("Error Change {} to existing GIndex {}".format(PgOPT.params['OG'][i], gindex), PgOPT.PGOPT['extlog'])
      cnd = "{} AND gindex = {}".format(dcnd, PgOPT.params['OG'][i])
      pgrec = PgDBI.pgget(tname, "meta_link, pindex", cnd, PgLOG.LGEREX)
      if not pgrec: PgLOG.pglog("Original GIndex {} not exists for {}".format(PgOPT.params['OG'][i], dsid), PgOPT.PGOPT['extlog'])
      mlrecs[i] = pgrec['meta_link']
      if pgrec['pindex']: tgroups[i] = 0

   # change groups, wfiles, and sfiles
   metacnt = prdcnt = savedcnt = webcnt = rccnt = pgcnt = modcnt = 0
   twcnt = tscnt = 0
   for i in range(ALLCNT):
      record = {'gindex' : PgOPT.params['GI'][i]}
      gcnd = "gindex = {}".format(PgOPT.params['OG'][i])
      cnd = "{} AND {}".format(dcnd, gcnd)
      modcnt += PgDBI.pgupdt(tname, record, cnd, PgLOG.LGEREX)
      prdcnt += PgDBI.pgget("dsperiod", "", cnd, PgLOG.LGEREX)
      chgcnt = PgSplit.pgupdt_wfile(dsid, record, gcnd, PgLOG.LGEREX)
      if chgcnt > 0:
         webcnt += chgcnt
         if re.search(r'^[XBW]$', mlrecs[i]):
            metacnt += PgMeta.record_meta_summary('W', dsid, PgOPT.params['OG'][i], record['gindex'])
      savedcnt += PgDBI.pgupdt("sfile", record, cnd, PgLOG.LGEREX)
      rccnt += PgDBI.pgupdt("rcrqst", record, cnd, PgLOG.LGEREX)

      if tgroups[i]:
         tgrec = {'tindex' : PgOPT.params['GI'][i]}
         tcnd = "tindex = {}".format(PgOPT.params['OG'][i])
         cnd = "{} AND {}".format(dcnd, tcnd)
         twcnt += PgSplit.pgupdt_wfile(dsid, tgrec, tcnd, PgLOG.LGEREX)
         tscnt += PgDBI.pgupdt("sfile", tgrec, cnd, PgLOG.LGEREX)

      pgrec = {'pindex' : PgOPT.params['GI'][i]}
      cnd = "pindex = {} AND {}".format(PgOPT.params['OG'][i], dcnd)
      pgcnt += PgDBI.pgupdt(tname, pgrec, cnd, PgLOG.LGEREX)
   
   PgLOG.pglog("{} of {} group{} changed".format(modcnt, ALLCNT, s), PgLOG.LOGWRN)
   if metacnt: PgMeta.process_meta_move('W')
   if modcnt > 0:
      if prdcnt > 0:
         s = 's' if prdcnt > 1 else ''
         PgLOG.pglog("Group info of {} period{} changed, modify the periods via metadata editor".format(prdcnt, s), PgLOG.LOGWRN)
      if pgcnt > 0:
         s = 's' if pgcnt > 1 else ''
         PgLOG.pglog("Parent Group Index info of {} group{} modified".format(pgcnt, s), PgLOG.LOGWRN)
      cnt = webcnt + savedcnt
      if cnt > 0:
         s = 's' if cnt > 1 else ''
         PgLOG.pglog("{}/{} associated Saved/Web file record{} modified for new group".format(savedcnt, webcnt, s), PgLOG.LOGWRN)
      if rccnt > 0:
         s = 's' if rccnt > 1 else ''
         PgLOG.pglog("{} associated Request Control record{} modified".format(rccnt, s), PgLOG.LOGWRN)
      cnt = twcnt + tscnt
      if cnt > 0:
         s = 's' if cnt > 1 else ''
         PgLOG.pglog("{}/{} associated Saved/Web file record{} modified for new top group".format(tscnt, twcnt, s), PgLOG.LOGWRN)

      PgDBI.reset_rdadb_version(dsid)

#
# view specialist defiend key/value pairs for given dsid
#
def view_keyvalues(dsid, kvalues, getkeys = 0):

   cond = "dsid = '{}' ".format(dsid)
   count = 0
   
   cnt = len(kvalues) if kvalues else 0
   if cnt == 1 and re.match(r'^all$', kvalues[0], re.I):
      cnt = 0
      getkeys = 1

   if cnt > 0:
      values = {'okeys' : [], 'value' : []}
      for i in range(cnt):
         pgrec = PgDBI.pgget("dsokeys", "value", "{}AND okey = '{}'".fomrat(cond, kvalues[i]), PgLOG.LGWNEX)
         if pgrec:
            values['okey'].append(kvalues[i])
            values['value'].append(pgrec['value'])
            count += 1
         else:
            PgLOG.pglog(kvalues[i] + ": key undefined", PgLOG.LOGERR)
   elif getkeys:
      values = PgDBI.pgmget("dsokeys", "okey, value", cond + "ORDER BY okey", PgLOG.LGWNEX)
      count = len(values['okey']) if values else 0

   if not count: return 0
   
   if 'FN' not in PgOPT.params: PgOPT.OUTPUT.write("{}{}{}\n".format(PgOPT.OPTS['DS'][1], PgOPT.params['ES'], dsid))
   if count == 1:
      PgOPT.OUTPUT.write("{}{}{}=>{}\n".format(PgOPT.OPTS['KV'][1], PgOPT.params['ES'], values['okey'][0], values['value'][0]))
   else:
      PgOPT.OUTPUT.write("{}{}\n".format(PgOPT.OPTS['KV'][1], PgOPT.params['DV']))
      for i in range(count):
         PgOPT.OUTPUT.write("{}=>{}{}\n".format(values['okey'][i], values['value'][i], PgOPT.params['DV']))

   return count

#
# set specialist defiend key/value pairs for given dsid
#
def set_keyvalues(dsid, kvalues):
   
   cnt = len(kvalues) if kvalues else 0
   if cnt == 0: return 0
   s = 's' if cnt > 1 else ''
   PgLOG.pglog("Set {} key/value pairs for {} ...".format(cnt, dsid), PgLOG.WARNLG)
   
   dcnt = mcnt = acnt = 0
   for i in range(cnt):
      ms= re.search(r'^(.*)=>(.*)$', kvalues[i])
      if ms:
         key = ms.group(1)
         value = ms.group(2)
         if not value: value = None
      else:
         PgLOG.pglog(kvalues[i] + ": key Undefined", PgLOG.LOGERR)
         continue
      cond = "dsid = '{}' AND okey = '{}'".format(dsid, key)
      pgrec = PgDBI.pgget("dsokeys", "value", cond, PgLOG.LGWNEX)
      if pgrec:
         if value is None:  # empty value, delete record
            dcnt += PgDBI.pgdel("dsokeys", cond, PgLOG.LGWNEX)
         elif pgrec['value'] is None or value != pgrec['value']:
            pgrec['value'] = value
            mcnt += PgDBI.pgupdt("dsokeys", pgrec, cond, PgLOG.LGWNEX)
      else:
         pgrec = {'dsid' : dsid, 'okey' : key, 'value' : value}
         acnt += PgDBI.pgadd("dsokeys", pgrec, PgLOG.LGWNEX)

   PgLOG.pglog("{}/{}/{} of {} key/value pairs added/modified/deleted for {}!".format(acnt, mcnt, dcnt, cnt, dsid), PgLOG.LOGWRN)

   return (acnt + mcnt + dcnt)

#
# record moved web file info
#
def set_web_move(pgrec):
   
   date = PgUtil.curdate()
   cond = "wid = {} and date = '{}'".format(pgrec['wid'], date)
   
   if not PgDBI.pgget("wmove", "", cond, PgLOG.LGWNEX):
      record = {'dsid' : pgrec['dsid'], 'uid' : PgOPT.PGOPT['UID'],
                'wfile' : pgrec['wfile'], 'wid' : pgrec['wid'], 'date' : date}
      PgDBI.pgadd("wmove", record, PgLOG.LGWNEX)

#
# reset file counts for saved groups
#
def reset_group_filenumber(dsid, act):

   ucnt = 0
   gindices = sorted(CHGGRPS)
   
   if gindices and gindices[0] != 0:
      for gindex in gindices:
         if gindex: ucnt += PgMeta.reset_filenumber(dsid, gindex, act)
   else:
      ucnt += PgMeta.reset_filenumber(dsid, 0, act)

   return ucnt

#
# reset top group indices for given groups
#
def reset_top_group_index(dsid, act):

   tcnt = 0
   cgidxs = {}
   if 'GI' in PgOPT.params:
      for gindex in PgOPT.params['GI']:
         if gindex is None or gindex in cgidxs: continue
         tcnt += reset_top_group_index(dsid, gindex, act)
         cgidxs[gindex] = gindex
   else:
      tcnt += reset_top_group_index(dsid, 0, act)

   return tcnt

#
# set the re-archived file counts for groups
#
def set_rearchive_filenumber(dsid, bidx, total, act):

   global CHGGRPS

   if 'GI' in PgOPT.params:
      lmt = bidx + 20
      if lmt > total: lmt = total
      for i in range(bidx, lmt):
         if PgOPT.params['GI'][i]: CHGGRPS[PgOPT.params['GI'][i]] = 1

   reset_group_filenumber(dsid, act)
   CHGGRPS = {}

#
# reset group metadata via scm
#
def reset_group_metadata(dsid, act):

   dcnd = "dsid = '{}'".format(dsid)
   gindices = sorted(CHGGRPS)
   if gindices:
      for gindex in gindices:
         if gindex:
            pgrec = PgDBI.pgget("dsgroup", "meta_link", "{} AND gindex = {}".format(dcnd, gindex), PgLOG.LGEREX)
         else:
            pgrec = PgDBI.pgget("dataset", "meta_link", dcnd, PgLOG.LGEREX)
            gindex = "all"
         if not pgrec: continue
         if act == 1 or act&4 and re.search(r'(Y|B|W)', pgrec['meta_link']):
            PgLOG.pgsystem("{} -d {} -rw {}".format(PgOPT.PGOPT['scm'], dsid, gindex))
   else:
      pgrec = PgDBI.pgget("dataset", "meta_link", dcnd, PgLOG.LGEREX)
      if pgrec:
         if act == 1 or act&4 and re.search(r'(Y|B|W)', pgrec['meta_link']):
            PgLOG.pgsystem("{} -d {} -rw all".format(PgOPT.PGOPT['scm'], dsid))

#
# get web file name for given local file name
#
def get_archive_filename(lfile):

   return lfile if 'KP' in PgOPT.params else op.basename(lfile)

#
# clean up local files and directories after action
#
def clean_local_files():
   
   cnt = 0
   if 'DD' in PgOPT.params: PgFile.record_delete_directory(None, PgOPT.params['DD'])

   for lfile in PgOPT.params['LF']:
      if lfile and PgFile.delete_local_file(lfile, PgOPT.PGOPT['emerol']): cnt += 1
   if cnt > 0:
      s = ("s" if cnt > 1 else "")
      PgLOG.pglog("cnt local files cleaned", PgOPT.PGOPT['emerol'])

   if 'DD' in PgOPT.params: PgFile.clean_delete_directory(PgOPT.PGOPT['wrnlog'])

#
# transfer cached ERRMSG between globally and locally 
#
def reset_errmsg(errcnt):

   global ERRCNT, ERRMSG
   ret = 0
   if errcnt < 0:   # cache ERRMSG globally
      PgLOG.PGLOG['ERRCNT'] += ERRCNT
      PgLOG.PGLOG['ERRMSG'] += ERRMSG
      ERRCNT = 0
      ERRMSG = ''
   else:
      if errcnt > 0:  # cache ERRMSG locally
         ERRMSG += PgLOG.PGLOG['ERRMSG']
         ERRCNT += errcnt
         ret = 1
      PgLOG.PGLOG['ERRCNT'] = 0
      PgLOG.PGLOG['ERRMSG'] = ''

   return ret

#
# copy a file to a alternate destination
#
def copy_alter_local(wfile, ahome):

   afile = wfile
   bproc = PgSIG.PGSIG['BPROC']
   afile = re.sub(PgLOG.PGLOG['DSDHOME'], ahome, afile)
   if bproc > 1: PgSIG.PGSIG['BPROC'] = 1
   PgFile.local_copy_local(afile, wfile, PgOPT.PGOPT['emerol']|OVERRIDE)
   if bproc != PgSIG.PGSIG['BPROC']: PgSIG.PGSIG['BPROC'] = bproc

#
# delete a file at alternate location
#
def delete_alter_local(wfile, ahome):

   afile = wfile
   bproc = PgSIG.PGSIG['BPROC']
   afile = re.sub(PgLOG.PGLOG['DSDHOME'], ahome, afile)
   if bproc > 1: PgSIG.PGSIG['BPROC'] = 1
   if op.exists(afile): PgFile.delete_local_file(afile, PgOPT.PGOPT['emerol'])
   if bproc != PgSIG.PGSIG['BPROC']: PgSIG.PGSIG['BPROC'] = bproc

#
# get version information
#
def get_version_info():

   tname = "dsvrsn"
   dsid = PgOPT.params['DS']
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get version control info of {} from RDADB ...".format(dsid), PgLOG.WARNLG)

   lens = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT['dsvrsn'], PgOPT.PGOPT['vrsnall'])
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "V"
   condition = PgArch.get_condition(tname)
   ocnd = PgOPT.get_order_string(onames, tname)
   pgrecs = PgDBI.pgmget(tname, "*", condition + ocnd, PgOPT.PGOPT['extlog'])

   PgOPT.OUTPUT.write("{}{}{}\n".format(PgOPT.OPTS['DS'][1], PgOPT.params['ES'], dsid))
   if pgrecs and 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs, fnames, hash)
   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{} version control{} retrieved".format(cnt, s), PgOPT.PGOPT['wrnlog'])
   else:
      PgLOG.pglog("No version control information retrieved", PgOPT.PGOPT['wrnlog'])

#
# get a DOI number for given dsid
#
def get_new_version(nrec, doi):

   nrec['doi'] = doi
   if not ('start_date' in nrec and nrec['start_date']): nrec['start_date'] = PgUtil.curdate()
   if not ('start_time' in nrec and nrec['start_time']): nrec['start_time'] = PgUtil.curtime()
   nrec['end_date'] = nrec['end_time'] = None
   nrec['status'] = 'A' if doi else 'P'
   nrec['iversion'] = 1

   return nrec

#
# replace old version record with a new one
#
def transfer_version_info(nrec, orec, doi):

   dsid = PgOPT.params['DS']
   vinfo = "Version Control {}".format(orec['vindex'])
   if not ('start_date' in nrec and nrec['start_date']): nrec['start_date'] = PgUtil.curdate()
   if not ('start_time' in nrec and nrec['start_time']): nrec['start_time'] = PgUtil.curtime()
   if PgUtil.cmptime(orec['start_date'], orec['start_time'], nrec['start_date'], nrec['start_time']) >= 0:
      PgOPT.action_error("New Version Control must start later than {} {} of {}".format(orec['start_date'], orec['start_time'], vinfo))

   record = {}
   record['end_date'] = nrec['start_date']
   record['end_time'] = nrec['start_time']
   record['status'] = "H"
   if doi == orec['doi']:
      nrec['doi'] = orec['doi']
      nrec['iversion'] = orec['iversion'] + 1
      nrec['status'] = 'A'
      if 'eversion' not in nrec and orec['eversion']: nrec['eversion'] = orec['eversion']
      if 'note' not in nrec: PgOPT.action_error("DOI {}: Miss a brief reason via Option -DE for new Version Control".format(doi))
   else:
      PgLOG.pglog("DOI {}: Superseded by {} for {} {}".format(orec['doi'], doi, dsid, vinfo), PgOPT.PGOPT['wrnlog'])
   PgDBI.pgupdt("dsvrsn", record, "vindex = {}".format(orec['vindex']), PgOPT.PGOPT['extlog'])
   PgLOG.pglog("{} {}: Set status to 'H'".format(dsid, vinfo), PgOPT.PGOPT['wrnlog'])

   return nrec

#
# add or modify version control information
#
def set_version_info():

   tname = "dsvrsn"
   dsid = PgOPT.params['DS']
   dcnd = "dsid = '{}'".format(dsid)
   msg = "{} version control".format(ALLCNT)
   if ALLCNT > 1: msg += "s"
   PgLOG.pglog("Set information of {} ...".format(msg), PgLOG.WARNLG)

   addcnt = modcnt = 0
   fnames = PgOPT.get_field_keys(tname, None, 'V')
   PgOPT.validate_multiple_values(tname, ALLCNT, fnames)

   for i in range(ALLCNT):
      vidx = PgOPT.params['VI'][i]
      doi = PgOPT.params['DN'][i] if 'DN' in PgOPT.params else ''
      actrec = None
      if vidx > 0:
         cnd = "vindex = {}".format(vidx)
         vinfo = "Version Control {}".format(vidx)
         pgrec = PgDBI.pgget(tname, "*", cnd, PgOPT.PGOPT['extlog'])
         if not pgrec: PgOPT.action_error(vinfo + ": Not in RDADB")
         if pgrec['doi']:
            if not doi:
               doi = pgrec['doi']
            elif doi != pgrec['doi']:
               PgOPT.action_error("{}: DOI {} exists, cannot change to {}".format(vinfo, pgrec['doi'], doi))
      else:
         pgrec = PgDBI.pgget(tname, "vindex", dcnd + " AND status = 'P'", PgOPT.PGOPT['extlog'])
         if pgrec:
            PgOPT.action_error("Cannot add new Version Control for Pending Version Control {} exists".format(pgrec['vindex']))
         else:
            actrec = PgDBI.pgget(tname, "*", dcnd + " AND status = 'A'", PgOPT.PGOPT['extlog'])
            if actrec and not doi: doi = actrec['doi']

      record = PgOPT.build_record(fnames, pgrec, tname, i)
      if not vidx:
         record = get_new_version(record, doi)
         if actrec: record = transfer_version_info(record, actrec, doi)
      if record:
         vidx = 0
         if pgrec:
            if 'status' in record:
               if record['status'] == "H":
                  PgOPT.action_error(vinfo + ": Cannot set status to 'H', use Action -TV to terminate")
               elif pgrec['status'] == 'H':
                  PgOPT.action_error("{}: Cannot set status to '{}' from 'H'".format(vinfo, record['status']))
               elif record['status'] == "A":
                  if not doi: PgOPT.action_error(vinfo + ": Cannot set status to 'A' for missing DOI")
               elif record['status'] == "P":
                  if doi: PgOPT.action_error(vinfo + ": Cannot set status to 'P' for DOI set")
            if 'end_date' in record and record['end_date'] or 'end_time' in record and record['end_time']:
               if 'end_date' in record and pgrec['end_date']:
                  PgOPT.action_error(vinfo + ": Cannot change ending date/time")
               else:
                  PgOPT.action_error(vinfo + ": Cannot set ending date/time, use Action -TV to terminate")
            if not PgDBI.pgupdt(tname, record, cnd, PgOPT.PGOPT['extlog']): continue
            modcnt += 1
            if 'doi' in record: vidx = pgrec['vindex']
         else:
            if 'status' in record:
               if record['status'] == "H":
                  PgOPT.action_error("Cannot add new Version Control with status 'H'")
               elif record['status'] == "A":
                  if not doi: PgOPT.action_error("Cannot add new Version Control with status 'A' for missing DOI")
            if 'end_date' in record and record['end_date'] or 'end_time' in record and record['end_time']:
               PgOPT.action_error("Cannot add new Version Control with ending date/time")
            record['dsid'] = dsid
            vidx = PgDBI.pgadd(tname, record, PgOPT.PGOPT['extlog']|PgLOG.AUTOID|PgLOG.DODFLT)
            if not vidx: continue
            vinfo = "Version Control {}".format(vidx)
            addcnt += 1
            if not doi: vidx = 0

         if vidx:
            vrec = {'vindex' : vidx}
            vcnd = "type = 'D' AND vindex = 0"
            fcnt = PgSplit.pgupdt_wfile(dsid, vrec, vcnd, PgOPT.PGOPT['extlog'])
            if fcnt > 0:
               s = 's' if fcnt > 1 else ''
               PgLOG.pglog("{}: Linked {} Web file record{}".format(vinfo, fcnt, s), PgOPT.PGOPT['wrnlog'])
            fcnt = PgDBI.pgupdt("sfile", vrec, "{} AND {}".format(dcnd, vcnd), PgOPT.PGOPT['extlog'])
            if fcnt > 0:
               s = 's' if fcnt > 1 else ''
               PgLOG.pglog("{}: Linked {} Saved file record{}".format(vinfo, fcnt, s), PgOPT.PGOPT['wrnlog'])

            vcnd = "type = 'D' AND vindex <> {}".format(vidx)
            fcnt = PgSplit.pgupdt_wfile(dsid, vrec, vcnd, PgOPT.PGOPT['extlog'])
            if fcnt > 0:
               s = 's' if fcnt > 1 else ''
               PgLOG.pglog("{}: Relinked {} Web file record{}".format(vinfo, fcnt, s), PgOPT.PGOPT['wrnlog'])

            fcnt = PgDBI.pgupdt("sfile", vrec, "{} AND {}".format(dcnd, vcnd), PgOPT.PGOPT['extlog'])
            if fcnt > 0:
               s = 's' if fcnt > 1 else ''
               PgLOG.pglog("{}: Relinked {} Saved file record{}".format(vinfo, fcnt, s), PgOPT.PGOPT['wrnlog'])

   PgLOG.pglog("{}/{} of {} added/modified in RDADB!".format(addcnt, modcnt, msg), PgOPT.PGOPT['wrnlog'])

#
# terminate version control information for given version indices
#
def terminate_version_info():

   msg = "{} Version Control".format(ALLCNT)
   if ALLCNT > 1: msg += "s"
   PgLOG.pglog("Terminate {} ...".format(msg), PgLOG.WARNLG)

   dsid = PgOPT.params['DS']
   PgOPT.validate_multiple_options(ALLCNT, ["ED", "ET"])
   doicnt = modcnt = delcnt = 0
   for i in range(ALLCNT):
      vidx = PgOPT.params['VI'][i]
      cnd = "vindex = {}".format(vidx)
      vinfo = "{} Version Control {}".format(dsid, vidx)
      pgrec = PgDBI.pgget("dsvrsn", "doi, status", cnd, PgOPT.PGOPT['extlog'])
      if not pgrec:
         PgLOG.pglog(vinfo + ": Not in RDADB", PgLOG.LOGERR)
         continue
      elif pgrec['status'] == 'H':
         PgLOG.pglog(vinfo + ": is in status 'H'", PgLOG.LOGERR)
         continue
      else:
         cnt = PgSplit.pgget_wfile(dsid, "", cnd, PgOPT.PGOPT['extlog'])
         if cnt:
            s = 's' if cnt > 1 else ''
            PgLOG.pglog("{}: Cannot terminate for {} associated Web data file{}".format(vinfo, cnt, s), PgLOG.LOGERR)
            continue
      if pgrec['doi']:
         orec = PgDBI.pgget("dsvrsn", "vindex", "doi = '{}' AND vindex <> {}".format(pgrec['doi'], vidx), PgOPT.PGOPT['extlog'])
         if orec:
            PgLOG.pglog("{}: Cannot terminate for DOI {} is asscoated to Version control {}".format(vinfo, pgrec['doi'], orec['vindex']), PgLOG.LOGERR)
            continue
         doicnt += 1
         record = {'status' : "H"}
         record['end_date'] = PgOPT.params['ED'][i] if 'ED' in PgOPT.params else PgUtil.curdate()
         record['end_time'] = PgOPT.params['ET'][i] if 'ET' in PgOPT.params else PgUtil.curtime()
         if PgDBI.pgupdt("dsvrsn", record, cnd, PgOPT.PGOPT['extlog']):
            PgLOG.pglog(vinfo + ": Set status to 'H' to terminate", PgOPT.PGOPT['wrnlog'])
            modcnt += 1
      elif PgDBI.pgdel("dsvrsn", cnd, PgOPT.PGOPT['extlog']):
         PgLOG.pglog(vinfo + ": Deleted for no DOI to terminate", PgOPT.PGOPT['wrnlog'])
         delcnt += 1

   PgLOG.pglog("{}/{} of {} terminated for DOI/Version Control".format(doicnt, modcnt, msg), PgOPT.PGOPT['wrnlog'])
   if delcnt: PgLOG.pglog("{}/{} of {} deleted".format(delcnt, modcnt, msg), PgOPT.PGOPT['wrnlog'])

#
# call main() to start program
#
if __name__ == "__main__": main()
