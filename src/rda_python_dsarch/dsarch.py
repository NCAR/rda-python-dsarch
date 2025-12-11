#!/usr/bin/env python3
##################################################################################
#     Title: dsarch
#    Author: Zaihua Ji, zji@ucar.edu
#      Date: 09/29/2020
#            2025-01-25 transferred to package rda_python_dsarch from
#            https://github.com/NCAR/rda-utility-programs.git
#            2025-12-09 vonvert to class PgArch
#   Purpose: utility program to archive data files of a given dataset onto GDEX
#            server; and save information of data files into RDADB
#    Github: https://github.com/NCAR/rda-python-dsarch.git
##################################################################################
import sys
import os
import re
from os import path as op
from .pg_meta import PgMeta
from .pg_arch import PgArch

class DsArch(PgArch, PgMeta):
   def __init__(self):
      super().__init__()  # initialize parent class
      self.ERRCNT = self.RETSTAT = self.ALLCNT = self.ADDCNT = self.MODCNT = self.self.OVERRIDE = 0
      self.TARFILES = {}
      self.VINDEX = {}
      self.CHGGRPS = {}
      self.ERRMSG = ''

   # function to read paramters
   def read_parameters(self):
      self.set_help_path(__file__)
      pgname = "dsarch"
      self.parsing_input(pgname)
      self.set_suid(self.PGLOG['EUID'] if self.OPTS[self.PGOPT['CACT']][2] > 0 else self.PGLOG['RUID'])
      if 'DS' in self.params:
         dsid = self.params['DS']
      else:
         dsid = None
         if self.PGOPT['ACTS'] == self.OPTS['GH'][0] and 'HF' in self.params:
            dsid = self.get_dsid(self.params['HF'], 'helpfile')
         elif self.PGOPT['ACTS'] == self.OPTS['GW'][0] and 'WF' in self.params:
            dsid = self.get_dsid(self.params['WF'], 'wfile')
         elif self.PGOPT['ACTS'] == self.OPTS['GS'][0] and 'SF' in self.params:
            dsid = self.get_dsid(self.params['SF'], 'sfile')
         elif self.PGOPT['ACTS'] == self.OPTS['GQ'][0] and 'QF' in self.params:
            dsid = self.get_dsid(self.params['QF'], 'bfile')
         if dsid: self.params['DS'] = dsid
      if 'GI' in self.params and 'OG' in self.params:
         # try to gather the file names before set in case not given
         if self.PGOPT['ACTS'] == self.OPTS['SS'][0]:
            self.params['SF'] = self.get_filenames("sfile")
         elif self.PGOPT['ACTS'] == self.OPTS['SW'][0]:
            self.params['WF'] = self.get_filenames("wfile")
      self.check_enough_options(self.PGOPT['CACT'], self.PGOPT['ACTS'])

   # start actions of dsarch
   def start_actions(self):
      setds = 0
      dsid = self.params['DS']
      self.OVERRIDE = self.OVRIDE if 'OE' in self.params else 0
      if 'RO' in self.params and 'ON' in self.params: del self.params['RO']
      if self.PGOPT['ACTS'] == self.OPTS['AH'][0]:
         self.ALLCNT = len(self.params['HF']) if 'HF' in self.params else len(self.params['LF'])
         if 'XC' in self.params:
            self.crosscopy_help_files('Copy')
         elif 'XM' in self.params:
            self.crosscopy_help_files('Move')
         else:
            self.archive_help_files()
         if 'CL' in self.params: self.delete_local_files()
      elif self.PGOPT['ACTS'] == self.OPTS['AQ'][0]:
         if 'XC' in self.params:
            self.ALLCNT = len(self.params['QF'])
            self.crosscopy_backup_files()
         else:
            self.archive_backup_file()
      elif self.PGOPT['ACTS'] == self.OPTS['AS'][0]:
         self.ALLCNT = len(self.params['SF']) if 'SF' in self.params else len(self.params['LF'])
         self.cache_group_info(self.ALLCNT)
         if 'XC' in self.params:
            self.crosscopy_saved_files('Copy')
         elif 'XM' in self.params:
            self.crosscopy_saved_files('Move')
         else:
            self.archive_saved_files()
            if 'CL' in self.params: self.delete_local_files()
      elif self.PGOPT['ACTS'] == self.OPTS['AW'][0]:
         self.ALLCNT = len(self.params['WF']) if 'WF' in self.params else len(self.params['LF'])
         if 'ML' in self.params and 'GX' in self.params: del self.params['ML']
         self.cache_group_info(self.ALLCNT)
         if 'XC' in self.params:
            self.crosscopy_web_files('Copy')
         elif 'XM' in self.params:
            self.crosscopy_web_files('Move')
         else:
            self.archive_web_files()
         if self.PGSIG['BPROC'] > 1: self.set_webfile_info()
         if 'CL' in self.params: self.delete_local_files()
      elif self.PGOPT['ACTS'] & self.OPTS['CG'][0]:
         self.ALLCNT = len(self.params['GI'])
         self.change_group_index()
      elif self.PGOPT['ACTS'] == self.OPTS['DG'][0]:
         self.ALLCNT = len(self.params['GI'])
         self.delete_group_info()
      elif self.PGOPT['ACTS'] == self.OPTS['DL'][0]:
         if 'DD' not in self.params: self.params['DD'] = -1
         self.ALLCNT = (len(self.params['HF']) if 'HF' in self.params else
                       (len(self.params['SF']) if 'SF' in self.params else
                       (len(self.params['WF']) if 'WF' in self.params else len(self.params['QF']))))
         if 'DD' in self.params: self.record_delete_directory(None, self.params['DD'])
         if 'HF' in self.params:
            self.delete_help_files()
         elif 'SF' in self.params:
            self.delete_saved_files()
         elif 'WF' in self.params:
            self.delete_web_files()
         else:
            self.delete_backup_files()
         if 'DD' in self.params: self.clean_delete_directory(self.PGOPT['wrnlog'])
      elif self.PGOPT['ACTS'] == self.OPTS['GA'][0]:
         if 'ON' in self.params: del self.params['ON'] # use default order string
         if 'RG' in self.params: del self.params['RG'] # not recursively for sub groups
         if 'FN' not in self.params: self.params['FN'] = 'ALL'
         # get dataset info first
         if not self.get_dataset_info(): return
         # get group info second
         self.cache_group_info(0)
         self.get_group_info()
         # get help file info
         self.get_helpfile_info()
         # get saved file info
         self.get_savedfile_info()
         # get web file info
         self.get_webfile_info()
         # get backup file info
         self.get_backfile_info()
      elif self.PGOPT['ACTS'] == self.OPTS['GD'][0]:
         self.get_dataset_info()
      elif self.PGOPT['ACTS'] == self.OPTS['GG'][0]:
         self.get_group_info()
      elif self.PGOPT['ACTS'] == self.OPTS['GH'][0]:
         self.get_helpfile_info()
      elif self.PGOPT['ACTS'] == self.OPTS['GQ'][0]:
         self.get_backfile_info()
      elif self.PGOPT['ACTS'] == self.OPTS['GS'][0]:
         self.cache_group_info(0)
         self.get_savedfile_info()
      elif self.PGOPT['ACTS'] == self.OPTS['GW'][0]:
         self.cache_group_info(0)
         self.get_webfile_info()
      elif self.PGOPT['ACTS'] == self.OPTS['GV'][0]:
         self.get_version_info()
      elif self.PGOPT['ACTS'] == self.OPTS['MV'][0]:
         if 'DD' not in self.params: self.params['DD'] = -1
         self.ALLCNT = (len(self.params['HF']) if 'HF' in self.params else
                   (len(self.params['SF']) if 'SF' in self.params else
                    (len(self.params['WF']) if 'WF' in self.params else
                     (len(self.params['QF']) if 'QF' in self.params else 0))))
         if 'DD' in self.params: self.record_delete_directory(None, self.params['DD'])
         if 'HF' in self.params:
            self.move_help_files()
         elif 'TS' in self.params:
            self.web_to_saved_files()
         elif 'TW' in self.params:
            self.saved_to_web_files()
         elif 'SF' in self.params:
            self.move_saved_files()
         elif 'WF' in self.params:
            self.move_web_files()
         else:
            self.move_backup_files()
         if 'DD' in self.params: self.clean_delete_directory(self.PGOPT['wrnlog'])
      elif self.PGOPT['ACTS'] == self.OPTS['RQ'][0]:
         if 'WF' in self.params or 'SF' in self.params:
            self.build_backup_filelist()
         self.ALLCNT = len(self.params['QF'])
         self.retrieve_backup_files()
         if 'WF' in self.params:
            self.ALLCNT = len(self.params['WF'])
            self.restore_backup_webfiles()
         if 'SF' in self.params:
            self.ALLCNT = len(self.params['SF'])
            self.restore_backup_savedfiles()
      elif self.PGOPT['ACTS'] == self.OPTS['SA'][0]:
         if 'IF' not in self.params:
            self.action_error("Missing input file via Option -IF")
         if self.get_input_info(self.params['IF'], "DATASET"):
            self.check_enough_options('SD', self.OPTS['SD'][0])
            self.set_dataset_info()
         if self.get_input_info(self.params['IF'], "DSGROUP") and 'GI' in self.params:
            self.check_enough_options('SG', self.OPTS['SG'][0])
            self.ALLCNT = len(self.params['GI'])
            self.set_group_info()
         self.params['RO'] = 1
         if self.get_input_info(self.params['IF'], "HELPFILE") and 'HF' in self.params:
            self.check_enough_options('SH', self.OPTS['SH'][0])
            self.ALLCNT = len(self.params['HF'])
            self.cache_group_info(self.ALLCNT)
            self.get_next_disp_order()   # in case not empty
            self.set_helpfile_info()
         if self.get_input_info(self.params['IF'], "SAVEDFILE") and 'SF' in self.params:
            self.check_enough_options('SS', self.OPTS['SS'][0])
            self.ALLCNT = len(self.params['SF'])
            self.cache_group_info(self.ALLCNT)
            self.get_next_disp_order()   # in case not empty
            self.set_savedfile_info()
         if self.get_input_info(self.params['IF'], "WEBFILE") and 'WF' in self.params:
            self.check_enough_options('SW', self.OPTS['SW'][0])
            self.ALLCNT = len(self.params['WF'])
            self.cache_group_info(self.ALLCNT)
            self.get_next_disp_order()   # in case not empty
            self.set_webfile_info()
         if self.get_input_info(self.params['IF'], "BACKFILE") and 'QF' in self.params:
            self.check_enough_options('SQ', self.OPTS['SQ'][0])
            self.ALLCNT = len(self.params['QF'])
            self.get_next_disp_order()   # in case not empty
            self.set_backfile_info()
      elif self.PGOPT['ACTS'] == self.OPTS['SD'][0]:
         setds = 2
      elif self.PGOPT['ACTS'] == self.OPTS['SG'][0]:
         self.ALLCNT = len(self.params['GI'])
         if 'WN' in self.params or 'WM' in self.params:
            for gindex in self.params['GI']:
               self.CHGGRPS[gindex] = 1
         else:
            self.set_group_info()
      elif self.PGOPT['ACTS'] == self.OPTS['SH'][0]:
         if 'HF' in self.params:
            self.ALLCNT = len(self.params['HF'])
            self.set_helpfile_info()
         elif 'ON' in self.params:
            self.reorder_filelist('hfile')
      elif self.PGOPT['ACTS'] == self.OPTS['SQ'][0]:
         if 'QF' in self.params:
            self.ALLCNT = len(self.params['QF'])
            self.set_backfile_info()
         elif 'ON' in self.params:
            self.reorder_filelist('bfile')
      elif self.PGOPT['ACTS'] == self.OPTS['SS'][0]:
         if 'SF' in self.params:
            self.ALLCNT = len(self.params['SF'])
            self.cache_group_info(self.ALLCNT)
            self.set_savedfile_info()
         elif 'ON' in self.params:
            self.reorder_filelist('sfile')
         if 'RD' in self.params: self.clean_dataset_directory(1)
         if 'WM' in self.params: self.params['WM'] = 8
         if 'WN' in self.params: self.params['WN'] = 8
         if 'RT' in self.params: self.params['RT'] = 8
      elif self.PGOPT['ACTS'] == self.OPTS['SW'][0]:
         if 'WF' in self.params:
            self.ALLCNT = len(self.params['WF'])
            self.cache_group_info(self.ALLCNT)
            self.set_webfile_info()
         elif 'ON' in self.params:
            self.reorder_filelist('wfile')
         if 'RD' in self.params: self.clean_dataset_directory(0)
         if 'WM' in self.params: self.params['WM'] = 4
         if 'WN' in self.params: self.params['WN'] = 4
         if 'RT' in self.params: self.params['RT'] = 4
      elif self.PGOPT['ACTS'] == self.OPTS['SV'][0]:
         self.ALLCNT = len(self.params['VI'])
         self.set_version_info()
      elif self.PGOPT['ACTS'] == self.OPTS['TV'][0]:
         self.ALLCNT = len(self.params['VI'])
         self.terminate_version_info()
      elif self.PGOPT['ACTS'] == self.OPTS['UC'][0]:
         self.reset_rdadb_version(dsid)
      if not (setds or self.PGOPT['CACT'] == 'SA'):
         if('BD' in self.params or 'ED' in self.params or
            'PS' in self.params or 'BT' in self.params or 'ET' in self.params):
            setds = 1
      if setds:
         # dataset info needs to be updated
         self.set_dataset_info("P" if setds == 1 else None)
      if self.OPTS[self.PGOPT['CACT']][2] > 0:
         if 'WN' in self.params:
            # reset dataset/group file counts
            self.pglog("Reset file counts for of {} ...".format(dsid), self.WARNLG)
            cnt = self.reset_group_filenumber(dsid, self.params['WN'])
            s = 's' if cnt > 1 else ''
            if cnt > 0: self.reset_rdadb_version(dsid)
            self.pglog("{} Dataset/Group Record{} set for file counts".format(cnt, s), self.WARNLG)
         if 'WM' in self.params:
            # reset dataset/group metadata
            self.pglog("Reset Dataset/Group metadata for {} ...".format(dsid), self.WARNLG)
            self.reset_group_metadata(dsid, self.params['WM'])
         if 'RT' in self.params:
            # reset top group index
            self.pglog("Reset top group indices for of dsid ...", self.WARNLG)
            cnt = self.reset_top_group_index(dsid, self.params['RT'])
            s = 's' if cnt > 1 else ''
            if cnt > 0: self.reset_rdadb_version(dsid)
            self.pglog("{} file Record{} set for top group index".format(cnt), self.WARNLG)
      if self.RETSTAT:
         errmsg = "Action {} for {} finished, but unsuccessfully".format(self.PGOPT['CACT'], dsid)
         self.reset_errmsg(-1)
         if self.PGLOG['DSCHECK']: self.record_dscheck_error(errmsg)
         self.pglog(errmsg, self.LGEREX)
      else:
         if self.PGLOG['DSCHECK']: self.record_dscheck_status("D")
         if self.OPTS[self.PGOPT['CACT']][2]:
            if 'EM' in self.params:
               self.reset_errmsg(-1)
               self.set_email("Action {} for {} finished".format(self.PGOPT['CACT'], dsid), self.EMLTOP)
               self.cmdlog(None, 0, self.LOGWRN|self.SNDEML)
            else:
               self.cmdlog()
      self.pgexit(0)

   # archive web/object files
   def archive_web_files(self):
      tname = 'wfile'
      dftloc = None
      dsid = self.params['DS']
      bucket = self.PGLOG['OBJCTBKT']    # default object store bucket
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      dslocflags = set()
      dflags = {}
      self.pglog("Archive {} Web file{} of {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      if 'ZD' in self.params or 'UZ' in self.params:
         self.compress_localfile_list(self.PGOPT['CACT'], self.ALLCNT)
      if 'QF' in self.params: self.params['QF'] = self.get_bid_numbers(self.params['QF'])
      self.validate_multiple_values(tname, self.ALLCNT)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} file{} processed for Web archive".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      if 'WT' not in self.params:
         self.params['WT'] = ['D']*self.ALLCNT
         self.OPTS['WT'][2] |= 2
      if 'WF' not in self.params:
         self.params['WF'] = [None]*self.ALLCNT
         self.OPTS['WF'][2] |= 2
      if 'AF' not in self.params:
         self.params['AF'] = [None]*self.ALLCNT
         self.OPTS['AF'][2] |= 2
      if 'LC' not in self.params:
         self.params['LC'] = [None]*self.ALLCNT
         self.OPTS['LC'][2] |= 2
      if 'SZ' not in self.params:
         self.params['SZ'] = [0]*self.ALLCNT
         self.OPTS['SZ'][2] |= 2
      if 'MC' not in self.params: self.params['MC'] = [None]*self.ALLCNT
      reorder = errcnt = metatotal = metacnt = self.ADDCNT = self.MODCNT = bgcnt = acnt = ocnt = 0
      perrcnt = self.ALLCNT
      efiles = [1]*self.ALLCNT
      if self.PGSIG['BPROC'] > 1:
         afiles = [None]*self.ALLCNT
         lfiles = [None]*self.ALLCNT
         ofiles = [None]*self.ALLCNT
      fnames = None
      override = self.OVERRIDE
      if not override and 'GF' in self.params: override = self.OVRIDE
      while True:
         for i in range(bidx, self.ALLCNT):
            if self.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
               if metacnt >= self.PGOPT['RSMAX']:
                  metatotal += self.process_metadata("W", metacnt, self.PGOPT['emerol'])
                  metacnt = 0
               if self.PGLOG['DSCHECK'] and metacnt == 0:
                  self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
               if 'EM' in self.params:
                  self.PGLOG['PRGMSG'] = "{}/{} of {} web file{} archived/processed".format(acnt, i, self.ALLCNT, s)
            lfile = locfile = self.params['LF'][i]
            if not (efiles[i] and locfile): continue
            efiles[i] = 0
            if not self.params['AF'][i]:
               self.params['AF'][i] = self.local_archive_format(lfile)
            lsize = self.local_file_size(lfile, 6, self.PGOPT['emerol'])
            if lsize <= 0:
               if lsize == -2:
                  errcnt += 1
                  efiles[i] = 1
               else:
                  self.params['LF'][i] = self.params['WF'][i] = None
               continue
            locflag = self.params['LC'][i]
            if not locflag or locflag == 'R':
               if not dftloc: dftloc = self.get_dataset_locflag(dsid)
               locflag = self.params['LC'][i] = dftloc
            if locflag == 'C': self.pglog(lfile + ": Cannot Archive Web File for CGD data", self.PGOPT['extlog'])
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
            type = self.params['WT'][i]
            if not self.params['MC'][i]: self.params['MC'][i] = self.get_md5sum(locfile)  # re-get MD5 Checksum
            if not (self.params['SZ'][i] and self.params['SZ'][i] == lsize):
               self.params['SZ'][i] = lsize
            if not self.params['WF'][i]: self.params['WF'][i] = self.get_archive_filename(lfile)
            afile = (self.get_web_path(i, self.params['WF'][i], 1, type) if warch else None)
            wfile = self.get_web_path(i, self.params['WF'][i], 0, type)
            ofile = self.get_object_path(wfile, dsid) if oarch else None
            pgrec = self.pgget_wfile(dsid, "*", "wfile = '{}'".format(wfile), self.PGOPT['extlog'])
            if pgrec and self.params['WF'][i] != wfile: self.params['WF'][i] = wfile
            if not re.search(r'^/', locfile): locfile = self.join_paths(self.PGLOG['CURDIR'], locfile)
            winfo = "{}-{}-{}".format(dsid, type, wfile)
            self.pglog("{}: Archive Web file from {} ...".format(wfile, locfile),  self.WARNLG)
            if warch and locfile == afile: warch = 0
            vsnctl = 1 if pgrec and pgrec['vindex'] and pgrec['data_size'] else 0
            chksum = self.params['MC'][i]
            if warch and (vsnctl or not override):
               info = self.check_local_file(afile, 1, self.PGOPT['emerol']|self.PFSIZE)
               if info:
                  if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                     self.pglog("Web-{}: Same-Checksum ARCHIVED at {}:{}".format(winfo, info['date_modified'], info['time_modified']), self.PGOPT['emllog'])
                     warch = 0
                  elif info['data_size'] == lsize:
                     self.pglog("Web-{}: Same-Size ARCHIVED at {}:{}".format(winfo, info['date_modified'], info['time_modified']), self.PGOPT['wrnlog'])
                     warch = 0
                  elif vsnctl and not override:
                     self.pglog("Web-{}: Cannot rearchive version controlled file".format(winfo), self.PGOPT['extlog'])
                     self.params['WF'][i] = None
                     continue
               elif info is not None:
                  errcnt += 1
                  efiles[i] = 1
                  dflags['G'] = self.PGLOG['DSDHOME']
                  continue
            if oarch:
               replace = 0
               info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])
               if info:
                  if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                     self.pglog("Object-{}-{}: Same-Checksum ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), self.PGOPT['emllog'])
                     oarch = 0
                  elif info['data_size'] == lsize:
                     self.pglog("Object-{}-{}: Same-Size ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), self.PGOPT['wrnlog'])
                     oarch = 0
                  elif vsnctl and not override:
                     self.pglog("Object-{}-{}: Cannot rearchive version controlled file".format(bucket, ofile), self.PGOPT['extlog'])
                     self.params['WF'][i] = None
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
                  if not self.local_copy_local(afile, lfile, self.PGOPT['emerol']|override):
                     errcnt += 1
                     efiles[i] = 1
                     dflags['G'] = self.PGLOG['DSDHOME']
                     continue
                  acnt += 1
               if oarch:
                  if replace: self.delete_object_file(ofile, bucket)
                  if not self.local_copy_object(ofile, lfile, bucket, None, self.PGOPT['emerol']|override):
                     errcnt += 1
                     efiles[i] = 1
                     dflags['O'] = bucket
                     continue
                  ocnt += 1
               if self.PGLOG['DSCHECK']: chksize += lsize
               if self.PGSIG['BPROC'] > 1:
                  afiles[i] = afile
                  lfiles[i] = lfile
                  ofiles[i] = ofile
                  bgcnt += 1
            if self.PGSIG['BPROC'] < 2:
               if not fnames: fnames = self.get_field_keys(tname, None, "G")  # get setting fields if not yet
               info = None
               if warch:
                  info = self.check_local_file(afile, 1, self.PGOPT['emerol']|self.PFSIZE)
               elif oarch:
                  info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])
               elif pgrec:
                  info = self.get_file_origin_info(wfile, pgrec)
               elif locflag == 'O':
                  info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])               
               wid = self.set_one_webfile(i, pgrec, wfile, fnames, type, info)
               if not wid:
                  self.params['LF'][i] = self.params['WF'][i] = None
                  continue
               if 'GX' in self.params and self.PGOPT['GXTYP'].find(type) > -1:
                  metacnt += self.record_meta_gather('W', dsid, wfile, self.params['DF'][i])
                  self.cache_meta_tindex(dsid, wid, 'W')
               if pgrec and pgrec['meta_link'] and pgrec['meta_link'] != 'N':
                  if 'DX' in self.params or self.PGOPT['GXTYP'].find(type) < 0 and self.PGOPT['GXTYP'].find(pgrec['type']) > -1:
                     metacnt += self.record_meta_delete('W', dsid, wfile)
                  elif 'GI' in self.params:
                     gindex = self.params['GI'][i]
                     if gindex != pgrec['gindex'] and (gindex or (self.OPTS['GI'][2]&2) == 0):
                        metacnt += self.record_meta_summary('W', dsid, gindex, pgrec['gindex'])
         if errcnt == 0 or errcnt >= perrcnt or self.PGLOG['DSCHECK']: break
         perrcnt = errcnt
         self.pglog("Rearchive failed {}/{} Web file{} for {}".format(errcnt, self.ALLCNT. s, dsid), self.PGOPT['emllog'])
         errcnt = self.reset_errmsg(0)
      if errcnt:
         if 'CL' in self.params: del self.params['CL']
         self.RETSTAT = self.reset_errmsg(errcnt)
         for i in range(bidx, self.ALLCNT):
            if efiles[i]: self.params['WF'][i] = ''
         if self.PGLOG['DSCHECK']:
            self.check_storage_dflags(dflags, self.PGLOG['DSCHECK'], self.PGOPT['emerol'])
      if bgcnt:
         self.check_background(None, 0, self.LOGWRN, 1)
         for i in range(self.ALLCNT):
            if afiles[i]:
               self.validate_gladearch(afiles[i], lfiles[i], i)
            if ofiles[i]:
               self.validate_objectarch(ofiles[i], lfiles[i], bucket, i)
      if acnt > 0:
         self.pglog("{} of {} Web file{} archived for {}".format(acnt, self.ALLCNT, s, self.params['DS']), self.PGOPT['emllog'])
      if ocnt > 0:
         self.pglog("{} of {} Object file{} archived for {}".format(ocnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if metacnt > 0:
         metatotal += self.process_metadata('W', metacnt, self.PGOPT['emerol'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (metatotal + self.ADDCNT + self.MODCNT + reorder) > 0:
         self.pglog("{}/{} of {} Web file record{} added/modified for {}!".format(self.ADDCNT, self.MODCNT, self.ALLCNT , s, dsid), self.PGOPT['emllog'])
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""
      if dslocflags: self.set_dataset_locflag(dsid, dslocflags.pop())

   # archive help files
   def archive_help_files(self):
      tname = 'hfile'
      dsid = self.params['DS']
      bucket = self.PGLOG['OBJCTBKT']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      dflags = {}
      self.pglog("Archive {} Help file{} of {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      self.validate_multiple_values(tname, self.ALLCNT)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} file{} processed for Help archive".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      if 'HT' not in self.params:
         self.params['HT'] = ['D']*self.ALLCNT
         self.OPTS['HT'][2] |= 2
      if 'HF' not in self.params:
         self.params['HF'] = [None]*self.ALLCNT
         self.OPTS['HF'][2] |= 2
      if 'AF' not in self.params:
         self.params['AF'] = [None]*self.ALLCNT
         self.OPTS['AF'][2] |= 2
      if 'LC' not in self.params:
         self.params['LC'] = [None]*self.ALLCNT
         self.OPTS['LC'][2] |= 2
      if 'SZ' not in self.params:
         self.params['SZ'] = [0]*self.ALLCNT
         self.OPTS['SZ'][2] |= 2
      if 'MC' not in self.params: self.params['MC'] = [None]*self.ALLCNT
      reorder = errcnt = self.ADDCNT = self.MODCNT = bgcnt = acnt = ocnt = 0
      perrcnt = self.ALLCNT
      efiles = [1]*self.ALLCNT
      if self.PGSIG['BPROC'] > 1:
         afiles = [None]*self.ALLCNT
         lfiles = [None]*self.ALLCNT
         ofiles = [None]*self.ALLCNT
      override = self.OVERRIDE
      fnames = None
      while True:
         for i in range(bidx, self.ALLCNT):
            if self.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
               if self.PGLOG['DSCHECK']:
                  self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
               if 'EM' in self.params:
                  self.PGLOG['PRGMSG'] = "{}/{} of {} help file{} archived/processed".format(acnt, i, self.ALLCNT, s)
            lfile = locfile = self.params['LF'][i]
            if not (efiles[i] and locfile): continue
            efiles[i] = 0
            if not self.params['AF'][i]:
               self.params['AF'][i] = self.local_archive_format(lfile)
            locflag = (self.params['LC'][i] if self.params['LC'][i] else '')
            if not locflag: locflag = self.params['LC'][i] = 'B'
            url = self.params['WU'][i] if 'WU' in self.params and self.params['WU'][i] else None
            if locflag == 'R' or url:
               self.pglog("{}: Set Help file on {} via Action -SH".format(lfile, (url if url else 'URL')), self.PGOPT['errlog'])
               self.params['LF'][i] = self.params['HF'][i] = None
               continue
            lsize = self.local_file_size(locfile, 6, self.PGOPT['emerol'])
            if lsize <= 0:
               if lsize == -2:
                  errcnt += 1
                  efiles[i] = 1
               else:
                  self.params['LF'][i] = self.params['HF'][i] = None
               continue
            oarch = harch = 1
            if locflag == 'O':
               harch = 0
            elif locflag == 'G':
               oarch = 0
            type = self.params['HT'][i]
            stype = self.HTYPE[type] if type in self.HTYPE else 'Help'
            hpath = self.HPATH[type] if type in self.HPATH else 'help'
            if not self.params['MC'][i]: self.params['MC'][i] = self.get_md5sum(locfile)  # re-get MD5 Checksum
            if not (self.params['SZ'][i] and self.params['SZ'][i] == lsize):
               self.params['SZ'][i] = lsize
            if not self.params['HF'][i]: self.params['HF'][i] = self.get_archive_filename(lfile)
            hfile = self.params['HF'][i]
            afile = self.get_help_path(i, hfile, 1, type)
            ofile = self.get_object_path(hfile, dsid, hpath) if oarch else None
            typstr = "type = '{}'".format(type)
            pgrec = self.pgget(tname, "*", "hfile = '{}' AND {} AND {}".format(hfile, typstr, dcnd), self.PGOPT['extlog'])
            if pgrec and pgrec['locflag'] == 'R':
               url = pgrec['url']
               self.pglog("{}: Reset existing Help file on {} via Action -SH".format(lfile, (url if url else 'URL')), self.PGOPT['errlog'])
               self.params['LF'][i] = self.params['HF'][i] = None
               continue
            if not re.search(r'^/', locfile): locfile = self.join_paths(self.PGLOG['CURDIR'], locfile)
            hinfo = "{}-{}-{}".format(dsid, stype, hfile)
            self.pglog("{}: Archive Help file from {} ...".format(hfile, locfile),  self.WARNLG)
            if harch and locfile == afile: harch = 0
            chksum = self.params['MC'][i]
            if harch and not self.OVERRIDE:
               info = self.check_local_file(afile, 1, self.PGOPT['emerol']|self.PFSIZE)
               if info:
                  if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                     self.pglog("Help-{}: Same-Checksum ARCHIVED at {}:{}".format(hinfo, info['date_modified'], info['time_modified']), self.PGOPT['emllog'])
                     harch = 0
                  elif info['data_size'] == lsize:
                     self.pglog("Help-{}: Same-Size ARCHIVED at {}:{}".format(hinfo, info['date_modified'], info['time_modified']), self.PGOPT['wrnlog'])
                     harch = 0
               elif info is not None:
                  errcnt += 1
                  efiles[i] = 1
                  continue
            if oarch:
               replace = 0
               info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])
               if info:
                  if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                     self.pglog("Object-{}-{}: Same-Checksum ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), self.PGOPT['emllog'])
                     oarch = 0
                  elif info['data_size'] == lsize:
                     self.pglog("Object-{}-{}: Same-Size ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), self.PGOPT['wrnlog'])
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
                  if not self.local_copy_local(afile, lfile, self.PGOPT['emerol']|override):
                     errcnt += 1
                     efiles[i] = 1
                     continue
                  acnt += 1
               if oarch:
                  if replace: self.delete_object_file(ofile, bucket)
                  if not self.local_copy_object(ofile, lfile, bucket, None, self.PGOPT['emerol']|override):
                     errcnt += 1
                     efiles[i] = 1
                     dflags['O'] = bucket
                     continue
                  ocnt += 1
               if self.PGLOG['DSCHECK']: chksize += lsize
               if self.PGSIG['BPROC'] > 1:
                  afiles[i] = afile
                  lfiles[i] = lfile
                  ofiles[i] = ofile
                  bgcnt += 1
            if self.PGSIG['BPROC'] < 2:
               if not fnames: fnames = self.get_field_keys(tname, None, "G")  # get setting fields if not yet
               info = None
               if harch:
                  info = self.check_local_file(afile, 1, self.PGOPT['emerol']|self.PFSIZE)
               elif oarch:
                  info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])
               elif pgrec:
                  info = self.get_file_origin_info(hfile, pgrec)
               elif locflag == 'O':
                  info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])
               hid = self.set_one_helpfile(i, pgrec, hfile, fnames, type, info)
               if not hid:
                  self.params['LF'][i] = self.params['WF'][i] = None
                  continue
         if errcnt == 0 or errcnt >= perrcnt or self.PGLOG['DSCHECK']: break
         perrcnt = errcnt
         self.pglog("Rearchive failed {}/{} Help file{} for {}".format(errcnt, self.ALLCNT. s, dsid), self.PGOPT['emllog'])
         errcnt = self.reset_errmsg(0)
      if errcnt:
         if 'CL' in self.params: del self.params['CL']
         self.RETSTAT = self.reset_errmsg(errcnt)
         for i in range(bidx, self.ALLCNT):
            if efiles[i]: self.params['HF'][i] = ''
         if self.PGLOG['DSCHECK']:
            self.check_storage_dflags(dflags, self.PGLOG['DSCHECK'], self.PGOPT['emerol'])
      if bgcnt:
         self.check_background(None, 0, self.LOGWRN, 1)
         for i in range(self.ALLCNT):
            if afiles[i]:
               self.validate_gladearch(afiles[i], lfiles[i], i)
            if ofiles[i]:
               self.validate_objectarch(ofiles[i], lfiles[i], bucket, i)
      if acnt > 0:
         self.pglog("{} of {} Help file{} archived for {}".format(acnt, self.ALLCNT, s, self.params['DS']), self.PGOPT['emllog'])
      if ocnt > 0:
         self.pglog("{} of {} Object file{} archived for {}".format(ocnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.ADDCNT + self.MODCNT + reorder) > 0:
         self.pglog("{}/{} of {} Help file record{} added/modified for {}!".format(self.ADDCNT, self.MODCNT, self.ALLCNT , s, dsid), self.PGOPT['emllog'])
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # archive save files
   def archive_saved_files(self):
      tname = 'sfile'
      dftloc = None
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      dflags = {}
      bucket = "gdex-decsdata"    # object store bucket 
      self.pglog("Archive {} Saved file{} of {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      if 'ZD' in self.params or 'UZ' in self.params:
         self.compress_localfile_list(self.PGOPT['CACT'], self.ALLCNT)
      if 'QF' in self.params: self.params['QF'] = self.get_bid_numbers(self.params['QF'])
      self.validate_multiple_values(tname, self.ALLCNT)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} file{} processed for SAVED archive".format(bidx, self.ALLCNT , s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      if 'SF' not in self.params:
         self.params['SF'] = [None]*self.ALLCNT
         self.OPTS['SF'][2] |= 2
      if 'AF' not in self.params:
         self.params['AF'] = [None]*self.ALLCNT
         self.OPTS['AF'][2] |= 2
      if 'LC' not in self.params:
         self.params['LC'] = [None]*self.ALLCNT
         self.OPTS['LC'][2] |= 2
      if 'SZ' not in self.params:
         self.params['SZ'] = [0]*self.ALLCNT
         self.OPTS['SZ'][2] |= 2
      if 'MC' not in self.params: self.params['MC'] = [None]*self.ALLCNT
      reorder = errcnt = self.ADDCNT = self.MODCNT = bgcnt = acnt = ocnt = 0
      perrcnt = self.ALLCNT
      efiles = [1]*self.ALLCNT
      if self.PGSIG['BPROC'] > 1:
         afiles = [None]*self.ALLCNT
         ofiles = [None]*self.ALLCNT
         lfiles = [None]*self.ALLCNT
      fnames = None
      override = self.OVERRIDE
      if not override and 'GF' in self.params: override = self.OVRIDE
      while True:
         for i in range(bidx, self.ALLCNT):
            if self.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
               if self.PGLOG['DSCHECK']:
                  self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
               if 'EM' in self.params:
                  self.PGLOG['PRGMSG'] = "{}/{} of {} saved file{} archived/processed".format(acnt, i, self.ALLCNT, s)
            lfile = locfile = self.params['LF'][i]
            if not (efiles[i] and locfile): continue
            efiles[i] = 0
            if not self.params['AF'][i]:
               self.params['AF'][i] = self.local_archive_format(lfile)
            lsize = self.local_file_size(lfile, 6, self.PGOPT['emerol'])
            if lsize <= 0:
               if lsize == -2:
                  errcnt += 1
                  efiles[i] = 1
               else:
                  self.params['LF'][i] = self.params['SF'][i] = None
               continue
            locflag = self.params['LC'][i]
            if not locflag or locflag == 'R':
               if not dftloc: dftloc = self.get_dataset_locflag(dsid)
               locflag = self.params['LC'][i] = dftloc
            if locflag == 'C': self.pglog(lfile + ": Cannot Archive Saved File for CGD data", self.PGOPT['extlog'])
            if locflag == 'B':
               oarch = sarch = 1
            elif locflag == 'O':
               oarch = 1
               sarch = 0
            else:
               oarch = 0
               sarch = 1
            if oarch: self.pglog(lfile + ": Cannot Archive Saved File onto Boreas", self.PGOPT['extlog'])
            if not self.params['MC'][i]: self.params['MC'][i] = self.get_md5sum(lfile)  # re-get MD5 Checksum
            if not (self.params['SZ'][i] and self.params['SZ'][i] == lsize):
               self.params['SZ'][i] = lsize
            if self.params['SF'][i]:
               sfile = self.params['SF'][i]
            else:
               sfile = self.params['SF'][i] = self.get_archive_filename(lfile)
            if 'ST' in self.params and self.params['ST'][i]:
               type = self.params['ST'][i]
               if self.PGOPT['SDTYP'].find(type) < 0:
                  self.pglog("{}-{}: Invalid Saved file Type '{}' to Archive".format(dsid, sfile, s), self.PGOPT['extlog'])
                  continue
            else:
               self.pglog("{}-{}: Miss Saved file Type to Archive".format(dsid, sfile), self.PGOPT['extlog'])
               continue
            afile = (self.get_saved_path(i, sfile, 1, type) if sarch else None)
            sfile = self.get_saved_path(i, sfile, 0, type)
            ofile = (self.join_paths(dsid, sfile) if oarch else None)
            pgrec = self.pgget(tname, "*", "{} AND sfile = '{}' AND type = '{}'".format(dcnd, sfile, type), self.PGOPT['extlog'])
            if not pgrec:
               pgrec = self.pgget(tname, "type", "{} AND sfile = '{}'".format(dcnd, sfile), self.PGOPT['extlog'])
               if pgrec:
                  self.pglog("{}-{}: Fail to archive, Move Action -MV to change file Type from '{}' to '{}'".format(dsid, sfile, pgrec['type'], type), self.PGOPT['emlerr'])
                  continue
            if pgrec and self.params['SF'][i] != sfile: self.params['SF'][i] = sfile
            sinfo = "{}-{}-{}".format(dsid, type, sfile)
            if not re.match(r'^/', locfile): locfile = self.join_paths(self.PGLOG['CURDIR'], locfile)
            if sarch and locfile == afile: sarch = 0
            self.pglog("{}: Archive Saved file from {} ...".format(sinfo, locfile),  self.WARNLG)
            vsnctl = 1 if pgrec and pgrec['vindex'] and pgrec['data_size'] else 0
            chksum = self.params['MC'][i]
            if sarch and (vsnctl or not override):
               info = self.check_local_file(afile, 1, self.PGOPT['emerol']|self.PFSIZE)
               if info:
                  if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                     self.pglog("Saved-{}: Same-Checksum ARCHIVED at {}:{}".format(sinfo, info['date_modified'], info['time_modified']), self.PGOPT['emllog'])
                     sarch = 0
                  elif info['data_size'] == lsize:
                     self.pglog("Saved-{}: Same-Size ARCHIVED at {}:{}".format(sinfo, info['date_modified'], info['time_modified']), self.PGOPT['wrnlog'])
                     sarch = 0
                  elif vsnctl and not override:
                     self.pglog("Saved-{}: Cannot rearchive version controlled file".format(sinfo), self.PGOPT['extlog'])
                     self.params['SF'][i] = None
                     continue
               elif info is not None:
                  errcnt += 1
                  efiles[i] = 1
                  dflags['G'] = self.PGLOG['DECSHOME']
                  continue
            if oarch:
               replace = 0
               info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])
               if info:
                  if pgrec and chksum and pgrec['checksum'] and pgrec['checksum'] == chksum:
                     self.pglog("Object-{}-{}: Same-Checksum ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), self.PGOPT['emllog'])
                     oarch = 0
                  elif info['data_size'] == lsize:
                     self.pglog("Object-{}-{}: Same-Size ARCHIVED at {}:{}".format(bucket, ofile, info['date_modified'], info['time_modified']), self.PGOPT['wrnlog'])
                     oarch = 0
                  elif vsnctl and not override:
                     self.pglog("Object-{}-{}: Cannot rearchive version controlled file".format(bucket, ofile), self.PGOPT['extlog'])
                     self.params['SF'][i] = None
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
                  if not self.local_copy_local(afile, lfile, self.PGOPT['emerol']|override):
                     errcnt += 1
                     efiles[i] = 1
                     continue
                  acnt += 1
               if oarch:
                  if replace: self.delete_object_file(ofile, bucket)
                  if not self.local_copy_object(ofile, lfile, bucket, None, self.PGOPT['emerol']|override):
                     errcnt += 1
                     efiles[i] = 1
                     continue
                  ocnt += 1
               if self.PGLOG['DSCHECK']: chksize += lsize
               if self.PGSIG['BPROC'] > 1:
                  afiles[i] = afile
                  ofiles[i] = ofile
                  lfiles[i] = lfile
                  bgcnt += 1
            if self.PGSIG['BPROC'] < 2:
               if not fnames: fnames = self.get_field_keys(tname, None, "G")  # get setting fields if not yet
               info = None
               if sarch:
                  info = self.check_local_file(afile, 1, self.PGOPT['emerol']|self.PFSIZE)
               elif oarch:
                  info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])
               elif pgrec:
                  info = self.get_file_origin_info(sfile, pgrec)
               elif locflag == 'O':
                  info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])
               sid = self.set_one_savedfile(i, pgrec, sfile, fnames, type, info)
               if not sid:
                  self.params['LF'][i] = self.params['SF'][i] = None
                  continue
         if errcnt == 0 or errcnt >= perrcnt or self.PGLOG['DSCHECK']: break
         perrcnt = errcnt
         self.pglog("Rearchive failed {}/{} Saved file{} for {}!".format(errcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
         errcnt = self.reset_errmsg(0)
      if errcnt:
         if 'CL' in self.params: del self.params['CL']
         self.RETSTAT = self.reset_errmsg(errcnt)
         for i in range(bidx, self.ALLCNT):
            if efiles[i]: self.params['SF'][i] = ''
         if self.PGLOG['DSCHECK']:
            self.check_storage_dflags(dflags, self.PGLOG['DSCHECK'], self.PGOPT['emerol'])
      if bgcnt:
         self.check_background(None, 0, self.LOGWRN, 1)
         for i in range(self.ALLCNT):
            if afiles[i]: self.validate_gladearch(afiles[i], lfiles[i], i)
            if ofiles[i]: self.validate_objectarch(ofiles[i], lfiles[i], bucket, i)
      if acnt > 0:
         self.pglog("{} of {} Saved file{} archived for {}".format(acnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if ocnt > 0:
         self.pglog("{] of {} Object file{} archived for {}".format(ocnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      self.pglog("{}/{} of {} Saved file record{} added/modified for {}!".format(self.ADDCNT, self.MODCNT, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.ADDCNT + self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # cross copy web files between glade and object store
   def crosscopy_web_files(self, aname):
      tname = 'wfile'
      dsid = self.params['DS']
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      dflags = {}
      bucket = self.PGLOG['OBJCTBKT']  # object store bucket
      self.pglog("Cross {} {} Web file{} of {} ...".format(aname, self.ALLCNT, s, dsid), self.WARNLG)
      self.validate_multiple_values(tname, self.ALLCNT)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} file{} processed for Web cross copy".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      reorder = errcnt = metatotal = metacnt = self.MODCNT = bgcnt = acnt = ocnt = 0
      perrcnt = self.ALLCNT
      efiles = [1]*self.ALLCNT
      self.params['LC'] = ['B']*self.ALLCNT
      if self.PGSIG['BPROC'] > 1:
         afiles = [None]*self.ALLCNT
         ofiles = [None]*self.ALLCNT
         warchs = [None]*self.ALLCNT
      fnames = None
      while True:
         for i in range(bidx, self.ALLCNT):
            if self.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
               if self.PGLOG['DSCHECK']:
                  self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
               if metacnt >= self.PGOPT['RSMAX']:
                  metatotal += self.process_metadata("W", metacnt, self.PGOPT['emerol'])
                  metacnt = 0
               if 'EM' in self.params:
                  self.PGLOG['PRGMSG'] = "{}/{} of {} web file{} archived/processed".format(acnt, i, self.ALLCNT, s)
            wfile = self.params['WF'][i]
            if not (efiles[i] and wfile): continue
            efiles[i] = 0
            type = self.params['WT'][i] if 'WT' in self.params else 'D'
            wfile = self.get_web_path(i, wfile, 0, type)
            winfo = "{}-{}-{}".format(dsid, type, wfile)
            pgrec = self.pgget_wfile(dsid, "*", "wfile = '{}' AND type = '{}'".format(wfile, type), self.PGOPT['extlog'])
            if not pgrec:
               self.pglog("{}: Cannot Cross {} Web File not in RDADB".format(winfo, aname), self.PGOPT['errlog'])
               continue
            elif pgrec['locflag'] == 'C':
               self.pglog("{}: Cannot Cross {} Web File for CGD data".format(winfo, aname), self.PGOPT['extlog'])
            afile = self.get_web_path(i, wfile, 1, type)
            ofile = self.join_paths(dsid, wfile)
            warch = oarch = 1
            self.pglog(winfo + ": Cross {} Web file ...".format(aname),  self.WARNLG)
            info = self.check_local_file(afile, 0, self.PGOPT['emerol']|self.PFSIZE)
            if info:
               warch = 0
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['G'] = self.PGLOG['DSDHOME']
               continue
            info = self.check_object_file(ofile, bucket, 0, self.PGOPT['emerol'])
            if info:
               oarch = 0
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['O'] = bucket
               continue
            if warch and oarch:
               self.pglog(winfo + ": Cannot Cross {}, Neither Web Nor Object file Exists".format(aname), self.PGOPT['errlog'])
               continue
            elif (warch + oarch) == 0 and pgrec['locflag'] == 'B':
               self.pglog(winfo + ": No need Cross {}, Both Web & Object Exist".format(aname), self.PGOPT['wrnlog'])
               continue
            if warch:
               if not self.object_copy_local(afile, ofile, bucket, self.PGOPT['emerol']|self.OVERRIDE):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['G'] = self.PGLOG['DSDHOME']
                  continue
               if aname == 'Move':
                  self.params['LC'][i] = 'G'
                  self.delete_object_file(ofile, bucket, self.PGOPT['extlog'])
               acnt += 1
            elif oarch:
               if not self.local_copy_object(ofile, afile, bucket, None, self.PGOPT['emerol']|self.OVERRIDE):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['O'] = bucket
                  continue
               if aname == 'Move':
                  self.params['LC'][i] = 'O'
                  self.delete_local_file(afile, self.PGOPT['extlog'])
               ocnt += 1
            if self.PGSIG['BPROC'] > 1:
               afiles[i] = afile
               ofiles[i] = ofile
               warchs[i] = warch
               bgcnt += 1
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            if self.PGSIG['BPROC'] < 2:
               if not fnames: fnames = self.get_field_keys(tname, None, "G")  # get setting fields if not yet
               info = self.get_file_origin_info(wfile, pgrec)
               wid = self.set_one_webfile(i, pgrec, wfile, fnames, type, info)
               if not wid:
                  self.params['WF'][i] = None
                  continue
               if 'GX' in self.params and self.PGOPT['GXTYP'].find(type) > -1:
                  metacnt += self.record_meta_gather('W', dsid, wfile, self.params['DF'][i])
                  self.cache_meta_tindex(dsid, wid, 'W')
               if pgrec['meta_link'] and pgrec['meta_link'] != 'N':
                  if 'DX' in self.params or self.PGOPT['GXTYP'].find(type) < 0 and self.PGOPT['GXTYP'].find(pgrec['type']) > -1:
                     metacnt += self.record_meta_delete('W', dsid, wfile)
                  elif 'GI' in self.params:
                     gindex = self.params['GI'][i]
                     if gindex != pgrec['gindex'] and (gindex or (self.OPTS['GI'][2]&2) == 0):
                        metacnt += self.record_meta_summary('W', dsid, gindex, pgrec['gindex'])
         if errcnt == 0 or errcnt >= perrcnt or self.PGLOG['DSCHECK']: break
         perrcnt = errcnt
         self.pglog("Recopy failed {}/{} Web file{} for {}".format(errcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
         errcnt = self.reset_errmsg(0)
      if errcnt:
         self.RETSTAT = self.reset_errmsg(errcnt)
         for i in range(bidx, self.ALLCNT):
            if efiles[i]: self.params['WF'][i] = ''
         if self.PGLOG['DSCHECK']:
            self.check_storage_dflags(dflags, self.PGLOG['DSCHECK'], self.PGOPT['emerol'])
      if bgcnt:
         self.check_background(None, 0, self.LOGWRN, 1)
         for i in range(self.ALLCNT):
            if warchs[i]:
               self.validate_gladearch(afiles[i], "{}-{}".format(bucket, ofiles[i]), i)
            elif ofiles[i]:
               self.validate_objectarch(ofiles[i], afiles[i], bucket, i)
      astr = 'Moved' if aname == 'Move' else 'Copied'
      if acnt > 0: self.pglog("{} of {} Web file{} Cross {} for {}".format(acnt, self.ALLCNT, s, astr, dsid), self.PGOPT['emllog'])
      if ocnt > 0: self.pglog("{} of {} Object file{} Cross {} for {}".format(ocnt, self.ALLCNT, s, astr, dsid), self.PGOPT['emllog'])
      if metacnt > 0:
         metatotal += self.process_metadata('W', metacnt, self.PGOPT['emerol'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (metatotal + self.MODCNT + reorder) > 0:
         self.pglog("{} of {} Web file record{} modified for {}!".format(self.MODCNT, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # cross copy help files between glade and object store
   def crosscopy_help_files(self, aname):
      tname = 'hfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      dflags = {}
      bucket = self.PGLOG['OBJCTBKT']
      self.pglog("Cross {} {} Help file{} of {} ...".format(aname, self.ALLCNT, s, dsid), self.WARNLG)
      self.validate_multiple_values(tname, self.ALLCNT)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} file{} processed for HELP archive".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      reorder = errcnt = self.MODCNT = bgcnt = acnt = ocnt = 0
      perrcnt = self.ALLCNT
      efiles = [1]*self.ALLCNT
      self.params['LC'] = ['B']*self.ALLCNT
      if self.PGSIG['BPROC'] > 1:
         afiles = [None]*self.ALLCNT
         ofiles = [None]*self.ALLCNT
         harchs = [None]*self.ALLCNT
      fnames = None
      while True:
         for i in range(bidx, self.ALLCNT):
            if self.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
               if self.PGLOG['DSCHECK']:
                  self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
               if 'EM' in self.params:
                  self.PGLOG['PRGMSG'] = "{}/{} of {} help file{} archived/processed".format(acnt, i, self.ALLCNT, s)
            hfile = self.params['HF'][i]
            if not (efiles[i] and hfile): continue
            efiles[i] = 0
            if 'HT' in self.params and self.params['HT'][i]:
               type = self.params['HT'][i]
               if type not in self.HTYPE:
                  self.pglog("{}-{}: Invalid Help file Type '{}' to Archive".format(dsid, hfile, type), self.PGOPT['emerol'])
                  continue
            else:
               self.pglog("{}-{}: Miss Help file Type to Archive".format(dsid, hfile), self.PGOPT['errlog'])
               continue
            stype = self.HTYPE[type] if type in self.HTYPE else 'Help'
            hfile = self.get_help_path(i, hfile, 0, type)
            afile = self.get_help_path(i, hfile, 1, type)
            hpath = self.HPATH[type] if type in self.HPATH else 'help'
            ofile = self.get_object_path(hfile, dsid, hpath)
            hinfo = "{}-{}-{}".format(dsid, type, hfile)
            pgrec = self.pgget(tname, "*", "{} and hfile = '{}' AND type = '{}'".format(dcnd, hfile, type), self.PGOPT['extlog'])
            if not pgrec:
               self.pglog(hinfo + ": Fail to Cross {} for Help file not in RDADB".format(aname), self.PGOPT['emlerr'])
               continue
            if pgrec['locflag'] == 'R':
               url = pgrec['url']
               if not url: url = 'URL'
               self.pglog("{}: Cannot Cross {} Help File on {}".format(hinfo, aname, url), self.PGOPT['emlerr'])
               continue
            if pgrec and self.params['HF'][i] != hfile: self.params['HF'][i] = hfile
            harch = oarch = 1
            self.pglog(hinfo + ": Cross {} Help file ...".format(aname),  self.WARNLG)
            info = self.check_local_file(afile, 0, self.PGOPT['emerol']|self.PFSIZE)
            if info:
               harch = 0
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['G'] = self.PGLOG['DECSHOME']
               continue
            info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])
            if info:
               oarch = 0
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['O'] = bucket
               continue
            if harch and oarch:
               self.pglog(hinfo + ": Cannot Cross {} Help file, on Neither Glade Nor Object".format(aname), self.PGOPT['errlog'])
               continue
            elif not (harch or oarch) and pgrec['locflag'] == 'B':
               self.pglog(hinfo + ": No need Cross {} Help file, on Both Glade & Object".format(aname), self.PGOPT['wrnlog'])
               continue
            if harch:
               if not self.object_copy_local(afile, ofile, bucket, self.PGOPT['emerol']|self.OVERRIDE):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['G'] = self.PGLOG['DECSHOME']
                  continue
               if aname == 'Move':
                  self.params['LC'][i] = 'G'
                  self.delete_object_file(ofile, bucket, self.PGOPT['extlog'])
               acnt += 1
            elif oarch:
               if not self.local_copy_object(ofile, afile, bucket, None, self.PGOPT['emerol']|self.OVERRIDE):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['O'] = bucket
                  continue
               if aname == 'Move':
                  self.params['LC'][i] = 'O'
                  self.delete_local_file(afile, self.PGOPT['extlog'])
               ocnt += 1
            if self.PGSIG['BPROC'] > 1:
               afiles[i] = afile
               ofiles[i] = ofile
               harchs[i] = harch
               bgcnt += 1
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            if self.PGSIG['BPROC'] < 2:
               if not fnames: fnames = self.get_field_keys(tname, None, "G")  # get setting fields if not yet
               info = self.get_file_origin_info(hfile, pgrec)
               hid = self.set_one_helpfile(i, pgrec, hfile, fnames, type, info)
               if not hid:
                  self.params['HF'][i] = None
                  continue
         if errcnt == 0 or errcnt >= perrcnt or self.PGLOG['DSCHECK']: break
         perrcnt = errcnt
         self.pglog("Re{} {} Help file{} for {}".format(aname, errcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
         errcnt = self.reset_errmsg(0)
      if errcnt:
         self.RETSTAT = self.reset_errmsg(errcnt)
         for i in range(bidx, self.ALLCNT):
            if efiles[i]: self.params['SF'][i] = ''
         if self.PGLOG['DSCHECK']:
            self.check_storage_dflags(dflags, self.PGLOG['DSCHECK'], self.PGOPT['emerol'])
      if bgcnt:
         self.check_background(None, 0, self.LOGWRN, 1)
         for i in range(self.ALLCNT):
            if harchs[i]:
               self.validate_gladearch(afiles[i], "{}-{}".format(bucket, ofiles[i]), i)
            elif ofiles[i]:
               self.validate_objectarch(ofiles[i], afiles[i], bucket, i)
      astr = 'Moved' if aname == 'Move' else 'Copied'
      if acnt > 0: self.pglog("{} of {} Help file{} Cross {} for {}".format(acnt, self.ALLCNT, s, astr, dsid), self.PGOPT['emllog'])
      if ocnt > 0: self.pglog("{} of {} Object file{} Cross {} for {}".format(ocnt, self.ALLCNT, s, astr, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      self.pglog("{} of {} Help file record{} modified for {}!".format(self.MODCNT, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if 'ON' in self.params: reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # cross copy save files between glade and object store
   def crosscopy_saved_files(self, aname):
      tname = 'sfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      dflags = {}
      bucket = "gdex-decsdata"
      self.pglog("Cross {} {} Saved file{} of {} ...".format(aname, self.ALLCNT, s, dsid), self.WARNLG)
      self.validate_multiple_values(tname, self.ALLCNT)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} file{} processed for SAVED archive".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      reorder = errcnt = self.MODCNT = bgcnt = acnt = ocnt = 0
      perrcnt = self.ALLCNT
      efiles = [1]*self.ALLCNT
      self.params['LC'] = ['B']*self.ALLCNT
      if self.PGSIG['BPROC'] > 1:
         afiles = [None]*self.ALLCNT
         ofiles = [None]*self.ALLCNT
         sarchs = [None]*self.ALLCNT
      fnames = None
      while True:
         for i in range(bidx, self.ALLCNT):
            if self.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
               if self.PGLOG['DSCHECK']:
                  self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
               if 'EM' in self.params:
                  self.PGLOG['PRGMSG'] = "{}/{} of {} saved file{} archived/processed".format(acnt, i, self.ALLCNT, s)
            sfile = self.params['SF'][i]
            if not (efiles[i] and sfile): continue
            efiles[i] = 0
            if 'ST' in self.params and self.params['ST'][i]:
               type = self.params['ST'][i]
               if self.PGOPT['SDTYP'].find(type) < 0:
                  self.pglog("{}-{}: Invalid Saved file Type '{}' to Archive".format(dsid, sfile, type), self.PGOPT['emerol'])
                  continue
            else:
               self.pglog("{}-{}: Miss Saved file Type to Archive".format(dsid, sfile), self.PGOPT['errlog'])
               continue
            sfile = self.get_saved_path(i, sfile, 0, type)
            afile = self.get_saved_path(i, sfile, 1, type)
            ofile = self.join_paths(dsid, sfile)
            sinfo = "{}-{}-{}".format(dsid, type, sfile)
            pgrec = self.pgget(tname, "*", "{} and sfile = '{}' AND type = '{}'".format(dcnd, sfile, type), self.PGOPT['extlog'])
            if not pgrec:
               self.pglog("{}: Fail to Cross {} for Saved file not in RDADB".format(sinfo, aname), self.PGOPT['emlerr'])
               continue
            elif pgrec['locflag'] == 'C':
               self.pglog("{}: Fail to Cross {} Saved File for CGD data".format(sinfo, aname), self.PGOPT['extlog'])
            if pgrec and self.params['SF'][i] != sfile: self.params['SF'][i] = sfile
            sarch = oarch = 1
            self.pglog(sinfo + ": Cross {} Saved file ...".format(aname),  self.WARNLG)
            info = self.check_local_file(afile, 0, self.PGOPT['emerol']|self.PFSIZE)
            if info:
               sarch = 0
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['G'] = self.PGLOG['DECSHOME']
               continue
            info = self.check_object_file(ofile, bucket, 1, self.PGOPT['emerol'])
            if info:
               oarch = 0
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['O'] = bucket
               continue
            if sarch and oarch:
               self.pglog(sinfo + ": Cannot Cross {}, Neither Saved Nor Object file Exists".format(aname), self.PGOPT['errlog'])
               continue
            elif not (sarch or oarch) and pgrec['locflag'] == 'B':
               self.pglog(sinfo + ": No need Cross {}, Both Saved & Object Exist".format(aname), self.PGOPT['wrnlog'])
               continue
            if sarch:
               if not self.object_copy_local(afile, ofile, bucket, self.PGOPT['emerol']|self.OVERRIDE):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['G'] = self.PGLOG['DECSHOME']
                  continue
               if aname == 'Move':
                  self.params['LC'][i] = 'G'
                  self.delete_object_file(ofile, bucket, self.PGOPT['extlog'])
               acnt += 1
            elif oarch:
               if not self.local_copy_object(ofile, afile, bucket, None, self.PGOPT['emerol']|self.OVERRIDE):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['O'] = bucket
                  continue
               if aname == 'Move':
                  self.params['LC'][i] = 'O'
                  self.delete_local_file(afile, self.PGOPT['extlog'])
               ocnt += 1
            if self.PGSIG['BPROC'] > 1:
               afiles[i] = afile
               ofiles[i] = ofile
               sarchs[i] = sarch
               bgcnt += 1
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            if self.PGSIG['BPROC'] < 2:
               if not fnames: fnames = self.get_field_keys(tname, None, "G")  # get setting fields if not yet
               info = self.get_file_origin_info(sfile, pgrec)
               sid = self.set_one_savedfile(i, pgrec, sfile, fnames, type, info)
               if not sid:
                  self.params['SF'][i] = None
                  continue
         if errcnt == 0 or errcnt >= perrcnt or self.PGLOG['DSCHECK']: break
         perrcnt = errcnt
         self.pglog("Recopy {} Saved file{} for {}".format(errcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
         errcnt = self.reset_errmsg(0)
      if errcnt:
         self.RETSTAT = self.reset_errmsg(errcnt)
         for i in range(bidx, self.ALLCNT):
            if efiles[i]: self.params['SF'][i] = ''
         if self.PGLOG['DSCHECK']:
            self.check_storage_dflags(dflags, self.PGLOG['DSCHECK'], self.PGOPT['emerol'])
      if bgcnt:
         self.check_background(None, 0, self.LOGWRN, 1)
         for i in range(self.ALLCNT):
            if sarchs[i]:
               self.validate_gladearch(afiles[i], "{}-{}".format(bucket, ofiles[i]), i)
            elif ofiles[i]:
               self.validate_objectarch(ofiles[i], afiles[i], bucket, i)
      astr = 'Moved' if aname == 'Move' else 'Copied'
      if acnt > 0: self.pglog("{} of {} Saved file{} Cross {} for {}".format(acnt, self.ALLCNT, s, astr, dsid), self.PGOPT['emllog'])
      if ocnt > 0: self.pglog("{} of {} Object file{} Cross {} for {}".format(ocnt, self.ALLCNT, s, astr, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      self.pglog("{} of {} Saved file record{} modified for {}!".format(self.MODCNT, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if 'ON' in self.params: reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # get backup file names in RDADB and on Quaser server for given dataset id
   def get_backup_filenames(self, bfile, dsid):
      ms = re.match(r'^/{}/(.+)$'.format(dsid), bfile)
      if ms:
         qfile = bfile
         bfile = ms.group(1)
      else:
         qfile = "/{}/{}".format(dsid, bfile)
      return (bfile, qfile)

   # archive a backup file for given wfiles/sfiles
   def archive_backup_file(self):
      tname = 'bfile'
      endpoint = self.PGLOG['BACKUPEP']
      drpoint = self.PGLOG['DRDATAEP']
      dsid = self.params['DS']
      qtype = self.params['QT'][0]
      (bfile, qfile) = self.get_backup_filenames(self.params['QF'][0], dsid)
      dobackup = 0 if 'TO' in self.params else 1
      note = self.params['DE'][0] if 'DE' in self.params else None
      chkstat = False if re.search(r'_changed.tar$', bfile) else True
      pgbck = self.pgget(tname, '*', "dsid = '{}' AND bfile = '{}'".format(dsid, bfile), self.PGOPT['extlog'])
      if pgbck and not self.OVERRIDE and pgbck['checksum']:
         return self.pglog(bfile + ": file in RDADB, delete it or add Option -OE to backup again", self.PGOPT['extlog'])
      if dobackup and not self.OVERRIDE and self.check_backup_file(qfile, endpoint, 0, self.PGOPT['extlog']):
         return self.pglog(qfile +": Backup file on Quasar, add Option -OE to override", self.PGOPT['extlog'])
      endpath = 'decsdata' if 'SF' in self.params else 'data'
      fromfile = "/{}/{}/{}/{}".format(endpath, endpoint, dsid, op.basename(bfile))
      tarfile = self.PGLOG['DSSDATA'] + fromfile
      self.make_local_directory(op.dirname(tarfile), self.PGOPT['extlog'])
      if self.check_local_file(tarfile, 0, self.PGOPT['extlog']):
         if not self.OVERRIDE:
            return self.pglog(fromfile + ": exists for Quasar backup, add Option -OE to override", self.PGOPT['extlog'])
         self.delete_local_file(tarfile, self.PGOPT['extlog'])
      tfmt = 'TAR'
      ccnt = scnt = wcnt = 0
      ifcnt = len(self.params['IF']) if 'IF' in self.params else 0 
      if self.PGLOG['DSCHECK']:
         if 'SF' in self.params:
            ccnt = len(self.params['SF'])
         elif 'WF' in self.params:
            ccnt = len(self.params['WF'])
         ifidx = 1
         while ifidx < ifcnt:
            buf = self.pgsystem("wc -l " + self.params['WF'][ifidx], self.LOGWRN, 16)
            ms = re.match(r'^(\d+)', buf)
            if ms: ccnt += int(ms.group(1))
            ifidx += 1
         ccnt *= 3
         self.set_dscheck_fcount(ccnt, self.PGOPT['extlog'])
      tinfo = {'bid' : 0, 'size' : 0, 'cnt' : 0, 'afmt' : '', 'dfmt' : '', 'sids' : [], 'wids' : []}
      if pgbck: tinfo['bid'] = pgbck['bid']
      ifidx = 1
      while True:
         if 'SF' in self.params:
            scnt += self.tar_backup_savedfiles(tarfile, tinfo, ccnt, chkstat)
         elif 'WF' in self.params:
            wcnt += self.tar_backup_webfiles(tarfile, tinfo, ccnt, chkstat)
         if ifidx >= ifcnt: break  # no more input file to read
         self.params['DS'] = self.read_one_infile(self.params['IF'][ifidx])
         ifidx += 1
      info = self.check_local_file(tarfile, 33)   # 1+32
      fsize = info['data_size'] if info else 0
   #   if fsize < tinfo['size']:
   #      self.pglog("{}: Backup file size {} is less than total file size {}".format(tarfile, fsize, tinfo['size']), self.PGOPT['extlog'])
      if fsize < self.PGLOG['ONEGBS']:
         self.pglog("{}: Backup file size {} is less than one GB".format(tarfile, fsize), self.PGOPT['extlog'])
      record = {'type' : qtype, 'data_format' : tinfo['dfmt'], 'data_size' : fsize,
                'uid' : self.PGOPT['UID'], 'checksum' : info['checksum'],
                'scount' : scnt, 'wcount' : wcnt}
      record['file_format'] = self.append_format_string(tinfo['afmt'], tfmt, 1)
      record['date_created'] = record['date_modified'] = info['date_modified']
      record['time_created'] = record['time_modified'] = info['time_modified']
      if dobackup:
         if qtype == 'D':
            dstat = self.local_copy_backup(qfile, fromfile, drpoint, self.PGOPT['errlog']|self.OVERRIDE)
            if not dstat: self.pglog("{}: Error Quaser Drdata for {}".format(bfile, dsid), self.PGOPT['extlog'])
         else:
            dstat = -1
         bstat = self.local_copy_backup(qfile, fromfile, endpoint, self.PGOPT['errlog']|self.OVERRIDE)
         if not bstat: self.pglog("{}: Error Quaser Backup for {}".format(bfile, dsid), self.PGOPT['extlog'])
         if dstat == self.FINISH: dstat = self.check_globus_finished(qfile, drpoint, self.PGOPT['errlog']|self.NOWAIT)
         if bstat == self.FINISH: bstat = self.check_globus_finished(qfile, endpoint, self.PGOPT['errlog']|self.NOWAIT)
         if dstat and bstat:
            self.delete_local_file(tarfile, self.PGOPT['extlog'])
            msg = tarfile + ": local tar file is removed"
            self.pglog(msg, self.PGOPT['wrnlog'])
         else:
            msg = tarfile + ": backup action is not complete and local tar file is not removed"
            self.pglog(msg, self.PGOPT['extlog'])
         record['status' ] = 'A'
      else:
         record['status' ] = 'T'
      bid = self.set_one_backfile(0, pgbck, bfile, None, qtype, dsid, record)
      if not bid:   # should not happen
         self.pglog("{}: Error add Quaser Backup file name in RDADB for {}".format(bfile, dsid), self.PGOPT['extlog'])
      tcnt = tinfo['cnt']
      tsize = tinfo['size']
      brec = {'bid' : bid}
      if scnt:
         for sid in tinfo['sids']:
            tcnt += self.pgupdt("sfile", brec, "sid = {}".format(sid))
            if ccnt and tcnt%20 == 0: self.set_dscheck_dcount(tcnt, tsize, self.PGOPT['extlog'])
      if wcnt:
         for wid in tinfo['wids']:
            tcnt += self.pgupdt_wfile(dsid, brec, "wid = {}".format(wid))
            if ccnt and tcnt%20 == 0: self.set_dscheck_dcount(tcnt, tsize, self.PGOPT['extlog'])
      if ccnt: self.set_dscheck_dcount(ccnt, tsize, self.PGOPT['extlog'])
      if dobackup:
         msg = "{}/{} Web/Saved files backed up to {} on '{}'".format(wcnt, scnt, qfile, endpoint)
         if dstat > 0: msg += " and '{}'".format(drpoint)
      else:
         msg = "{}/{} Web/Saved files tar to {}".format(wcnt, scnt, tarfile)
      self.pglog(msg, self.PGOPT['wrnlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      self.reset_rdadb_version(dsid)
      self.record_delete_directory(None, (self.params['DD'] if 'DD' in self.params else 0))

   # tarring saved files to Quasar backup file
   def tar_backup_savedfiles(self, tarfile, tinfo, ccnt, chkstat):
      scnt = len(self.params['SF'])
      dsid = self.params['DS']
      fcnd = "dsid = '{}' and sfile = ".format(dsid)
      s = 's' if scnt > 1 else ''
      self.pglog("tar {} Saved file{} of {} to {} ...".format(scnt, s, dsid, tarfile), self.WARNLG)
      self.validate_multiple_options(scnt, ["ST", 'DF', 'AF'])
      if 'ST' not in self.params:
         self.params['ST'] = [None]*scnt
         self.INOPTS['ST'] = 1
      if 'DF' not in self.params:
         self.params['DF'] = [None]*scnt
         self.INOPTS['DF'] = 1
      if 'AF' not in self.params:
         self.params['AF'] = [None]*scnt
         self.INOPTS['AF'] = 1
      dshome = "{}/{}".format(self.PGLOG['DECSHOME'], dsid)
      tarhome = "{}/{}/{}".format(self.PGLOG['DECSHOME'], self.PGLOG['BACKUPEP'], dsid)
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
         if ccnt and tcnt%20 == 0: self.set_dscheck_dcount(tcnt, tsize, self.PGOPT['extlog'])
         sfile = self.params['SF'][i]
         stype = self.params['ST'][i]
         relative = 0 if re.match(r'^/', sfile) else 1
         if not stype:
            ms = re.match(r'^([{}])/((.+)$'.format(self.PGOPT['SDTYP']), sfile)
            if ms:
               stype = ms.group(1)
               sfile = ms.group(2)
         tcnd = " AND type = '{}'".format(stype) if stype else ''
         pgrec = self.pgget('sfile', fields, "{}'{}'{}".format(fcnd, sfile, tcnd), self.PGOPT['extlog'])
         if not pgrec:
            self.pglog("{}-{}: Saved file record not in RDADB".format(dsid, sfile), self.PGOPT['extlog'])
         elif chkstat and pgrec['bid'] and pgrec['bid'] != tinfo['bid']:
            self.file_backup_status(pgrec, 0, self.PGOPT['extlog'])
         if not stype: stype = pgrec['type']
         if pgrec['locflag'] == 'O':
            tardir = tarhome
            savedfile = "{}/{}".format(tardir, sfile)
            ofile = self.join_paths(dsid, sfile)
            self.object_copy_local(savedfile, ofile, 'gdex-decsdata', self.PGOPT['wrnlog'])
            tmpfiles[i] = savedfile
         else:
            tardir = "{}/{}".format(dshome, stype)
            savedfile = "{}/{}".format(tardir, sfile) if relative else sfile
         if not op.exists(savedfile):
            self.pglog(savedfile + ": Saved file not exists to backup", self.PGOPT['extlog'])
         if relative:
            tarcmds[i] = "tar -{}vf {} -C {} {}".format(topt, tarfile, tardir, sfile)
         else:
            tarcmds[i] = "tar -{}vf {} {}".format(topt, tarfile, sfile)
         topt = 'u'
         tcnt += 1
         sids.append(pgrec['sid'])
         tsize += pgrec['data_size']
         # get combined data format
         if self.params['DF'][i]: pgrec['data_format'] = self.params['DF'][i]
         if self.params['AF'][i]: pgrec['file_format'] = self.params['AF'][i]
         if pgrec['data_format'] and pgrec['data_format'] != pdfmt:
            pdfmt = pgrec['data_format']
            dfmt = self.append_format_string(dfmt, pdfmt)
         if pgrec['file_format'] and pgrec['file_format'] != pafmt:
            pafmt = pgrec['file_format']
            afmt = self.append_format_string(afmt, pafmt)
     # do tar actions
      for i in range(scnt):
         if ccnt and tcnt%20 == 0: self.set_dscheck_dcount(tcnt, tsize, self.PGOPT['extlog'])
         tcnt += self.pgsystem(tarcmds[i], self.PGOPT['extlog'], 5)
         if tmpfiles[i]: self.delete_local_file(tmpfiles[i], self.PGOPT['extlog'])
      tinfo['cnt'] = tcnt
      tinfo['size'] = tsize
      tinfo['dfmt'] = dfmt
      tinfo['afmt'] = afmt
      tinfo['sids'] = sids
      return scnt

   # tarring web files to Quasar backup file
   def tar_backup_webfiles(self, tarfile, tinfo, ccnt, chkstat):
      wcnt = len(self.params['WF'])
      dsid = self.params['DS']
      fcnd = "wfile = "
      s = 's' if wcnt > 1 else ''
      self.pglog("tar {} Web file{} of {} to {} ...".format(wcnt, s, dsid, tarfile), self.WARNLG)
      self.validate_multiple_options(wcnt, ["WT", 'DF', 'AF'])
      if 'WT' not in self.params:
         self.params['WT'] = [None]*wcnt
         self.INOPTS['WT'] = 1
      if 'DF' not in self.params:
         self.params['DF'] = [None]*wcnt
         self.INOPTS['DF'] = 1
      if 'AF' not in self.params:
         self.params['AF'] = [None]*wcnt
         self.INOPTS['AF'] = 1
      dshome = "{}/{}".format(self.PGLOG['DSDHOME'], dsid)
      tarhome = "{}/{}/{}".format(self.PGLOG['DSDHOME'], self.PGLOG['BACKUPEP'], dsid)
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
         if ccnt and (tcnt%20) == 0: self.set_dscheck_dcount(tcnt, tsize, self.PGOPT['extlog'])
         wfile = self.params['WF'][i]
         wtype = self.params['WT'][i]
         fcnd = "wfile = '{}'".format(wfile)
         relative = 0 if re.match(r'^/', wfile) else 1
         if wtype: fcnd += " AND type = '{}'".format(wtype)
         pgrec = self.pgget_wfile(dsid, '*', fcnd, self.PGOPT['extlog'])
         if not pgrec:
            self.pglog("{}-{}: Web file record not in RDADB".format(dsid, wfile), self.PGOPT['extlog'])
         elif chkstat and pgrec['bid'] and pgrec['bid'] != tinfo['bid']:
            self.file_backup_status(pgrec, 0, self.PGOPT['extlog'])
         if pgrec['locflag'] == 'O':
            tardir = tarhome
            webfile = "{}/{}".format(tardir, wfile)
            ofile = self.join_paths(dsid, wfile)
            self.object_copy_local(webfile, ofile, self.PGLOG['OBJCTBKT'], self.PGOPT['wrnlog'])
            tmpfiles[i] = webfile
         else:
            tardir = dshome
            webfile = "{}/{}".format(tardir, wfile) if relative else wfile
         if not op.exists(webfile):
            self.pglog(webfile + ": Web file not exists to backup", self.PGOPT['extlog'])
         if relative:
            tarcmds[i] = "tar -{}vf {} -C {} {}".format(topt, tarfile, tardir, wfile)
         else:
            tarcmds[i] = "tar -{}vf {} {}".format(topt, tarfile, wfile)
         topt = 'u'
         tcnt += 1
         wids.append(pgrec['wid'])
         tsize += pgrec['data_size']
         # get combined data format
         if self.params['DF'][i]: pgrec['data_format'] = self.params['DF'][i]
         if self.params['AF'][i]: pgrec['file_format'] = self.params['AF'][i]
         if pgrec['data_format'] and pgrec['data_format'] != pdfmt:
            pdfmt = pgrec['data_format']
            dfmt = self.append_format_string(dfmt, pdfmt)
         if pgrec['file_format'] and pgrec['file_format'] != pafmt:
            pafmt = pgrec['file_format']
            afmt = self.append_format_string(afmt, pafmt)
     # do tar actions
      for i in range(wcnt):
         if ccnt and tcnt%20 == 0: self.set_dscheck_dcount(tcnt, tsize, self.PGOPT['extlog'])
         tcnt += self.pgsystem(tarcmds[i], self.PGOPT['extlog'], 5)
         if tmpfiles[i]: self.delete_local_file(tmpfiles[i], self.PGOPT['extlog'])
      tinfo['cnt'] = tcnt
      tinfo['size'] = tsize
      tinfo['dfmt'] = dfmt
      tinfo['afmt'] = afmt
      tinfo['wids'] = wids
      return wcnt

   # retrieve backup tar files from Quasar servers
   def retrieve_backup_files(self):
      tname = 'bfile'
      endpoint = self.PGLOG['BACKUPEP']
      dsid = self.params['DS']
      dcnt = bidx = chksize = 0
      s = 's' if self.ALLCNT > 1 else ''
      self.check_block_path(self.PGLOG['CURDIR'], "Retrieve Backup file{}".format(s), self.PGOPT['extlog'])
      self.pglog("Retrieving {} Backup file{} ...".format(self.ALLCNT, s), self.WARNLG)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Backup file{} downloaded".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            chksize = self.PGLOG['DSCHECK']['size']
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{} of {} Backup file{} downloaded/processed".format(dcnt, i, self.ALLCNT, s)
         (bfile, qfile) = self.get_backup_filenames(self.params['QF'][i], dsid)
         pgrec = self.pgget(tname, "*", "bfile = '{}'".format(bfile), self.LGEREX)
         if not pgrec:
            self.pglog("Backup-{}: is not in RDADB".format(bfile, self.PGLOG['MISSFILE']), self.LOGWRN)
            continue
         ms = re.match(r'^<(ds\d+\.\d+)_(\w)_\d+.txt>', pgrec['note'])
         if not ms:
            self.pglog("Backup-{}: Note field not formatted properly\n{}".format(bfile, pgrec['note']), self.LGEREX)
         fdsid = ms.group(1)
         ftype = ms.group(2)
         if fdsid != dsid: qfile = "/{}/{}".format(fdsid, bfile)
         info = self.check_backup_file(qfile)
         if not info:
            self.pglog("Backup-{}: {}".format(qfile, self.PGLOG['MISSFILE']), self.LOGWRN)
            continue
         endpath = 'decsdata' if ftype == 'S' else 'data'
         todir = "/{}/{}/{}".format(endpath, endpoint, fdsid)
         tardir = "{}{}".format(self.PGLOG['DSSDATA'], todir)
         self.make_local_directory(tardir, self.PGOPT['extlog'])
         tofile = "{}/{}".format(todir, op.basename(bfile))
         tarfile = "{}/{}".format(tardir, op.basename(bfile))
         self.TARFILES[pgrec['bid']] = tarfile
         tinfo = self.check_local_file(tarfile, 0, self.PGOPT['extlog'])
         if tinfo:
            self.pglog(tarfile + ": tar file exists already", self.PGOPT['wrnlog'])
         elif self.backup_copy_local(tofile, qfile, endpoint, self.PGOPT['extlog']):
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            dcnt += 1
            self.set_local_mode(tarfile, 1, 0o664)
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      self.pglog("{} of Quasar Backup file{} downloaded for {}".format(dcnt, self.ALLCNT, s, dsid), self.LOGWRN)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # untar backup file to retrieve web files
   def restore_backup_webfiles(self):
      tname = 'wfile'
      dsid = self.params['DS']
      bucket = self.PGLOG['OBJCTBKT']    # default object store bucket
      s = 's' if self.ALLCNT > 1 else ''
      wcnt = ocnt = bidx = chksize = 0
      dflags = {}
      self.pglog("Restore {} Web file{} of {} from backup ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Web file{} restored".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      if 'WT' not in self.params:
         self.params['WT'] = ['D']*self.ALLCNT
         self.OPTS['WT'][2] |= 2
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{} of {} web file{} restored/processed".format(wcnt, i, self.ALLCNT, s)
         wfile = self.params['WF'][i]
         type = self.params['WT'][i]
         pgrec = self.pgget_wfile(dsid, "*", "wfile = '{}' AND type = '{}'".format(wfile, type), self.PGOPT['extlog'])
         if not pgrec:  # should not happen
            self.pglog(wfile + ": Web File Not in RDADB", self.PGOPT['extlog'])
         oarch = warch = 1
         if pgrec['locflag'] == 'O':
            warch = 0
         elif pgrec['locflag'] == 'G':
            oarch = 0
         ofile = (self.join_paths(dsid, wfile) if oarch else None)
         if warch:
            tardir = "{}/{}".format(self.PGLOG['DSDHOME'], dsid)
         else:
            tardir = "{}/{}/{}".format(self.PGLOG['DSDHOME'], self.PGLOG['BACKUPEP'], dsid)
         afile = "{}/{}".format(tardir, wfile)
         tmpfile = None if warch else afile
         ainfo = self.check_local_file(afile, 0, self.PGOPT['extlog'])
         if ainfo:
            self.pglog(afile + ": File exists already", self.PGOPT['wrnlog'])
         else:
            tarfile = self.TARFILES[pgrec['bid']]
            ainfo = self.get_backup_member_file(pgrec, tarfile, tardir)
            tarcmd = "tar -xvf {} -C {} {}".format(tarfile, tardir, wfile)
            self.pgsystem(tarcmd, self.PGOPT['extlog'], 5)
            ainfo = self.check_local_file(afile, 0, self.PGOPT['extlog'])
            if not ainfo:
               self.pglog("{}: Error untar File {}".format(tarfile, afile), self.PGOPT['wrnlog'])
            if warch: wcnt += 1
         if ainfo['data_size'] != pgrec['data_size']:
            self.pglog("{}: Different Restored/RDADB file sizes {}/{}".format(afile, ainfo['data_size'], pgrec['data_size']), self.PGOPT['wrnlog'])
         if oarch:
            oinfo = self.check_object_file(ofile, bucket, 0, self.PGOPT['extlog'])
            if oinfo:
               self.pglog(ofile + ": Object file exists", self.PGOPT['wrnlog'])
            elif self.local_copy_object(ofile, afile, bucket, None, self.PGOPT['extlog']):
               ocnt += 1
         if tmpfile: self.delete_local_file(tmpfile, self.PGOPT['extlog'])
         if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
      if wcnt > 0:
         self.pglog("{} of {} Web file{} restored for {}".format(wcnt, self.ALLCNT, s, self.params['DS']), self.PGOPT['emllog'])
      if ocnt > 0:
         self.pglog("{} of {} Object file{} restored for {}".format(ocnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # untar backup tarfile to get a single member file
   def get_backup_member_file(self, pgrec, tarfile, tardir):
      mfile = wfile = pgrec['wfile']
      note = pgrec['note']
      while not re.search(r'{}<:>'.format(mfile), note):
         ms = re.search(r'>MV .* File (\S+) To .*{}'.format(mfile), note)
         if not ms: break
         mfile = ms.group(1)
      tarcmd = "tar -xvf {} -C {} {}".format(tarfile, tardir, mfile)
      self.pgsystem(tarcmd, self.PGOPT['extlog'], 5)
      afile = '{}/{}'.format(tardir, mfile)
      ainfo = self.check_local_file(afile, 0, self.PGOPT['extlog']|self.PFSIZE)
      if ainfo and mfile != wfile:
         nfile = '{}/{}'.format(tardir, wfile)
         self.move_local_file(nfile, afile, self.PGOPT['extlog'])     
         ainfo = self.check_local_file(nfile, 0, self.PGOPT['extlog']|self.PFSIZE)
      return ainfo

   # untar backup file to retrieve saved files
   def restore_backup_savedfiles(self):
      tname = 'sfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      bucket = "gdex-decsdata"    # default object store bucket
      s = 's' if self.ALLCNT > 1 else ''
      scnt = ocnt = bidx = chksize = 0
      dflags = {}
      self.pglog("Restore {} Saved file{} of {} from backup ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Saved file{} restored".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{} of {} saved file{} restored/processed".format(scnt, i, self.ALLCNT, s)
         sfile = self.params['SF'][i]
         type = self.params['ST'][i]
         pgrec = self.pgget(tname, "*", "sfile = '{}' AND type = '{}' AND {}".format(sfile, type, dcnd), self.PGOPT['extlog'])
         if not pgrec:  # should not happen
            self.pglog(sfile + ": Saved File Not in RDADB", self.PGOPT['extlog'])
         oarch = sarch = 1
         if pgrec['locflag'] == 'O':
            sarch = 0
         elif pgrec['locflag'] == 'G':
            oarch = 0
         ofile = (self.join_paths(dsid, sfile) if oarch else None)
         if sarch:
            tardir = "{}/{}".format(self.PGLOG['DECSHOME'], dsid)
         else:
            tardir = "{}/{}/{}".format(self.PGLOG['DECSHOME'], self.PGLOG['BACKUPEP'], dsid)
         afile = "{}/{}".format(tardir, sfile)
         tmpfile = None if sarch else afile
         ainfo = self.check_local_file(afile, 0, self.PGOPT['extlog'])
         if ainfo:
            self.pglog(afile + ": File exists already", self.PGOPT['wrnlog'])
         else:
            tarfile = self.TARFILES[pgrec['bid']]
            ainfo = self.get_backup_member_file(pgrec, tarfile, tardir)
            tarcmd = "tar -xvf {} -C {} {}".format(tarfile, tardir, sfile)
            self.pgsystem(tarcmd, self.PGOPT['extlog'], 5)
            ainfo = self.check_local_file(afile, 0, self.PGOPT['extlog'])
            if not ainfo:
               self.pglog("{}: Error untar File {}".format(tarfile, afile), self.PGOPT['wrnlog'])
            if sarch: scnt += 1
         if ainfo['data_size'] != pgrec['data_size']:
            self.pglog("{}: Different Restored/RDADB file sizes {}/{}".format(afile, ainfo['data_size'], pgrec['data_size']), self.PGOPT['wrnlog'])
         if oarch:
            oinfo = self.check_object_file(ofile, bucket, 0, self.PGOPT['extlog'])
            if oinfo:
               self.pglog(ofile + ": Object file exists", self.PGOPT['wrnlog'])
            elif self.local_copy_object(ofile, afile, bucket, None, self.PGOPT['extlog']):
               ocnt += 1
         if tmpfile: self.delete_local_file(tmpfile, self.PGOPT['extlog'])
         if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
      if scnt > 0:
         self.pglog("{} of {} Saved file{} restored for {}".format(scnt, self.ALLCNT, s, self.params['DS']), self.PGOPT['emllog'])
      if ocnt > 0:
         self.pglog("{} of {} Object file{} restored for {}".format(ocnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # cross copy quasar backup files between Globus endpoints gdex-quasar and gdex-quasar-drdata
   def crosscopy_backup_files(self):
      tname = 'bfile'
      qtype = 'D'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      dflags = {}
      bpoint = 'gdex-quasar'
      dpoint = 'gdex-quasar-drdata'
      self.pglog("Cross Copy {} Quasar file{} of {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      self.validate_multiple_values(tname, self.ALLCNT)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} file{} processed for Quasar archive".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      reorder = errcnt = self.MODCNT = bgcnt = bcnt = dcnt = 0
      perrcnt = self.ALLCNT
      efiles = [1]*self.ALLCNT
      if self.PGSIG['BPROC'] > 1:
         qfiles = [None]*self.ALLCNT
         barchs = [None]*self.ALLCNT
      fnames = None
      while True:
         for i in range(bidx, self.ALLCNT):
            if self.PGSIG['BPROC'] < 2 and i > bidx and ((i-bidx)%20) == 0:
               if self.PGLOG['DSCHECK']:
                  self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
               if 'EM' in self.params:
                  self.PGLOG['PRGMSG'] = "{}/{} of {} Quasar file{} archived/processed".format(bcnt, i, self.ALLCNT, s)
            bfile = self.params['QF'][i]
            if not (efiles[i] and bfile): continue
            efiles[i] = 0
            (bfile, qfile) = self.get_backup_filenames(bfile, dsid)
            binfo = "{}-{}".format(dsid, bfile)
            pgrec = self.pgget(tname, "*", "{} and bfile = '{}'".format(dcnd, bfile), self.PGOPT['extlog'])
            if not pgrec:
               self.pglog(binfo + ": Fail to Cross Copy for Quasar file not in RDADB", self.PGOPT['emlerr'])
               continue
            if pgrec and self.params['QF'][i] != bfile: self.params['QF'][i] = bfile
            barch = darch = 1
            self.pglog(binfo + ": Cross Copy Quasar file ...",  self.WARNLG)
            info = self.check_backup_file(bfile, bpoint, 0, self.PGOPT['emerol'])
            if info:
               barch = 0
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['B'] = bpoint
               continue
            info = self.check_backup_file(bfile, dpoint, 0, self.PGOPT['emerol'])
            if info:
               darch = 0
            elif info is not None:
               errcnt += 1
               efiles[i] = 1
               dflags['D'] = dpoint
               continue
            if barch and darch:
               self.pglog(binfo + ": Cannot Cross Copy, Neither Backup Nor Drdata file Exists", self.PGOPT['errlog'])
               continue
            elif not (barch or darch) and pgrec['type'] == 'D':
               self.pglog(binfo + ": No need Cross Copy, Both Backup & Drdata Exist", self.PGOPT['wrnlog'])
               continue
            if barch:
               if not self.endpoint_copy_endpoint(qfile, qfile, bpoint, dpoint, self.PGOPT['emerol']|self.OVERRIDE):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['B'] = bpoint
                  continue
               bcnt += 1
            elif darch:
               if not self.endpoint_copy_endpoint(qfile, qfile, dpoint, bpoint, self.PGOPT['emerol']|self.OVERRIDE):
                  errcnt += 1
                  efiles[i] = 1
                  dflags['D'] = dpoint
                  continue
               dcnt += 1
            if self.PGSIG['BPROC'] > 1:
               qfiles[i] = qfile
               barchs[i] = barch
               bgcnt += 1
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            if self.PGSIG['BPROC'] < 2:
               if not fnames: fnames = self.get_field_keys(tname)  # get setting fields if not yet
               bid = self.set_one_backfile(i, pgrec, bfile, fnames, qtype)
               if not bid:
                  self.params['QF'][i] = None
                  continue
         if errcnt == 0 or errcnt >= perrcnt or self.PGLOG['DSCHECK']: break
         perrcnt = errcnt
         self.pglog("Recopy {} Quasar file{} for {}".format(errcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
         errcnt = self.reset_errmsg(0)
      if errcnt:
         self.RETSTAT = self.reset_errmsg(errcnt)
         for i in range(bidx, self.ALLCNT):
            if efiles[i]: self.params['QF'][i] = ''
         if self.PGLOG['DSCHECK']:
            self.check_storage_dflags(dflags, self.PGLOG['DSCHECK'], self.PGOPT['emerol'])
      if bgcnt:
         self.check_background(None, 0, self.LOGWRN, 1)
         for i in range(self.ALLCNT):
            if barchs[i]:
               self.validate_backarch(qfiles[i], "{}-{}".format(bpoint, qfiles[i]), i)
            elif qfiles[i]:
               self.validate_backarch(qfiles[i], "{}-{}".format(dpoint, qfiles[i]), i)
      if bcnt > 0: self.pglog("{} of {} Backup file{} Cross Copied for {}".format(bcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if dcnt > 0: self.pglog("{} of {} Drdata file{} Cross Copied for {}".format(dcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      self.pglog("{} of {} Quasar Backup file record{} modified for {}!".format(self.MODCNT, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if 'ON' in self.params: reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # validate a webfile archive, fatal is error
   def validate_gladearch(self, file, ofile, i):
      info = self.check_local_file(file, 6, self.PGOPT['errlog']|self.PFSIZE)
      if not info:
         self.pglog("Error archiving {} to {}".format(ofile, file), self.LGEREX)
      elif self.OPTS['SZ'][2]&2 and self.params['SZ'] and info['data_size'] != self.params['SZ'][i]:
         self.pglog("Wrong Sizes: ({}){}/({}){}".format(file, info['data_size'], ofile, self.params['SZ'][i]), self.LGEREX)
      else:
         self.set_local_mode(file, 1, self.PGLOG['FILEMODE'], info['mode'], info['logname'], self.PGOPT['errlog'])

   # validate an object store file archive, fatal is error
   def validate_objectarch(self, file, ofile, bucket, i):
      info = self.check_object_file(file, bucket, 0, self.PGOPT['errlog'])
      if not info:
         self.pglog("Error archiving {} to {}-{}".format(ofile, bucket, file), self.LGEREX)
      elif self.OPTS['SZ'][2]&2 and self.params['SZ'] and info['data_size'] != self.params['SZ'][i]:
         self.pglog("Wrong Sizes: ({}-{}){}/({}){}".format(bucket, file, info['data_size'], ofile, self.params['SZ'][i]), self.LGEREX)

   # validate an object store file archive, fatal is error
   def validate_backarch(self, file, ofile, endpoint, i):
      info = self.check_backup_file(file, endpoint, 0, self.PGOPT['errlog'])
      if not info:
         self.pglog("Error archiving {} to {}-{}".format(ofile, endpoint, file), self.LGEREX)
      elif self.OPTS['SZ'][2]&2 and self.params['SZ'] and info['data_size'] != self.params['SZ'][i]:
         self.pglog("Wrong Sizes: ({}-{}){}/({}){}".format(endpoint, file, info['data_size'], ofile, self.params['SZ'][i]), self.LGEREX)

   # get dataset info
   def get_dataset_info(self):
      tname = 'dataset'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      hash = self.TBLHASH[tname]
      self.pglog("Get dataset info for {} ...".format(dsid), self.WARNLG)
      fnames = self.params['FN'] if 'FN' in self.params else ''
      kvalues = self.params['KV'] if 'KV' in self.params else []
      getkeys = 1 if (kvalues or re.match(r'^all$', fnames, re.I)) else 0
      if not getkeys or fnames:
         self.OUTPUT.write("{}{}{}\n".format(self.OPTS['DS'][1], self.params['ES'], dsid))
         if self.PGOPT['ACTS'] == self.OPTS['GA'][0]:
            self.OUTPUT.write("[{}]\n".format(tname.upper()))   # get all action
         fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['dsall'])
         pgrec = self.pgget(tname, "*", dcnd, self.LGEREX)
         self.print_row_format(pgrec, fnames, hash)
      self.view_keyvalues(dsid, kvalues, getkeys)
      if fnames and 'PE' in self.params: self.get_period_info()
      if 'WN' in self.params: self.view_filenumber(dsid, 0)
      return 1   # get one dataset record

   # get dataset period information
   def get_period_info(self):
      tname = "dsperiod"
      dsid = self.params['DS']
      hash = self.TBLHASH[tname]
      fnames = self.PGOPT[tname]
      onames = self.params['ON'] if 'ON' in self.params else "G"
      condition = self.get_condition(tname) + self.get_order_string(onames, tname) + ", dorder"
      pgrecs = self.pgmget(tname, "*", condition, self.LGEREX)
      if pgrecs:
         lens = self.all_column_widths(pgrecs, fnames, hash) if 'FO' in self.params else None
         self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         s = 's' if cnt  > 1 else ''
         self.pglog("{} period record{} retrieved for {}".format(cnt, s, dsid), self.LOGWRN)

   # add a new or modify an existing datatset record into RDADB
   def set_dataset_info(self, include = None):
      tname = 'dataset'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      self.pglog("Set {} info of {} ...".format(tname, dsid), self.WARNLG)
      mcnt = acnt = pcnt = kcnt = 0
      fnames = self.get_field_keys(tname, include)
      if fnames: # set dataset record
         pgrec = self.pgget(tname, self.get_string_fields(fnames, tname), dcnd, self.LGEREX)
         record = self.build_record(fnames, pgrec, tname, 0)
         if record:
            if 'backflag' in record and record['backflag'] == 'P': record['backflag'] = 'N'
            if 'locflag' in record and record['locflag'] in 'BR': record['locflag'] = 'G'
            if pgrec:
               record['date_change'] = self.curdate()
               if self.pgupdt(tname, record, dcnd, self.LGEREX):
                  mcnt += 1
                  if 'use_rdadb' in record and re.search(r'^[PYW]$', record['use_rdadb']):
                     self.params['WN'] = 6
            else:
               record['dsid'] = dsid
               record['date_change'] = record['date_create'] = self.curdate()
               if not record['use_rdadb']: record['use_rdadb'] = 'Y'
               acnt += self.pgadd(tname, record, self.LGEREX)
      if acnt == 0: pcnt = self.set_period_info(dcnd)   # set dsperiod record
      kvalues = self.params['KV'] if 'KV' in self.params else []
      kcnt = self.set_keyvalues(dsid, kvalues)
      if (pcnt + kcnt + mcnt + acnt) == 0:
         if not include: self.pglog("No change of dataset record for {}!".format(dsid), self.LOGWRN)
      else:
         if acnt:
            self.pglog("Dataset record added for {}!".format(dsid),  self.LOGWRN)
         else:
            self.pglog("Dataset record modified for {}!".format(dsid),  self.LOGWRN)
         if pcnt + mcnt:
            self.reset_rdadb_version(dsid)

   # add new or modify existing period record into RDADB
   def set_period_info(self, dcnd):
      tname = 'dsperiod'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      dorder = 1
      allcnt = (len(self.params['ED']) if 'ED' in self.params else
                (len(self.params['BD']) if 'BD' in self.params else
                 (len(self.params['BT']) if 'BT' in self.params else
                  (len(self.params['ET']) if 'ET' in self.params else 0))))
      if not allcnt: return 0
      s = 's' if allcnt > 1 else ''
      self.pglog("Set {} period{} for {} ...".format(allcnt, s, dsid), self.WARNLG)
      fnames = self.get_field_keys(tname, None, "G")
      if not fnames: return 0
      self.validate_multiple_values(tname, allcnt, fnames)
      pcnt = 0
      for i in range(allcnt):
         gindex = self.params['GI'][i] if 'GI' in self.params else 0
         if gindex == 0:
            tmpcnd = "{} AND gindex = {} AND dorder = {}".format(dcnd, gindex, dorder)
            dorder += 1
         else:
            tmpcnd = "{} AND gindex = {}".format(dcnd, gindex)
         pgrec = self.pgget(tname, self.get_string_fields(fnames, tname), tmpcnd, self.LGEREX)
         if not pgrec:
            self.pglog("No Period for {}\nAdd it via metadata editor".format(tmpcnd), self.LOGWRN)
            continue
         record = self.build_record(fnames, pgrec, tname, i)
         if record:
            sdpcmd = "sdp -d {} -g {}".format(dsid[2:], gindex)
            if 'date_start' in record: sdpcmd += " -bd " + record['date_start']
            if 'date_end' in record: sdpcmd += " -ed " + record['date_end']
            if 'time_start' in record: sdpcmd += " -bt " + record['time_start']
            if 'time_end' in record: sdpcmd += " -et " + record['time_end']
            if self.pgsystem(sdpcmd): pcnt += 1
      if pcnt > 0: self.pglog("{} of {} period{} modified for {}!".format(pcnt, allcnt, s, dsid), self.LOGWRN)
      return pcnt

   # delete group information for given dataset and index list
   def delete_group_info(self):
      tname = 'dsgroup'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Delete {} group{} from {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      delcnt = webcnt = savedcnt = prdcnt = chdcnt = 0
      record = {}
      record['gindex'] = record['tindex'] = 0
      for gindex in self.params['GI']:
         gcnd = "gindex = {}".format(gindex)
         cnd = "{} AND {}".format(dcnd, gcnd)
         webcnt = self.pgget_wfile(dsid, '', gcnd, self.LGEREX)
         savedcnt = self.pgget("sfile", '', cnd, self.LGEREX)
         prdcnt = self.pgget("dsperiod", "", cnd, self.LGEREX)
         chdcnt = self.pgget(tname, "", "{} AND pindex = {}".format(dcnd, gindex), self.LGEREX)
         if (webcnt + savedcnt + prdcnt + chdcnt) > 0:
            ss = 's' if (webcnt + savedcnt + prdcnt + chdcnt) else ''
            self.pglog("Can not delete GroupIndex {}, due to".format(gindex), self.LOGWRN)
            self.pglog("{}/{}/{}/{} reocrd{} of ".format(webcnt, savedcnt, prdcnt, chdcnt, ss) +
                        "WebFile/SavedFile/GroupPeriod/SubGroup still in RDADB for the group", self.LOGWRN)
            continue
         delcnt += self.pgdel(tname, cnd, self.LGEREX)
      self.pglog("{} of {} group{} deleted from {}".format(delcnt, self.ALLCNT, s, dsid), self.LOGWRN)
      if delcnt > 0: self.reset_rdadb_version(dsid)

   # get group information
   def get_group_info(self):
      tname = "dsgroup"
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      hash = self.TBLHASH[tname]
      self.pglog("Get group info of {} from RDADB ...".format(dsid), self.WARNLG)
      lens = oflds = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      if 'RG' in self.params and 'GI' in self.params: self.get_subgroups("GG")
      fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['gpall'])
      onames = self.params['ON'] if 'ON' in self.params else "I"
      qnames = fnames
      if fnames.find('Y') > -1 and fnames.find('X') < 0: qnames += 'X'
      if 'WN' in self.params and fnames.find('I') < 0: qnames += 'I'
      qnames += self.append_order_fields(onames, qnames, tname, "Y")
      condition = self.get_condition(tname)
      if 'ON' in self.params and ('OB' in self.params or re.search(r'Y', onames, re.I)):
         oflds = self.append_order_fields(onames, None, tname)
      else:
         condition += self.get_order_string(onames, tname)
      pgrecs = self.pgmget(tname, self.get_string_fields(qnames, tname, None, "Y"), condition, self.LGEREX)
      if self.PGOPT['ACTS'] == self.OPTS['GA'][0]:
         self.OUTPUT.write("[{}]\n".format(tname.upper()))   # get all action
      else:
         self.OUTPUT.write("{}{}{}\n".format(self.OPTS['DS'][1], self.params['ES'], dsid))
      if pgrecs:
         if fnames.find('Y') > -1: pgrecs['pname'] = self.group_index_to_id(pgrecs['pindex'])
         if 'FO' in self.params: lens = self.all_column_widths(pgrecs, fnames, hash)
         if oflds: pgrecs = self.sorthash(pgrecs, oflds, hash, self.params['OB'])
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         s = 's' if cnt > 1 else ''
         self.pglog("{} group record{} retrieved".format(cnt, s), self.LOGWRN)
         if 'WN' in self.params: self.view_filenumber(dsid, pgrecs['gindex'], cnt)
      else:
         self.pglog("No group found for " + condition, self.LOGWRN)

   # add or modify group information
   def set_group_info(self):
      if self.ALLCNT == 0 or self.params['GI'][0] == 0: return  # skip group index 0
      tname = 'dsgroup'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Set {} group{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      if 'PI' in self.params or 'PN' in self.params: self.validate_groups(1)
      fnames = self.get_field_keys(tname, None, 'Y')
      if not fnames: return self.pglog("Nothing to set for group!", self.LOGWRN)
      self.validate_multiple_values(tname, self.ALLCNT, fnames)
      fields = self.get_string_fields(fnames, tname) + ", level, gidx"
      tcnt = addcnt = modcnt = 0
      for i in range(self.ALLCNT):
         gindex = self.params['GI'][i]
         if gindex == 0: continue # skip group index 0
         pgrec = self.pgget(tname, fields, "{} AND gindex = {}".format(dcnd, gindex), self.LGEREX)
         record = self.build_record(fnames, pgrec, tname, i)
         if record:
            level = 1
            pindex = record['pindex'] if 'pindex' in record and record['pindex'] else 0
            if pindex:
               if abs(pindex) >= abs(int(gindex)):
                  self.pglog("{}-{}: Parent Group Index {} must be smaller than current Index!".format(dsid, gindex, pindex), self.LGEREX)
               prec = self.pgget(tname, 'grptype', "{} AND gindex = {}".format(dcnd, pindex), self.LGEREX)
               if not prec:
                  self.pglog("{}-{}: Parent Group Index {} not on file!".format(dsid, gindex, pindex), self.LGEREX)
               if prec['grptype'] == 'I':
                  if 'grptype' not in record:
                     if not pgrec: record['grptype'] = 'I'
                  elif record['grptype'] == 'P':
                     self.pglog("{}-{}: cannot set Public for Parent Group {} is Internal".format(dsid, gindex, pindex), self.LGEREX)
               level = self.get_group_levels(dsid, pindex, level + 1)
               if pgrec:
                  self.params['WN'] = 6
                  self.CHGGRPS[record['pindex']] = 1
                  self.CHGGRPS[pgrec['pindex']] = 1
            if not pgrec or level != pgrec['level']: record['level'] = level
            if pgrec:
               modcnt += self.pgupdt(tname, record, "gidx = {}".format(pgrec['gidx']), self.LGEREX)
               if pindex: tcnt += self.reset_top_group_index(dsid, gindex, 6)
               if 'grptype' in record:
                  self.params['WN'] = 6
                  self.CHGGRPS[gindex] = 1
                  if record['grptype'] == 'I': self.set_for_internal_group(gindex)
            else:
               record['dsid'] = dsid
               record['gindex'] = gindex
               addcnt += self.pgadd(tname, record, self.LGEREX)
      self.pglog("{}/{} of {} group{} added/modified for {}!".format(addcnt, modcnt, self.ALLCNT, s, dsid), self.LOGWRN)
      if (addcnt + modcnt + tcnt) > 0: self.reset_rdadb_version(dsid)

   # set subgroups and files to Internal for given group index
   def set_for_internal_group(self, gindex):
      tname = 'dsgroup'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      grp = "{}-{}".format(dsid, gindex)
      gcnd = "gindex = {} AND status = 'P'".format(gindex)
      cnd = "{} AND {}".format(dcnd, gcnd)
      pgrecs = self.pgmget_wfile(dsid, 'wfile, type', gcnd, self.PGOPT['extlog'])
      cnt = len(pgrecs['wfile']) if pgrecs else 0
      srec = {'status' : 'I'}
      if cnt:
         s = 's' if cnt > 1 else ''
         self.pglog("{}: set {} web file{} to Internal".format(grp, cnt, s), self.PGOPT['wrnlog'])
         if self.pgupdt_wfile(dsid, srec, gcnd, self.PGOPT['extlog']):
            for i in range(cnt):
               self.change_wfile_mode(dsid, pgrecs['wfile'][i], pgrecs['type'][i], 'P', 'I')
      cnt = self.pgget('sfile', '', cnd, self.PGOPT['extlog'])
      if cnt:
         s = 's' if cnt > 1 else ''
         self.pglog("{}: set {} saved file{} to Internal".format(grp, cnt, s), self.PGOPT['wrnlog'])
         self.pgupdt("sfile", srec, cnd, self.PGOPT['extlog'])
      cnd = "{} AND pindex = {} AND grptype = 'P'".format(dcnd, gindex)
      pgrecs = self.pgmget(tname, 'gindex', cnd, self.PGOPT['extlog'])
      cnt = (len(pgrecs['gindex']) if pgrecs else 0)
      if cnt:
         s = 's' if cnt > 1 else ''
         self.pglog("{}: set {} subgroup{} to Internal".format(grp, cnt, s), self.PGOPT['wrnlog'])
         self.pgexec("UPDATE dsgroup SET grptype = 'I' WHERE " + cnd, self.PGOPT['extlog'])
         for i in range(cnt):
            self.set_for_internal_group(pgrecs['gindex'][i])

   # get WEB file information
   def get_webfile_info(self):
      tname = "wfile"
      dsid = self.params['DS']
      hash = self.TBLHASH[tname]
      self.pglog("Get Web file info of {} from RDADB ...".format(dsid), self.WARNLG)
      lens = oflds = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      if 'RG' in self.params and 'GI' in self.params: self.get_subgroups("GW")
      fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['wfall'])
      if 'QF' in self.params and fnames.find('B') < 0: fnames += 'B'
      if 'QT' in self.params:
         if fnames.find('B') < 0: fnames += 'B'
         if fnames.find('Q') < 0: fnames += 'Q'
      onames = self.params['ON'] if 'ON' in self.params else "ITO"
      qnames = fnames
      if 'TT' in self.params and fnames.find('S') < 0: qnames , 'S'
      if 'RN' in self.params:
         if fnames.find('I') < 0: qnames += 'I'
         if fnames.find('T') < 0: qnames += 'T'
      qnames += self.append_order_fields(onames, qnames, tname)
      condition = self.get_condition(tname)
      if 'ON' in self.params and ('OB' in self.params or re.search(r'(B|P)', onames, re.I)):
         oflds = self.append_order_fields(onames, None, tname)
      else:
         condition += self.get_order_string(onames, tname)
      if self.PGOPT['ACTS'] == self.OPTS['GA'][0]:
         self.OUTPUT.write("[WEBFILE]\n")   # get all action
      else:
         self.OUTPUT.write("{}{}{}\n".format(self.OPTS['DS'][1], self.params['ES'], dsid))
      fields = self.get_string_fields(qnames, tname)
      if qnames.find('Q') > -1:
         tjoin = "LEFT JOIN bfile ON wfile.bid = bfile.bid"
         pgrecs = self.pgmget_wfile_join(dsid, tjoin, fields, condition, self.LGEREX)
      else:
         pgrecs = self.pgmget_wfile(dsid, fields, condition, self.LGEREX)
      if pgrecs:
         if 'bid' in pgrecs: pgrecs['bid'] = self.get_quasar_backfiles(pgrecs['bid'])
         if 'FO' in self.params: lens = self.all_column_widths(pgrecs, fnames, hash)
         if oflds: pgrecs = self.sorthash(pgrecs, oflds, hash, self.params['OB'])
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         if 'RN' in self.params:
            if tname in pgrecs:
               pgrecs[tname] = self.get_relative_names(pgrecs[tname], pgrecs['gindex'], pgrecs['type'])
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         if 'TT' in self.params: self.print_statistics(pgrecs['data_size'])
         s = 's' if cnt > 1 else ''
         self.pglog("{} Web file record{} retrieved".format(cnt, s), self.LOGWRN)
      else:
         self.pglog("No Web file found for " + condition, self.LOGWRN)

   # get Help file information
   def get_helpfile_info(self):
      tjoin = tname = "hfile"
      dsid = self.params['DS']
      hash = self.TBLHASH[tname]
      self.pglog("Get Help file info of {} from RDADB ...".format(dsid), self.WARNLG)
      lens = oflds = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['hfall'])
      onames = self.params['ON'] if 'ON' in self.params else "TO"
      qnames = fnames
      if 'TT' in self.params and fnames.find('S') < 0: qnames , 'S'
      qnames += self.append_order_fields(onames, qnames, tname)
      condition = self.get_condition(tname)
      if 'ON' in self.params and 'OB' in self.params:
         oflds = self.append_order_fields(onames, None, tname)
      else:
         condition += self.get_order_string(onames, tname)
      if self.PGOPT['ACTS'] == self.OPTS['GA'][0]:
         self.OUTPUT.write("[HELPFILE]\n")   # get all action
      else:
         self.OUTPUT.write("{}{}{}\n".format(self.OPTS['DS'][1], self.params['ES'], dsid))
      pgrecs = self.pgmget(tjoin, self.get_string_fields(qnames, tname), condition, self.LGEREX)
      if pgrecs:
         if 'FO' in self.params: lens = self.all_column_widths(pgrecs, fnames, hash)
         if oflds: pgrecs = self.sorthash(pgrecs, oflds, hash, self.params['OB'])
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         if 'TT' in self.params: self.print_statistics(pgrecs['data_size'])
         s = 's' if cnt > 1 else ''
         self.pglog("{} Help file record{} retrieved".format(cnt, s), self.LOGWRN)
      else:
         self.pglog("No Help file found for " + condition, self.LOGWRN)

   # get Saved file information
   def get_savedfile_info(self):
      tjoin = tname = "sfile"
      dojoin = 0
      dsid = self.params['DS']
      hash = self.TBLHASH[tname]
      self.pglog("Get Saved file info of {} from RDADB ...".format(dsid), self.WARNLG)
      lens = oflds = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      if 'RG' in self.params and 'GI' in self.params: self.get_subgroups("GS")
      fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['sfall'])
      if 'QF' in self.params or 'QT' in self.params:
         if fnames.find('B') < 0: fnames += 'B'
         if fnames.find('Q') < 0: fnames += 'Q'
      onames = self.params['ON'] if 'ON' in self.params else "ITO"
      qnames = fnames
      if 'TT' in self.params and fnames.find('S') < 0: qnames += 'S'
      if 'RN' in self.params:
         if fnames.find('I') < 0: qnames += 'I'
         if fnames.find('T') < 0: qnames += 'T'
      qnames += self.append_order_fields(onames, qnames, tname)
      if qnames.find('Q') > -1: dojoin = 1
      condition = self.get_condition(tname, None, None, dojoin)
      if 'ON' in self.params and ('OB' in self.params or re.search(r'(B|P)', onames, re.I)):
         oflds = self.append_order_fields(onames, None, tname)
      else:
         condition += self.get_order_string(onames, tname)
      if self.PGOPT['ACTS'] == self.OPTS['GA'][0]:
         self.OUTPUT.write("[SAVEDFILE]\n")   # get all action
      else:
         self.OUTPUT.write("{}{}{}\n".format(self.OPTS['DS'][1], self.params['ES'], dsid))
      if dojoin: tjoin += " LEFT JOIN bfile ON sfile.bid = bfile.bid"
      pgrecs = self.pgmget(tjoin, self.get_string_fields(qnames, tname), condition, self.LGEREX)
      if pgrecs:
         if 'bid' in pgrecs: pgrecs['bid'] = self.get_quasar_backfiles(pgrecs['bid'])
         if 'FO' in self.params: lens = self.all_column_widths(pgrecs, fnames, hash)
         if oflds: pgrecs = self.sorthash(pgrecs, oflds, hash, self.params['OB'])
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         if 'RN' in self.params:
            if tname in pgrecs:
               pgrecs[tname] = self.get_relative_names(pgrecs[tname], pgrecs['gindex'], pgrecs['type'], 1)
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         if 'TT' in self.params: self.print_statistics(pgrecs['data_size'])
         s = 's' if cnt > 1 else ''
         self.pglog("{} Saved file record{} retrieved".format(cnt, s), self.LOGWRN)
      else:
         self.pglog("No Saved file found for " + condition, self.LOGWRN)

   # get Quasar Backup file information
   def get_backfile_info(self):
      tname = "bfile"
      dsid = self.params['DS']
      hash = self.TBLHASH[tname]
      self.pglog("Get Quasar Backup file info of {} from RDADB ...".format(dsid), self.WARNLG)
      lens = oflds = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['bfall'])
      onames = self.params['ON'] if 'ON' in self.params else "O"
      qnames = fnames
      if 'TT' in self.params and fnames.find('S') < 0: qnames += 'S'
      qnames += self.append_order_fields(onames, qnames, tname)
      condition = self.get_condition(tname)
      if 'ON' in self.params and 'OB' in self.params:
         oflds = self.append_order_fields(onames, None, tname)
      else:
         condition += self.get_order_string(onames, tname)
      if self.PGOPT['ACTS'] == self.OPTS['GA'][0]:
         self.OUTPUT.write("[BACKFILE]\n")   # get all action
      else:
         self.OUTPUT.write("{}{}{}\n".format(self.OPTS['DS'][1], self.params['ES'], dsid))
      pgrecs = self.pgmget(tname, self.get_string_fields(qnames, tname), condition, self.LGEREX)
      if pgrecs:
         if 'FO' in self.params: lens = self.all_column_widths(pgrecs, fnames, hash)
         if oflds: pgrecs = self.sorthash(pgrecs, oflds, hash, self.params['OB'])
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         if 'TT' in self.params: self.print_statistics(pgrecs['data_size'])
         s = 's' if cnt > 1 else ''
         self.pglog("{} Quasar Backup file record{} retrieved".format(cnt, s), self.LOGWRN)
      else:
         self.pglog("No Quasar Backup file found for " + condition, self.LOGWRN)

   # add or modify WEB file information
   def set_webfile_info(self, include = None):
      bidx = 0
      dftloc = None
      tname = 'wfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Setting {} Web file record{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      if 'MC' not in self.params: # create place hold for MD5 cehcksum
         self.params['MC'] = [None]*self.ALLCNT
         if 'SC' not in self.params: self.OPTS['MC'][2] |= 2
      if 'LC' not in self.params:
         self.params['LC'] = [None]*self.ALLCNT
         self.OPTS['LC'][2] |= 2
      if 'QF' in self.params: self.params['QF'] = self.get_bid_numbers(self.params['QF'])
      fnames = self.get_field_keys(tname, include, "Q")
      if not fnames:
         if not include: self.pglog("Nothing to set for Web file!", self.PGOPT['emlerr'])
         return
      self.validate_multiple_values(tname, self.ALLCNT, fnames)
      self.validate_multiple_options(self.ALLCNT, ["WP"])
      if 'RO' in self.params and 'DO' not in self.params:
         if 'O' not in fnames: fnames += 'O'
         self.params['DO'] = [0]*self.ALLCNT
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} web file record{} processed for set".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
      reorder = metatotal = metacnt = self.ADDCNT = self.MODCNT = 0
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if metacnt >= self.PGOPT['RSMAX']:
               metatotal += self.process_metadata("W", metacnt, self.PGOPT['emerol'])
               metacnt = 0
            if self.PGLOG['DSCHECK'] and metacnt == 0:
               self.set_dscheck_dcount(i, 0, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{} of {} {} Web file record{} added/modified/processed".format(self.ADDCNT, self.MODCNT, i, self.ALLCNT, dsid, s)
         if not self.params['WF'][i]: continue
         if 'WT' in self.params and self.params['WT'][i]:
            type = self.params['WT'][i]
            deftype = 0
         else:
            type = 'D'
            deftype = 1
        # validate web file
         file = self.get_web_path(i, self.params['WF'][i], 0, type)
         pgrec = self.pgget_wfile(dsid, "*", "wfile = '{}'".format(file), self.LGEREX)
         olocflag = pgrec['locflag'] if pgrec else ''
         locflag = self.params['LC'][i]
         if not locflag:
            if not dftloc: dftloc = self.get_dataset_locflag(dsid)
            locflag = self.params['LC'][i] = olocflag if olocflag else dftloc
         if olocflag and locflag != olocflag:
            self.pglog("{}-{}: Cannot reset Web file Location Flag {} to {}".format(dsid, file, olocflag, locflag), self.PGOPT['errlog'])
            continue
         if not self.params['MC'][i] and ('SC' in self.params or not (pgrec and pgrec['checksum'])):
            if locflag != 'O':
               ofile = self.get_web_path(i, self.params['WF'][i], 1, type)
               self.params['MC'][i] = self.get_md5sum(ofile)
            elif 'SC' in self.params:
               self.pglog("{}-{}: Cannot set MD5 checksum for web file on Object Store only".format(dsid, file), self.PGOPT['errlog'])
               continue
         if pgrec and self.params['WF'][i] != file: self.params['WF'][i] = file
         if pgrec and pgrec['type'] and deftype and type != pgrec['type']: type = pgrec['type']
         info = None
         if locflag == 'O' and not pgrec:
            info = self.check_object_file(self.join_paths(dsid, file), self.PGLOG['OBJCTBKT'], 1, self.PGOPT['emlerr'])
         wid = self.set_one_webfile(i, pgrec, file, fnames, type, info)
         if not wid: continue
         wfile = self.get_web_path(i, self.params['WF'][i], 0, type)
         if 'GX' in self.params and self.PGOPT['GXTYP'].find(type) > -1:
            fmt = self.params['DF'][i] if 'DF' in self.params else (pgrec['data_format'] if pgrec else None)
            metacnt += self.record_meta_gather('W', dsid, wfile, fmt)
            self.cache_meta_tindex(dsid, wid, 'W')
         if pgrec and pgrec['meta_link'] and pgrec['meta_link'] != 'N':
            if 'DX' in self.params or self.PGOPT['GXTYP'].find(type) < 0 and self.PGOPT['GXTYP'].find(pgrec['type']) > -1:
               metacnt += self.record_meta_delete('W', dsid, wfile)
            elif 'GI' in self.params:
               gindex = self.params['GI'][i]
               if gindex != pgrec['gindex'] and (gindex or (self.OPTS['GI'][2]&2) == 0):
                  metacnt += self.record_meta_summary('W', dsid, gindex, pgrec['gindex'])
      self.pglog("{}/{} of {} Web file record{} added/modified for {}!".format(self.ADDCNT, self.MODCNT, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if metacnt > 0:
         metatotal += self.process_metadata('W', metacnt, self.PGOPT['emerol'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, 0, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (metatotal + self.ADDCNT + self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ''

   # add or modify one Web file record
   def set_one_webfile(self, i, pgrec, file, flds, type, info = None, ndsid = None, sact = 0):
      tname = 'wfile'
      gindex = (self.params['GI'][i] if 'GI' in self.params and self.OPTS['GI'][2]&2 == 0 else (pgrec['gindex'] if pgrec else 0))
      dsid = ndsid if ndsid else self.params['DS']
      wid = pgrec['wid'] if pgrec else 0
      if not type: type = 'D'
      if 'RO' in self.params and 'DO' in self.params:
         self.params['DO'][i] = self.get_next_disp_order(dsid, gindex)
      record = self.build_record(flds, pgrec, tname, i)
      if pgrec and (pgrec['status'] == 'D' or self.PGOPT['ACTS']&self.OPTS['AW'][0]):
         record['uid'] = self.PGOPT['UID']
         if not (info and info['date_modified']):
            info = self.check_local_file(self.get_web_path(i, file, 1, type),
                                           1, self.PGOPT['emerol']|self.PFSIZE)
         if info:
            record['data_size'] = info['data_size']
            record['date_modified'] = info['date_modified']
            record['time_modified'] = info['time_modified']
            record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
            record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
         else:
            record['date_modified'] = self.curdate()
      if record:
         ccnt = fcnt = 0
         mlink = 0
         if 'meta_link' in record:
            del record['meta_link']
            mlink = 1
         if 'locflag' in record and record['locflag'] == 'R': record['locflag'] = 'G'
         if not ('type' in record or pgrec and pgrec['type'] == type): record['type'] = type
         if not ('vindex' in record or (pgrec and pgrec['vindex'])):
            if dsid not in self.VINDEX: self.VINDEX[dsid] = self.get_version_index(dsid, self.PGOPT['extlog'])
            if self.VINDEX[dsid]: record['vindex'] = self.VINDEX[dsid]
         if 'status' not in record or record['status'] == 'P':
            if gindex and self.get_group_type(dsid, gindex) == 'I':
               if 'status' in record:
                  self.pglog("{}-{}: Keep file status Internal for Internal group {}".format(dsid, file, gindex), self.PGOPT['wrnlog'])
               record['status'] = 'I'
         if 'checksum' in record: self.pglog("md5({}-{})={}".format(dsid, file, record['checksum']), self.LOGWRN)
         if 'gindex' in record and record['gindex']: record['tindex'] = self.get_top_gindex(dsid, record['gindex'])
         stat = self.check_file_flag(file, info, record, pgrec)
         if not stat: return stat
         if pgrec:
            if pgrec['wfile'] != file: record['wfile'] = file
            if 'bid' in record and record['bid'] and pgrec['bid'] and record['bid'] != pgrec['bid']:
               return self.pglog("{}: Cannot change link to backup ID ({}/{})".format(file, record['bid'], pgrec['bid']), self.PGOPT['emlerr'])
            if 'data_format' in record and not record['data_format']:
               del record['data_format']
               if not record: return 0
            if pgrec['status'] == 'D':
               if not ('status' in record and record['status']): record['status'] = 'P'
            ccnt = self.record_webfile_changes(dsid, gindex, record, pgrec)
            if self.pgupdt_wfile_dsid(dsid, pgrec['dsid'], record, pgrec['wid'], self.LGEREX):
               self.MODCNT += 1
               fcnt = 1
               if 'status' in record: self.change_wfile_mode(dsid, file, type, pgrec['status'], record['status'])
            if mlink or pgrec['meta_link'] and pgrec['meta_link'] == 'Y': self.set_meta_link(dsid, file)
         else:
            if record['wfile'] != file: record['wfile'] = file
            record['uid'] = self.PGOPT['UID']
            if 'status' not in record: record['status'] = 'P'
            if not info:
               info = self.check_local_file(self.get_web_path(i, file, 1, type), 1, self.PGOPT['emerol']|self.PFSIZE)
               stat = self.check_file_flag(file, info, record)
               if not stat: return stat
            if info:
               record['data_size'] = info['data_size']
               record['date_modified'] = info['date_modified']
               record['time_modified'] = info['time_modified']
               record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
               record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
            else:
               return self.pglog("{}-{}: {}".format(type, file, self.PGLOG['MISSFILE']), self.PGOPT['emlerr'])
            if 'disp_order' not in record:
               record['disp_order'] = self.get_next_disp_order(dsid, gindex, tname, type)
            ccnt = self.record_webfile_changes(dsid, gindex, record)
            wid = self.pgadd_wfile(dsid, record, self.LGEREX|self.AUTOID)
            if wid:
               self.ADDCNT += 1
               fcnt = 1
         if ccnt:
            if not sact: sact = 4
            self.save_filenumber(None, sact, 1, fcnt)
      return wid

   # check 
   def check_file_flag(self, file, info, record, pgrec = None):
      if not info: return self.SUCCESS
      fflag = 'F' if info['isfile'] else 'P'
      if 'fileflag' in record:
         if fflag != record['fileflag']:
            return self.pglog("{}: Cannot set File Flag '{}' to '{}'".format(file, fflag, record['fileflag']), self.PGOPT['emlerr'])
      elif not (pgrec and pgrec['fileflag'] == fflag):
         record['fileflag'] = fflag
      return self.SUCCESS

   # add or modify Help file information
   def set_helpfile_info(self):
      bidx = 0
      tname = 'hfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Setting {} Help file record{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      if 'MC' not in self.params: # create place hold for MD5 cehcksum
         self.params['MC'] = [None]*self.ALLCNT
         if 'SC' not in self.params: self.OPTS['MC'][2] |= 2
      fnames = self.get_field_keys(tname)
      if not fnames: return self.pglog("Nothing to set for Help file!", self.PGOPT['emlerr'])
      self.validate_multiple_values(tname, self.ALLCNT, fnames)
      if 'RO' in self.params and 'DO' not in self.params:
         if 'O' not in fnames: fnames += 'O'
         self.params['DO'] = [0]*self.ALLCNT
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} help file record{} processed for set".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
      reorder = self.ADDCNT = self.MODCNT = 0
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.add_dscheck_dcount(20, 0, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{} of {} {} Help file record{} added/modified/processed".format(self.ADDCNT, self.MODCNT, i, self.ALLCNT, dsid, s)
         hfile = self.params['HF'][i]
         if not hfile: continue
         type = self.params['HT'][i]
         if self.PGOPT['HFTYP'].find(type) < 0:
            self.pglog("{}-{}: Unknown Help File Type {} to set file information".format(dsid, hfile, type), self.PGOPT['errlog'])
            continue
        # validate help file
         pgrec = self.pgget(tname, "*", "hfile = '{}' AND type = '{}' AND {}".format(hfile, type, dcnd), self.LGEREX)
         olocflag = pgrec['locflag'] if pgrec else ''
         locflag = self.params['LC'][i] if 'LC' in self.params else olocflag
         if olocflag and locflag != olocflag:
            self.pglog("{}-{}: Cannot reset Help file Location Flag {} to {}".format(dsid, hfile, olocflag, locflag), self.PGOPT['errlog'])
            continue
         getmc = 0 if 'WU' in self.params or (pgrec and pgrec['url']) else 1
         if getmc and not self.params['MC'][i] and ('SC' in self.params or not (pgrec and pgrec['checksum'])):
            ofile = self.get_help_path(i, hfile, 1, type)
            self.params['MC'][i] = self.get_md5sum(ofile)
         self.set_one_helpfile(i, pgrec, hfile, fnames, type)
      self.pglog("{}/{} of {} Help file record{} added/modified for {}!".format(self.ADDCNT, self.MODCNT, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']: self.set_dscheck_dcount(self.ALLCNT, 0, self.PGOPT['extlog'])
      if 'ON' in self.params: reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.ADDCNT + self.MODCNT + reorder) > 0: self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ''

   # add or modify one help file record
   def set_one_helpfile(self, i, pgrec, file, flds, type, info = None, ndsid = None):
      tname = 'hfile'
      dsid = ndsid if ndsid else self.params['DS']
      hid = pgrec['hid'] if pgrec else 0
      if 'RO' in self.params and 'DO' in self.params:
         self.params['DO'][i] = self.get_next_disp_order(dsid)
      record = self.build_record(flds, pgrec, tname, i)
      if pgrec and (pgrec['status'] == 'D' or self.PGOPT['ACTS']&self.OPTS['AH'][0]):
         record['uid'] = self.PGOPT['UID']
         if not (info and info['date_modified']):
            info = self.check_local_file(self.get_help_path(i, file, 1, type), 1, self.PGOPT['emerol']|self.PFSIZE)
         if info:
            record['data_size'] = info['data_size']
            record['date_modified'] = info['date_modified']
            record['time_modified'] = info['time_modified']
            record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
            record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
         else:
            record['date_modified'] = self.curdate()
      if ndsid: record['dsid'] = ndsid
      if record:
         ccnt = fcnt = 0
         mlink = 0
         if not ('type' in record or pgrec and pgrec['type'] == type): record['type'] = type
         if 'checksum' in record: self.pglog("md5({}-{})={}".format(dsid, file, record['checksum']), self.LOGWRN)
         if 'url' in record:
            record['locflag'] = 'R'
            record['date_modified'] = self.curdate()
         stat = self.check_file_flag(file, info, record, pgrec)
         if not stat: return stat
         if pgrec:
            if pgrec['hfile'] != file: record['hfile'] = file
            if not ndsid:
               if not pgrec['dsid'] or pgrec['dsid'] == self.PGLOG['DEFDSID']:
                  record['dsid'] = dsid
               elif dsid != pgrec['dsid']:
                  return self.pglog("{}-{}: in {}, Move to {} first".format(type, file, pgrec['dsid'], dsid), self.PGOPT['emlerr'])
            if 'data_format' in record and not record['data_format']:
               del record['data_format']
               if not record: return 0
            if self.pgupdt(tname, record, "hid = {}".format(pgrec['hid']), self.LGEREX):
               self.MODCNT += 1
               fcnt = 1
         else:
            if record['hfile'] != file: record['hfile'] = file
            record['dsid'] = dsid
            record['uid'] = self.PGOPT['UID']
            if 'status' not in record: record['status'] = 'P'
            if not (info or 'url' in record):
               info = self.check_local_file(self.get_help_path(i, file, 1, type), 1, self.PGOPT['emerol']|self.PFSIZE)
               stat = self.check_file_flag(file, info, record, pgrec)
               if not stat: return stat
            if info:
               record['data_size'] = info['data_size']
               record['date_modified'] = info['date_modified']
               record['time_modified'] = info['time_modified']
               record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
               record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
            elif 'url' in record:
               record['locflag'] = 'R'
               record['date_modified'] = self.curdate()
            else:
               return self.pglog("{}-{}: {}".format(type, file, self.PGLOG['MISSFILE']), self.PGOPT['emlerr'])
            if 'disp_order' not in record:
               record['disp_order'] = self.get_next_disp_order(dsid, 0, tname, type)
            hid = self.pgadd(tname, record, self.LGEREX|self.AUTOID)
            if hid:
               self.ADDCNT += 1
               fcnt = 1
      return hid

   # add or modify save file information
   def set_savedfile_info(self):
      bidx = 0
      dftloc = None
      tname = 'sfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Setting {} Saved file record{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      if 'MC' not in self.params: # create place hold for MD5 cehcksum
         self.params['MC'] = [None]*self.ALLCNT
         if 'SC' not in self.params: self.OPTS['MC'][2] |= 2
      if 'LC' not in self.params:
         self.params['LC'] = [None]*self.ALLCNT
         self.OPTS['LC'][2] |= 2
      if 'QF' in self.params: self.params['QF'] = self.get_bid_numbers(self.params['QF'])
      fnames = self.get_field_keys(tname, None, "Q")
      if not fnames: return self.pglog("Nothing to set for Saved file!", self.PGOPT['emlerr'])
      self.validate_multiple_values(tname, self.ALLCNT, fnames)
      self.validate_multiple_options(self.ALLCNT, ["SP"])
      if 'RO' in self.params and 'DO' not in self.params:
         if 'O' not in fnames: fnames += 'O'
         self.params['DO'] = [0]*self.ALLCNT
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Saved file record{} processed for set".format(bidx, self.ALLCNT), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 8)
      reorder = self.ADDCNT = self.MODCNT = 0
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.add_dscheck_dcount(20, 0, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{} of {} {} Saved file record{} added/modified/processed".format(self.ADDCNT, self.MODCNT, i, self.ALLCNT, dsid, s)
         file = self.params['SF'][i]
         if not file: continue
         if 'ST' in self.params and self.params['ST'][i]:
            type = self.params['ST'][i]
            if self.PGOPT['SDTYP'].find(type) < 0:
               self.pglog("{}-{}: Invalid Saved file Type '{}' to set".format(dsid, file, type), self.PGOPT['emerol'])
               continue
         else:
            self.pglog("{}-{}: Miss Saved file Type to Set".format(dsid, file), self.PGOPT['errlog'])
            continue
         typstr = "type = '{}'".format(type)
        # validate saved file
         file = self.get_saved_path(i, file, 0, type)
         pgrec = self.pgget(tname, "*", "{} AND sfile = '{}' AND {}".format(dcnd, file, typstr), self.LGEREX)
         if not pgrec:
            pgrec = self.pgget("sfile", "type", "{} AND sfile = '{}'".format(dcnd, file), self.PGOPT['extlog'])
            if pgrec:
               self.pglog("{}-{}: Fail to set, Move Action -MV to change file Type from '{}' to '{}'".format(dsid, file, pgrec['type'], type), self.PGOPT['emlerr'])
               continue
         olocflag = pgrec['locflag'] if pgrec else ''
         locflag = self.params['LC'][i]
         if not locflag:
            if not dftloc: dftloc = self.get_dataset_locflag(dsid)
            locflag = self.params['LC'][i] = dftloc
         if olocflag and locflag != olocflag:
            self.pglog("{}-{}: Cannot reset Saved file Location Flag '{}' to '{}'".format(dsid, file, olocflag, locflag), self.PGOPT['errlog'])
            continue
         if not self.params['MC'][i] and ('SC' in self.params or not (pgrec and pgrec['checksum'])):
            if locflag != 'O':
               ofile = self.get_saved_path(i, self.params['SF'][i], 1, type)
               self.params['MC'][i] = self.get_md5sum(ofile)
            elif 'SC' in self.params:
               self.pglog("{}-{}: Cannot set MD5 checksum for saved file on Object Store only".format(dsid, file), self.PGOPT['errlog'])
               continue
         if pgrec and self.params['SF'][i] != file: self.params['SF'][i] = file
         info = None
         if locflag == 'O' and not pgrec:
            info = self.check_object_file(self.join_paths(dsid, file), 'gdex-decsdata', 1, self.PGOPT['emlerr'])
         self.set_one_savedfile(i, pgrec, file, fnames, type, info)
      self.pglog("{}/{} of {} Saved file record{} added/modified for {}!".format(self.ADDCNT, self.MODCNT, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, 0, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.ADDCNT + self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ''

   # add or modify one saved file record
   def set_one_savedfile(self, i, pgrec, file, flds, type, info = None, ndsid = None, sact = 0):
      tname = 'sfile'
      gindex = (self.params['GI'][i] if 'GI' in self.params and self.OPTS['GI'][2]&2 == 0 else (pgrec['gindex'] if pgrec else 0))
      dsid = ndsid if ndsid else self.params['DS']
      sid = pgrec['sid'] if pgrec else 0
      if not type: type = 'P'
      if 'RO' in self.params and 'DO' in self.params:
         self.params['DO'][i] = self.get_next_disp_order(dsid, gindex)
      record = self.build_record(flds, pgrec, tname, i)
      if pgrec and self.PGOPT['ACTS']&self.OPTS['AS'][0]:
         record['uid'] = self.PGOPT['UID']
         if not (info and info['date_modified']):
            info = self.check_local_file(self.get_saved_path(i, file, 1, type), 1, self.PGOPT['emerol']|self.PFSIZE)
         if info:
            record['data_size'] = info['data_size']
            record['date_modified'] = info['date_modified']
            record['time_modified'] = info['time_modified']
            record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
            record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
         else:
            record['date_modified'] = self.curdate()
      if ndsid: record['dsid'] = ndsid
      if record:
         ccnt = fcnt = 0
         if 'locflag' in record and record['locflag'] == 'R': record['locflag'] = 'G'
         if not ('type' in record or (pgrec and pgrec['type'] == type)): record['type'] = type
         if not ('vindex' in record or pgrec and pgrec['vindex']):
            if dsid not in self.VINDEX: self.VINDEX[dsid] = self.get_version_index(dsid, self.PGOPT['extlog'])
            if self.VINDEX[dsid]: record['vindex'] = self.VINDEX[dsid]
         if 'status' not in record or record['status'] == 'P':
            if gindex and self.get_group_type(dsid, gindex) == 'I':
               if 'status' in record:
                  self.pglog("{}-{}: Keep file status Internal for Internal group {}".format(dsid, file, gindex), self.PGOPT['wrnlog'])
               record['status'] = 'I'
         if 'checksum' in record and record['checksum']: self.pglog("md5({}-{})={}".format(dsid, file, record['checksum']), self.LOGWRN)
         if 'gindex' in record and record['gindex']: record['tindex'] = self.get_top_gindex(dsid, record['gindex'])
         stat = self.check_file_flag(file, info, record, pgrec)
         if not stat: return stat
         if pgrec:
            if pgrec['sfile'] != file: record['sfile'] = file
            if not ndsid:
               if not pgrec['dsid'] or pgrec['dsid'] == self.PGLOG['DEFDSID']:
                  record['dsid'] = dsid
               elif dsid != pgrec['dsid']:
                  return self.pglog("{}-{}: in {}, Move to {} first".format(type, file, pgrec['dsid'], dsid), self.PGOPT['emlerr'])
            if 'bid' in record and record['bid'] and pgrec['bid'] and record['bid'] != pgrec['bid']:
               return self.pglog("{}: Cannot change link to backup ID ({}/{})".format(file, record['bid'], pgrec['bid']), self.PGOPT['emlerr'])
            if 'data_format' in record and not record['data_format']:
               del record['data_format']
               if not record: return 0
            if pgrec['status'] == 'D':
               if 'status' not in record: record['status'] = 'P'
            ccnt = self.record_savedfile_changes(dsid, gindex, record, pgrec)
            if self.pgupdt(tname, record, "sid = {}".format(pgrec['sid']), self.LGEREX):
               self.MODCNT += 1
               fcnt = 1
         else:
            if record['sfile'] != file: record['sfile'] = file
            record['dsid'] = dsid
            record['uid'] = self.PGOPT['UID']
            if 'status' not in record: record['status'] = 'P'
            if not info:
               info = self.check_local_file(self.get_saved_path(i, file, 1, type), 1, self.PGOPT['emerol']|self.PFSIZE)
               stat = self.check_file_flag(file, info, record, pgrec)
               if not stat: return stat
            if info:
               record['data_size'] = info['data_size']
               record['date_modified'] = info['date_modified']
               record['time_modified'] = info['time_modified']
               record['date_created'] = info['date_created'] if 'date_created' in info else record['date_modified']
               record['time_created'] = info['time_created'] if 'time_created' in info else record['time_modified']
            else:
               return self.pglog("{}-{}: {}".format(type, file, self.PGLOG['MISSFILE']), self.PGOPT['emlerr'])
            if 'disp_order' not in record:
               record['disp_order'] = self.get_next_disp_order(dsid, gindex, tname, type)
            ccnt = self.record_savedfile_changes(dsid, gindex, record)
            sid = self.pgadd("sfile", record, self.LGEREX|self.AUTOID)
            if sid:
               self.ADDCNT += 1
               fcnt = 1
         if ccnt:
            if not sact: sact = 8
            self.save_filenumber(None, sact, 1, fcnt)
      return sid

   # add or modify Quasar backup file information
   def set_backfile_info(self):
      bidx = zipped = 0
      tname = 'bfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Setting {} Backup file record{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      fnames = self.get_field_keys(tname, None)
      if not fnames: return self.pglog("Nothing to set for Backup file!", self.PGOPT['emlerr'])
      self.validate_multiple_values(tname, self.ALLCNT, fnames)
      if 'RO' in self.params and 'DO' not in self.params:
         if 'O' not in fnames: fnames += 'O'
         self.params['DO'] = [0]*self.ALLCNT
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Backup file record{} processed for set".format(bidx, self.ALLCNT), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
      reorder = self.ADDCNT = self.MODCNT = 0
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.add_dscheck_dcount(20, 0, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{} of {} {} Backup file record{} added/modified/processed".format(self.ADDCNT, self.MODCNT, i, self.ALLCNT, dsid, s)
         file = self.params['QF'][i]
         if not file: continue
         if 'QT' in self.params and self.params['QT'][i]:
            type = self.params['QT'][i]
            if 'BD'.find(type) < 0:
               self.pglog("{}-{}: Invalid Backup file Type '{}' to set".format(dsid, file, type), self.PGOPT['emerol'])
               continue
         else:
            self.pglog("{}-{}: Miss Backup file Type to Set".format(dsid, file), self.PGOPT['errlog'])
            continue
         typstr = "type = '{}'".format(type)
        # validate saved file
         pgrec = self.pgget(tname, "*", "{} AND bfile = '{}' AND {}".format(dcnd, file, typstr), self.LGEREX)
         if not pgrec:
            pgrec = self.pgget("sfile", "type", "{} AND bfile = '{}'".format(dcnd, file), self.PGOPT['extlog'])
            if pgrec:
               self.pglog("{}-{}: Fail to set, Move Action -MV to change file Type from '{}' to '{}'".format(dsid, file, pgrec['type'], type), self.PGOPT['emlerr'])
               continue
         self.set_one_backfile(i, pgrec, file, fnames, type)
      self.pglog("{}/{} of {} Backup file record{} added/modified for {}!".format(self.ADDCNT, self.MODCNT, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, 0, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.ADDCNT + self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ''

   # add or modify one Quasar backup file record
   def set_one_backfile(self, i, pgrec, file, flds, type, ndsid = None, record = None):
      tname = 'bfile'
      dsid = ndsid if ndsid else self.params['DS']
      bid = pgrec['bid'] if pgrec else 0
      if 'RO' in self.params and 'DO' in self.params:
         self.params['DO'][i] = self.get_next_disp_order(dsid)
      if not record: record = self.build_record(flds, pgrec, tname, i)
      if ndsid: record['dsid'] = ndsid
      if record:
         if pgrec:
            if 'data_format' in record and not record['data_format']:
               del record['data_format']
               if not record: return 0
            if pgrec['status'] == 'D':
               if 'status' not in record: record['status'] = 'A'
            if self.pgupdt(tname, record, "bid = {}".format(bid), self.LGEREX):
               self.MODCNT += 1
         else:
            if tname not in record or record[tname] != file: record[tname] = file
            record['dsid'] = dsid
            record['uid'] = self.PGOPT['UID']
            if 'status' not in record: record['status'] = 'A'
            info = self.check_backup_file("/{}/{}".format(dsid, file), 'gdex-quasar', 1, self.PGOPT['emerol'])
            if info:
               if not info['isfile']: return self.pglog(file + ": is a directory", self.PGOPT['emlerr'])
               record['data_size'] = info['data_size']
               record['date_created'] = record['date_modified'] = info['date_modified']
               record['time_created'] = record['time_modified'] = info['time_modified']
            else:
               return self.pglog("{}-{}: {}".format(type, file, self.PGLOG['MISSFILE']), self.PGOPT['emlerr'])
            if 'disp_order' not in record:
               record['disp_order'] = self.get_next_disp_order(dsid, 0, tname)
            bid = self.pgadd(tname, record, self.LGEREX|self.AUTOID)
            if bid:
               self.ADDCNT += 1
      return bid

   # moving WEB files tochange file paths/names, and/or from one dataset to another
   def move_web_files(self):
      tname = 'wfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      tmpds = tmpgs = None
      bidx = chksize = 0
      bucket = self.PGLOG['OBJCTBKT']
      rcnt = len(self.PGLOG['WEBHOSTS'])
      s = 's' if self.ALLCNT > 1 else ''
      if 'OD' not in self.params:
         self.params['OD'] = dsid
      elif self.params['OD'] != dsid:
         tmpds = dsid
      if 'GI' not in self.params:
         if 'OG' in self.params:
            self.params['GI'] = self.params['OG']
            self.validate_groups()
      elif 'OG' not in self.params:
           if 'GI' in self.params: self.params['OG'] = self.params['GI']
      elif 'OG' in self.params != self.params['GI']:
         tmpgs = self.params['GI']
      if tmpds:
         self.pglog("Move {} Web file{} from {} to {} ...".format(self.ALLCNT, s, self.params['OD'], dsid), self.WARNLG)
      else:
         self.pglog("Move {} Web file{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      self.validate_multiple_options(self.ALLCNT, ["WT", "OT", "OG", "RF"])
      if 'RF' not in self.params: self.params['RF'] = self.params['WF']
      if 'OT' not in self.params and 'WT' in self.params: self.params['OT'] = self.params['WT']
      if tmpds: self.params['DS'] = self.params['OD']
      if tmpgs: self.params['GI'] = self.params['OG']
      aolds = [None]*self.ALLCNT
      wolds = [None]*self.ALLCNT
      oolds = [None]*self.ALLCNT
      tolds = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         type = self.params['OT'][i] if 'OT' in self.params and self.params['OT'][i] else 'D'
         aolds[i] = self.get_web_path(i, self.params['RF'][i], 5, type)
         wolds[i] = self.get_web_path(i, self.params['RF'][i], 4, type)
         oolds[i] = self.get_object_path(wolds[i], self.params['DS'])
         tolds[i] = type
      if tmpds: self.params['DS'] = tmpds
      if tmpgs: self.params['GI'] = tmpgs
      self.cache_group_info(self.ALLCNT, 1)
      init = 1 if (tmpds or tmpgs) else 0
      anews = [None]*self.ALLCNT
      wnews = [None]*self.ALLCNT
      onews = [None]*self.ALLCNT
      tnews = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         type = self.params['WT'][i] if 'WT' in self.params and self.params['WT'][i] else 'D'
         anews[i] = self.get_web_path(i, self.params['WF'][i], 5, type, init)
         wnews[i] = self.get_web_path(i, self.params['WF'][i], 4, type)
         onews[i] = self.get_object_path(wnews[i], dsid)
         tnews[i] = type
         init = 0
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Web file{} processed for move".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      fnames = "FIT"
      self.validate_multiple_values(tname, self.ALLCNT, fnames)
      reorder = metatotal = metacnt = wcnt = ocnt = self.MODCNT = 0
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if metacnt > self.PGOPT['RSMAX']:
               metatotal += self.process_metadata("W", metacnt, self.PGOPT['emerol'])
               metacnt = 0
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{} of {} Web file{} moved/record-modified/processed".format(wcnt, self.MODCNT, i, self.ALLCNT, s)
         type = tolds[i]
         pgrec = self.pgget_wfile(self.params['OD'], "*", "wfile = '{}' AND type = '{}'".format(wolds[i], type), self.LGEREX)
         if not pgrec:
            self.pglog("{}: Type '{}' Web File not in RDADB for {}".format(wolds[i], type, self.params['OD']), self.PGOPT['emlerr'])
            continue
         elif pgrec['status'] == 'D':
            self.pglog("{}: Type '{}' Web File is not active in RDADB for {}".format(wolds[i], type, dsid), self.PGOPT['emlerr'])
            continue
         elif pgrec['dsid'] != self.params['OD']:
            self.pglog("{}: Web File is actually in {}".format(wolds[i], pgrec['dsid']), self.PGOPT['emlerr'])
            continue
         elif pgrec['vindex'] and tmpds:
            self.pglog(wolds[i] + ": cannot move version controlled Web file to a different dataset", self.PGOPT['emlerr'])
            continue
         elif tmpds and self.pgget("dsvrsn" , "", "{} AND status = 'A'".format(dcnd), self.PGOPT['extlog']):
            self.pglog("{}: cannot move Web file to version controlled dataset {}".format(wnews[i], dsid), self.PGOPT['emlerr'])
            continue
         elif pgrec['locflag'] == 'C':
            self.pglog(wolds[i] + ": Cannot move Web File for CGD data", self.PGOPT['extlog'])
         newrec = self.pgget_wfile(dsid, "*", "wfile = '{}' AND type = '{}'".format(wnews[i], tnews[i]), self.LGEREX)
         if newrec and newrec['status'] != 'D':
            self.pglog("{}: cannot move to existing file {} of {}".format(wolds[i], newrec['wfile'], newrec['dsid']), self.PGOPT['emlerr'])
            continue
         if (pgrec['gindex'] and 'GI' not in self.params and
             not self.pgget("dsgroup", "", "{} and gindex = {}".format(dcnd, pgrec['gindex']), self.PGOPT['extlog'])):
            self.pglog("Group Index {} is not in RDADB for {}\n".format(pgrec['gindex'], dsid) +
                        "Specify Original/New group index via options -OG/-GI", self.PGOPT['extlog'])
         omove = wmove = 1
         if pgrec['locflag'] == 'O':
            wmove = 0
         elif pgrec['locflag'] == 'G':
            omove = 0
         if wmove and aolds[i] != anews[i]:
            ret = self.move_local_file(anews[i], aolds[i], self.PGOPT['emerol']|self.OVERRIDE)
            if not ret:
               self.RETSTAT = 1
               continue
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            wcnt += 1
            self.set_web_move(pgrec)
         if omove and oolds[i] != onews[i]:
            if not self.move_object_file(onews[i], oolds[i], bucket, bucket, self.PGOPT['emerol']|self.OVERRIDE):
               self.RETSTAT = 1
               continue
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            ocnt += 1
         if newrec: self.pgdel_wfile(dsid, "wid = {}".format(newrec['wid']), self.PGOPT['extlog'])
         if not self.set_one_webfile(i, pgrec, wnews[i], fnames, tnews[i], None, tmpds): continue
         if pgrec['bid']: self.save_move_info(pgrec['bid'], wolds[i], type, 'W', self.params['OD'], wnews[i], tnews[i], 'W', dsid)
         if self.PGOPT['GXTYP'].find(type) > -1 and pgrec['meta_link'] and pgrec['meta_link'] != 'N':
            if tmpds or wolds[i] != wnews[i]:
               metacnt += self.record_meta_move('W', self.params['OD'], dsid, wolds[i], wnews[i])
            elif 'GI' in self.params:
               gindex = self.params['GI'][i]
               if gindex != pgrec['gindex'] and (gindex or (self.OPTS['GI'][2]&2) == 0):
                  metacnt += self.record_meta_summary('W', dsid, gindex, pgrec['gindex'])
      self.pglog("{}/{}/{}, Disk/Object/Record, of {} Web file{} moved".format(wcnt, ocnt, self.MODCNT, self.ALLCNT, s), self.PGOPT['emllog'])
      if metacnt > 0:
         metatotal += self.process_metadata('W', metacnt, self.PGOPT['emerol'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, 0, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (metatotal + self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
         if 'OD' in self.params and self.params['OD'] != dsid:
            self.reset_rdadb_version(self.params['OD'])
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ''

   # moving Help files to change file paths/names, and/or from one dataset to another
   def move_help_files(self):
      tname = 'hfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      tmpds = None
      bidx = chksize = 0
      bucket = self.PGLOG['OBJCTBKT']
      rcnt = len(self.PGLOG['WEBHOSTS'])
      s = 's' if self.ALLCNT > 1 else ''
      if 'OD' not in self.params:
         self.params['OD'] = dsid
      elif self.params['OD'] != dsid:
         tmpds = dsid
      if tmpds:
         self.pglog("Move {} Help file{} from {} to {} ...".format(self.ALLCNT, s, self.params['OD'], dsid), self.WARNLG)
      else:
         self.pglog("Move {} Help file{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      self.validate_multiple_options(self.ALLCNT, ["HT", "OT", "RF"])
      if 'RF' not in self.params: self.params['RF'] = self.params['HF']
      if 'OT' not in self.params and 'HT' in self.params: self.params['OT'] = self.params['HT']
      if tmpds: self.params['DS'] = self.params['OD']
      aolds = [None]*self.ALLCNT
      holds = [None]*self.ALLCNT
      oolds = [None]*self.ALLCNT
      tolds = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         type = self.params['OT'][i] if 'OT' in self.params and self.params['OT'][i] else 'D'
         stype = self.HTYPE[type] if type in self.HTYPE else 'Help'
         hpath = self.HPATH[type] if type in self.HPATH else 'help'
         aolds[i] = self.get_help_path(i, self.params['RF'][i], 1, type)
         holds[i] = self.get_help_path(i, self.params['RF'][i], 0, type)
         oolds[i] = self.get_object_path(holds[i], self.params['DS'], hpath)
         tolds[i] = type
      init = 1 if tmpds else 0
      if tmpds: self.params['DS'] = tmpds
      anews = [None]*self.ALLCNT
      hnews = [None]*self.ALLCNT
      onews = [None]*self.ALLCNT
      tnews = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         type = self.params['HT'][i] if 'HT' in self.params and self.params['HT'][i] else 'D'
         stype = self.HTYPE[type] if type in self.HTYPE else 'Help'
         hpath = self.HPATH[type] if type in self.HPATH else 'help'
         anews[i] = self.get_help_path(i, self.params['HF'][i], 1, type, init)
         hnews[i] = self.get_help_path(i, self.params['HF'][i], 0, type)
         onews[i] = self.get_object_path(hnews[i], dsid, hpath)
         tnews[i] = type
         init = 0
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Help file{} processed for move".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            chksize = self.PGLOG['DSCHECK']['size']
      fnames = "FT"
      self.validate_multiple_values(tname, self.ALLCNT, fnames)
      reorder = hcnt = ocnt = self.MODCNT = 0
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{} of {} Help file{} moved/record-modified/processed".format(hcnt, self.MODCNT, i, self.ALLCNT, s)
         type = tolds[i]
         pgrec = self.pgget("hfile", "*", "hfile = '{}' AND type = '{}' AND dsid = '{}'".format(holds[i], type, self.params['OD']), self.LGEREX)
         if not pgrec:
            self.pglog("{}: Type '{}' Help File not in RDADB for {}".format(holds[i], type, self.params['OD']), self.PGOPT['emlerr'])
            continue
         newrec = self.pgget(tname, "*", "hfile = '{}' AND type = '{}' AND {}".format(hnews[i], tnews[i], dcnd), self.LGEREX)
         if newrec:
            self.pglog("{}: cannot move to existing file {} of {}".format(holds[i], newrec['hfile'], newrec['dsid']), self.PGOPT['emlerr'])
            continue
         if pgrec['url']:
            self.pglog("{}-{}: Cannot move Help file on remote URL".format(holds[i], pgrec['url']), self.PGOPT['errlog'])
            self.params['LF'][i] = self.params['HF'][i] = None
            continue
         omove = hmove = 1
         if pgrec['locflag'] == 'O':
            hmove = 0
         elif pgrec['locflag'] == 'G':
            omove = 0
         if hmove and aolds[i] != anews[i]:
            ret = self.move_local_file(anews[i], aolds[i], self.PGOPT['emerol']|self.OVERRIDE)
            if not ret:
               self.RETSTAT = 1
               continue
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            hcnt += 1
         if omove and oolds[i] != onews[i]:
            if not self.move_object_file(onews[i], oolds[i], bucket, bucket, self.PGOPT['emerol']|self.OVERRIDE):
               self.RETSTAT = 1
               continue
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            ocnt += 1
         if not self.set_one_helpfile(i, pgrec, hnews[i], fnames, tnews[i], None, tmpds): continue
      self.pglog("{}/{}/{}/{}, Disk/Object/Record, of {} Help file{} moved".format(hcnt, ocnt, self.MODCNT, self.ALLCNT, s), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']: self.set_dscheck_dcount(self.ALLCNT, 0, self.PGOPT['extlog'])
      if 'ON' in self.params: reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
         if 'OD' in self.params and self.params['OD'] != dsid:
            self.reset_rdadb_version(self.params['OD'])
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ''

   # moving Saved files to Web files, both on glade and object store
   def saved_to_web_files(self):
      tname = 'wfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      frombucket = "gdex-decsdata"
      tobucket = self.PGLOG['OBJCTBKT']
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      dslocflags = set()
      tmpds = tmpgs = None
      if 'OD' not in self.params:
         self.params['OD'] = dsid
      elif self.params['OD'] != dsid:
         tmpds = dsid
      if 'GI' not in self.params:
         if 'OG' in self.params:
            self.params['GI'] = self.params['OG']
            self.validate_groups()
      elif 'OG' not in self.params:
           if 'GI' in self.params: self.params['OG'] = self.params['GI']
      elif 'OG' in self.params != self.params['GI']:
         tmpgs = self.params['GI']
      if tmpds:
         self.pglog("Move {} Saved to Web file{} from {} to {} ...".format(self.ALLCNT, s, self.params['OD'], dsid), self.WARNLG)
      else:
         self.pglog("Move {} Saved to Web file{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      self.validate_multiple_options(self.ALLCNT, ["WT", "OT", "OG", "RF"])
      if 'RF' not in self.params: self.params['RF'] = (self.params['SF'] if 'SF' in self.params else self.params['WF'])
      if 'OT' not in self.params and 'ST' in self.params: self.params['OT'] = self.params['ST']
      if tmpds: self.params['DS'] = self.params['OD']
      if tmpgs: self.params['GI'] = self.params['OG']
      aolds = [None]*self.ALLCNT
      solds = [None]*self.ALLCNT
      oolds = [None]*self.ALLCNT
      tolds = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         type = self.params['OT'][i] if 'OT' in self.params and self.params['OT'][i] else 'V'
         aolds[i] = self.get_saved_path(i, self.params['RF'][i], 5, type)
         solds[i] = self.get_saved_path(i, self.params['RF'][i], 4, type)
         oolds[i] = self.join_paths(self.params['DS'], solds[i])
         tolds[i] = type
      if tmpds: self.params['DS'] = tmpds
      if tmpgs: self.params['GI'] = tmpgs
      if 'WF' not in self.params: self.params['WF'] = (self.params['SF'] if 'SF' in self.params else self.params['RF'])
      self.cache_group_info(self.ALLCNT, 1)
      init = 1 if (tmpds or tmpgs) else 0
      anews = [None]*self.ALLCNT
      wnews = [None]*self.ALLCNT
      onews = [None]*self.ALLCNT
      tnews = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         type = self.params['WT'][i] if 'WT' in self.params and self.params['WT'][i] else 'D'
         anews[i] = self.get_web_path(i, self.params['WF'][i], 5, type, init)
         wnews[i] = self.get_web_path(i, self.params['WF'][i], 4, type)
         onews[i] = self.join_paths(dsid, wnews[i])
         tnews[i] = type
         init = 0
      self.validate_multiple_values(tname, self.ALLCNT)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Saved to Web file{} processed for move".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      if 'QF' in self.params:
         self.params['QF'] = self.get_bid_numbers(self.params['QF'])
      else:
         self.params['QF'] = [0]*self.ALLCNT
         self.OPTS['QF'][2] |= 2
      if 'GI' not in self.params: self.params['GI'] = [0]*self.ALLCNT
      if 'SZ' not in self.params: self.params['SZ'] = [0]*self.ALLCNT
      if 'DF' not in self.params: self.params['DF'] = [None]*self.ALLCNT
      if 'AF' not in self.params: self.params['AF'] = [None]*self.ALLCNT
      if 'LC' not in self.params: self.params['LC'] = [None]*self.ALLCNT
      if 'MC' not in self.params: self.params['MC'] = [None]*self.ALLCNT
      if 'DE' not in self.params: self.params['DE'] = [None]*self.ALLCNT
      fnames = None
      reorder = metatotal = metacnt = wcnt = ocnt = dcnt = self.ADDCNT = self.MODCNT = 0
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if metacnt > self.PGOPT['RSMAX']:
               metatotal += self.process_metadata("W", metacnt, self.PGOPT['emerol'])
               metacnt = 0
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{}/{}/{}, Disk/Object/RecordDelete/RecordAdd/Proccessed, of {} Saved file{} moved".format(wcnt, ocnt, dcnt, self.ADDCNT, i, self.ALLCNT, s)
         type = tolds[i]
         pgrec = self.pgget("sfile", "*", "sfile = '{}' AND dsid = '{}' AND type = '{}'".format(solds[i], self.params['OD'], type), self.LGEREX)
         if not pgrec:
            self.pglog("{}: Type '{}' Saved File not in RDADB for {}".format(solds[i], type, self.params['OD']), self.PGOPT['emlerr'])
            continue
         if tnews[i] == 'O' or tnews[i] == 'S':
            self.pglog("{}: Cannot move Saved File to Web Type '{}' in {}".format(solds[i], tnews[i], dsid), self.PGOPT['emlerr'])
            continue
         elif pgrec['locflag'] == 'C':
            self.pglog(solds[i] + ": Cannot move Saved File to Web file for CGD data", self.PGOPT['extlog'])
         newrec = self.pgget_wfile(dsid, "*", "wfile = '{}'".format(wnews[i]), self.LGEREX)
         if newrec and newrec['status'] != 'D':
            self.pglog("{}: cannot move Saved to existing Web file {} of {}".format(solds[i], newrec['wfile'], newrec['dsid']), self.PGOPT['emlerr'])
            continue
         if (pgrec['gindex'] and not ('GI' in self.params and self.params['GI'][i]) and
             not self.pgget("dsgroup", "", "{} and gindex = {}".format(dcnd, pgrec['gindex']), self.PGOPT['extlog'])):
            self.pglog("Group Index {} is not in RDADB for {}\n".format(pgrec['gindex'], dsid) +
                        "Specify Original/New group index via options -OG/-GI", self.PGOPT['extlog'])
         sfrom = omove = wmove = 1
         ofrom = 0
         locflag = 'G'
   #      locflag = pgrec['locflag']
   #      if locflag == 'O':
   #         sfrom = 0
   #      elif locflag == 'G':
   #         ofrom = 0
         if not self.params['LC'][i] or self.params['LC'][i] == 'R':
            self.params['LC'][i] = locflag
         else:
            locflag = self.params['LC'][i]
         if locflag == 'O':
            wmove = 0
            dslocflags.add('O')
         elif locflag == 'G':
            omove = 0
            dslocflags.add('G')
         if wmove:
            if sfrom:
               stat = self.move_local_file(anews[i], aolds[i], self.PGOPT['emerol']|self.OVERRIDE)
               sfrom = 0
            else:
               stat = self.object_copy_local(anews[i], oolds[i], frombucket, self.PGOPT['emerol']|self.OVERRIDE)
            if not stat:
               self.RETSTAT = 1
               continue
            wcnt += 1
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         if omove:
            if sfrom:
               stat = self.local_copy_object(onews[i], aolds[i], tobucket, None, self.PGOPT['emerol']|self.OVERRIDE)
            elif wmove:
               stat = self.local_copy_object(onews[i], anews[i], tobucket, None, self.PGOPT['emerol']|self.OVERRIDE)
            else:
               stat = self.move_object_file(onews[i], oolds[i], tobucket, frombucket, self.PGOPT['emerol']|self.OVERRIDE)
               ofrom = 0
            if not stat:
               self.RETSTAT = 1
               continue
            ocnt += 1
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         if sfrom: self.delete_local_file(aolds[i], self.PGOPT['emerol'])
         if ofrom: self.delete_object_file(oolds[i], frombucket, self.PGOPT['emerol'])
         if pgrec['bid'] and not self.params['QF'][i]: self.params['QF'][i] = pgrec['bid']
         if pgrec['gindex'] and not self.params['GI'][i]: self.params['GI'][i] = pgrec['gindex']
         if pgrec['data_size'] and not self.params['SZ'][i]: self.params['SZ'][i] = pgrec['data_size']
         if pgrec['data_format'] and not self.params['DF'][i]: self.params['DF'][i] = pgrec['data_format']
         if pgrec['file_format'] and not self.params['AF'][i]: self.params['AF'][i] = pgrec['file_format']
         if pgrec['checksum'] and not self.params['MC'][i]: self.params['MC'][i] = pgrec['checksum']
         if pgrec['note'] and not self.params['DE'][i]: self.params['DE'][i] = pgrec['note']
         if not fnames: fnames = self.get_field_keys(tname, None, "G")
         self.record_filenumber(pgrec['dsid'], pgrec['gindex'], 8, pgrec['type'], -1, -pgrec['data_size'])
         dcnt += self.pgdel_sfile("sid = {}".format(pgrec['sid']), self.PGOPT['extlog'])
         info = self.get_file_origin_info(wnews[i], pgrec)
         wid = self.set_one_webfile(i, newrec, wnews[i], fnames, tnews[i], info, dsid, 12)
         if not wid: continue
         if pgrec['bid']: self.save_move_info(pgrec['bid'], solds[i], type, 'S', self.params['OD'], wnews[i], tnews[i], 'W', dsid)
         if 'GX' in self.params and self.PGOPT['GXTYP'].find(type) > -1:
            metacnt += self.record_meta_gather('W', dsid, wnews[i], self.params['DF'][i])
            self.cache_meta_tindex(dsid, wid, 'W')
      self.pglog("{}/{}/{}/{}, Disk/Object/RecordDelete/RecordAdd, of {} Saved file{} moved".format(wcnt, ocnt, dcnt, self.ADDCNT, self.ALLCNT, s), self.PGOPT['emllog'])
      if metacnt > 0:
         metatotal += self.process_metadata('W', metacnt, self.PGOPT['emerol'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, 0, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (metatotal + reorder + dcnt + self.ADDCNT + self.MODCNT) > 0:
         self.reset_rdadb_version(dsid)
         if 'OD' in self.params and self.params['OD'] != dsid: self.reset_rdadb_version(self.params['OD'])
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""
      if dslocflags: self.set_dataset_locflag(dsid, dslocflags.pop())

   # delete Web files from a given dataset
   def delete_web_files(self):
      tname = 'wfile'
      dsid = self.params['DS']
      bucket = self.PGLOG['OBJCTBKT']
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      rcnt = len(self.PGLOG['WEBHOSTS'])
      self.pglog("Delete {} Web file{} from {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      reorder = metatotal = metacnt = dcnt = mcnt = ocnt = wcnt = 0
      self.cache_group_info(self.ALLCNT, 0)
      self.validate_multiple_options(self.ALLCNT, ["WT", 'VI', 'QF', 'LC'])
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Web file{} processed for delete".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if metacnt >= self.PGOPT['RSMAX']:
               metatotal += self.process_meta_delete("W", self.PGOPT['emerol'])
               metacnt = 0
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{}/{}/{}, Disk/Object/DeleteRecord/ModifyRecord/Proccessed,of {} Web file{} deleted".format(wcnt, ocnt, dcnt, mcnt, i, self.ALLCNT, s)
         if 'WT' in self.params and self.params['WT'][i]:
            type = self.params['WT'][i]
         else:
            type = 'D'
         wfile = self.get_web_path(i, self.params['WF'][i], 0, type)
         pgrec = self.pgget_wfile(dsid, "*", "wfile = '{}' AND type = '{}'".format(wfile, type), self.LGEREX)
         if not pgrec:
            self.pglog("{}: type '{}' Web file is not in RDADB".format(wfile, type), self.PGOPT['errlog'])
            continue
         odel = wdel = oflag = wflag = 1
         locflag = pgrec['locflag']
         if locflag == 'O':
            wflag = 0
         elif locflag == 'G':
            oflag = 0
         elif locflag == 'C':
            wflag = oflag = 0
         if 'LC' in self.params and self.params['LC'][i]: locflag = self.params['LC'][i]
         if locflag == 'O':
            wdel = 0
         elif locflag == 'G':
            odel = 0
         elif locflag == 'C':
            wdel = odel = 0
         if (wflag+oflag) == (wdel+odel):
            vindex = self.params['VI'][i] if 'VI' in self.params else pgrec['vindex']
            if vindex:
               self.pglog(wfile + ": Web file is version controlled, add option -vi 0 to force delete", self.PGOPT['errlog'])
               continue
            bid = self.params['QF'][i] if 'QF' in self.params else pgrec['bid']
            if bid:
               self.pglog(wfile + ": Web file is Quasar backed up, add option -qf '' to force delete", self.PGOPT['errlog'])
               continue
            if self.PGOPT['GXTYP'].find(type) > -1 and ('DX' in self.params or pgrec and pgrec['meta_link'] and pgrec['meta_link'] != 'N'):
               metacnt += self.record_meta_delete('W', dsid, wfile)
         if wdel:
            afile = self.get_web_path(i, self.params['WF'][i], 1, type)
            fcnt = self.delete_local_file(afile, self.PGOPT['emerol'])
            if fcnt:
               wcnt += 1
               wflag = 0
               if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            elif self.check_local_file(afile) is None:
               wflag = 0
         if odel:
            ofile = self.get_object_path(wfile, dsid)
            if self.delete_object_file(ofile, bucket, self.PGOPT['emerol']):
               ocnt += 1
               oflag = 0
               if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            elif self.check_object_file(ofile, bucket) is None:
               oflag = 0
         wcnd = "wid = {}".format(pgrec['wid'])
         if (oflag + wflag) > 0:
            locflag = "O" if oflag else "G"
            lrec = {'locflag' : locflag}
            mcnt += self.pgupdt_wfile(dsid, lrec, wcnd, self.LGEREX)
         else:
            ccnt = self.record_filenumber(dsid, pgrec['gindex'], 4, (pgrec['type'] if pgrec['status'] == 'P' else ''), -1, -pgrec['data_size'])
            fcnt = self.pgdel_wfile(dsid, wcnd, self.LGEREX)
            if fcnt: dcnt += fcnt
            if ccnt: self.save_filenumber(dsid, 4, 1, fcnt)
      if (wcnt + ocnt + dcnt + mcnt) > 0:
         self.pglog("{}/{}/{}/{}, Disk/Object/DeleteRecord/ModifyRecord, of {} Web file{} deleted for {}".format(wcnt, ocnt, dcnt, mcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if metacnt > 0:
         metatotal += self.process_meta_delete('W', self.PGOPT['emerol'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (metatotal + dcnt + mcnt + reorder) > 0:
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # delete Help files from a given dataset
   def delete_help_files(self):
      tname = 'hfile'
      dsid = self.params['DS']
      bucket = self.PGLOG['OBJCTBKT']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      rcnt = len(self.PGLOG['WEBHOSTS'])
      self.pglog("Delete {} Help file{} from {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      reorder = dcnt = hcnt = ocnt = mcnt = 0
      self.cache_group_info(self.ALLCNT, 0)
      self.validate_multiple_options(self.ALLCNT, ["HT", 'LC'])
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Help file{} processed for delete".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            chksize = self.PGLOG['DSCHECK']['size']
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{}/{}/{}, Disk/Record/Proccessed,of {} Help file{} deleted".format(hcnt, dcnt, mcnt, i, self.ALLCNT, s)
         hfile = self.params['HF'][i]
         if 'HT' in self.params and self.params['HT'][i]:
            type = self.params['HT'][i]
         else:
            self.pglog("{}: Specify Help file Type via Option -HT to delete".format(hfile), self.PGOPT['errlog'])
            continue
         stype = self.HTYPE[type] if type in self.HTYPE else 'Help'
         hpath = self.HPATH[type] if type in self.HPATH else 'help'
         pgrec = self.pgget(tname, "*", "hfile = '{}' AND type = '{}' AND {}".format(hfile, type, dcnd), self.LGEREX)
         if not pgrec:
            self.pglog("{}: {} file is not in RDADB".format(hfile, stype), self.PGOPT['errlog'])
            continue
         if not type: type = pgrec['type']
         odel = hdel = oflag = hflag = 1
         locflag = pgrec['locflag']
         if locflag == 'O':
            hflag = 0
         elif locflag == 'G':
            oflag = 0
         if 'LC' in self.params and self.params['LC'][i]: locflag = self.params['LC'][i]
         if locflag == 'O':
            hdel = 0
         elif locflag == 'G':
            odel = 0
         if hdel:
            afile = self.get_help_path(i, hfile, 1, type)
            if pgrec['url']:
               fcnt = 1
            else:
               fcnt = 0
               for j in range(rcnt):
                  fcnt += self.delete_local_file(afile, self.PGOPT['emerol'])
            if fcnt:
               hcnt += 1
               hflag = 0
               if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            elif self.check_local_file(afile) is None:
               hflag = 0
         if odel:
            ofile = self.get_object_path(hfile, dsid, hpath)
            if self.delete_object_file(ofile, bucket, self.PGOPT['emerol']):
               ocnt += 1
               oflag = 0
               if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            elif self.check_object_file(ofile, bucket) is None:
               oflag = 0
         if (oflag + hflag) > 0:
            locflag = "O" if oflag else "G"
            mcnt += self.pgexec("UPDATE hfile SET locflag = '{}' WHERE hid = {}".format(locflag, pgrec['hid']), self.LGEREX)
         else:
            dcnt += self.pgdel(tname, "hid = {}".format(pgrec['hid']), self.LGEREX)
      if (hcnt + ocnt + dcnt + mcnt) > 0:
         self.pglog("{}/{}/{}/{}, Disk/Object/DeleteRecord/ModifyRecord, of {} Web file{} deleted for {}".format(hcnt, ocnt, dcnt, mcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']: self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'ON' in self.params: reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (dcnt + reorder) > 0: self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # moving Saved files to change file paths/names, and/or from one dataset to another
   def move_saved_files(self):
      tname = 'sfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      bucket = "gdex-decsdata"
      s = 's' if self.ALLCNT > 1 else ''
      bidx = chksize = 0
      tmpds = tmpgs = None
      if 'OD' not in self.params:
         self.params['OD'] = dsid
      elif self.params['OD'] != dsid:
         tmpds = dsid
      if 'GI' not in self.params:
         if 'OG' in self.params:
            self.params['GI'] = self.params['OG']
            self.validate_groups()
      elif 'OG' not in self.params:
           if 'GI' in self.params: self.params['OG'] = self.params['GI']
      elif 'OG' in self.params != self.params['GI']:
         tmpgs = self.params['GI']
      if tmpds:
         self.pglog("Move {} Saved file{} from {} to {} ...".format(self.ALLCNT, s, self.params['OD'], dsid), self.WARNLG)
      else:
         self.pglog("Move {} Saved file{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      self.validate_multiple_options(self.ALLCNT, ["ST", "OT", "OG", "RF"])
      if 'RF' not in self.params: self.params['RF'] = self.params['SF']
      if 'OT' not in self.params and 'ST' in self.params: self.params['OT'] = self.params['ST']
      if tmpds: self.params['DS'] = self.params['OD']
      if tmpgs: self.params['GI'] = self.params['OG']
      aolds = [None]*self.ALLCNT
      solds = [None]*self.ALLCNT
      oolds = [None]*self.ALLCNT
      tolds = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         type = self.params['OT'][i] if 'OT' in self.params and self.params['OT'][i] else 'P'
         aolds[i] = self.get_saved_path(i, self.params['RF'][i], 5, type)
         solds[i] = self.get_saved_path(i, self.params['RF'][i], 4, type)
         oolds[i] = self.get_object_path(solds[i], self.params['DS'])
         tolds[i] = type
      if tmpds: self.params['DS'] = tmpds
      if tmpgs: self.params['GI'] = tmpgs
      self.cache_group_info(self.ALLCNT, 1)
      init = 1 if (tmpds or tmpgs) else 0
      anews = [None]*self.ALLCNT
      snews = [None]*self.ALLCNT
      onews = [None]*self.ALLCNT
      tnews = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         type = self.params['ST'][i] if 'ST' in self.params and self.params['ST'][i] else 'P'
         anews[i] = self.get_saved_path(i, self.params['SF'][i], 5, type, init)
         snews[i] = self.get_saved_path(i, self.params['SF'][i], 4, type)
         onews[i] = self.get_object_path(snews[i], dsid)
         tnews[i] = type
         init = 0
      fnames = "FIT"
      self.validate_multiple_values(tname, self.ALLCNT, fnames)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Saved file{} moved".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      reorder = self.MODCNT = scnt = ocnt = 0
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{}/{}, Disk/Object/Record/Proccessed, of {}self.ALLCNT Saved files moved".format(scnt, ocnt, self.MODCNT, i, self.ALLCNT, s)
         type = tolds[i]
         pgrec = self.pgget(tname, "*", "sfile = '{}' AND dsid = '{}' AND type = '{}'".format(solds[i], self.params['OD'], type), self.LGEREX)
         if not pgrec:
            self.pglog("{}: Type '{}' Saved File not in RDADB for {}".format(solds[i], type, self.params['OD']), self.PGOPT['emlerr'])
            continue
         elif pgrec['dsid'] != self.params['OD']:
            self.pglog("{}: Saved File is actually in {}".format(solds[i], pgrec['dsid']), self.PGOPT['emlerr'])
            continue
         elif pgrec['vindex'] and tmpds:
            self.pglog(solds[i] + ": cannot move version controlled Saved file to a different dataset", self.PGOPT['emlerr'])
            continue
         elif tmpds and self.pgget("dsvrsn" , "", "{} AND status = 'A'".format(dcnd), self.PGOPT['extlog']):
            self.pglog("{}: cannot move Saved file to version controlled dataset {}".format(snews[i], dsid), self.PGOPT['emlerr'])
            continue
         elif pgrec['locflag'] == 'C':
            self.pglog(solds[i] + ": Cannot move Saved File for CGD data", self.PGOPT['extlog'])
         newrec = self.pgget(tname, "*", "sfile = '{}' AND {} AND type = '{}'".format(snews[i], dcnd, tnews[i]), self.LGEREX)
         if newrec:
            self.pglog("{}: cannot move to existing file {} of {}".format(solds[i], newrec['sfile'], newrec['dsid']), self.PGOPT['emlerr'])
            continue
         if (pgrec['gindex'] and not self.params['GI'] and
             not self.pgget("dsgroup", "", "{} and gindex = {}".format(dcnd, pgrec['gindex']), self.PGOPT['extlog'])):
            self.pglog("Group Index {} is not in RDADB for {}\n".format(pgrec['gindex'], dsid) +
                        "Specify Original/New group index via options -OG/-GI", self.PGOPT['extlog'])
         omove = 0
         smove = 1
   #      if pgrec['locflag'] == 'O':
   #         smove = 0
   #      elif pgrec['locflag'] == 'G':
   #         omove = 0
         if smove and aolds[i] != anews[i]:
            if not self.move_local_file(anews[i], aolds[i], self.PGOPT['emerol']|self.OVERRIDE):
               self.RETSTAT = 1
               continue
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            scnt += 1
         if omove and oolds[i] != onews[i]:
            if not self.move_object_file(onews[i], oolds[i], bucket, bucket, self.PGOPT['emerol']|self.OVERRIDE):
               self.RETSTAT = 1
               continue
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            ocnt += 1
         self.set_one_savedfile(i, pgrec, snews[i], fnames, tnews[i], None, tmpds)
         if pgrec['bid']: self.save_move_info(pgrec['bid'], solds[i], type, 'S', self.params['OD'], snews[i], tnews[i], 'S', dsid)
      self.pglog("{}/{}/{}, Disk/Object/Record, of {} Saved file{} moved".format(scnt, ocnt, self.MODCNT, self.ALLCNT, s), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
         if 'OD' in self.params and self.params['OD'] != dsid:
            self.reset_rdadb_version(self.params['OD'])
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""  

   # moving Quasar backup files from one dataset to another
   def move_backup_files(self):
      s = 's' if self.ALLCNT > 1 else ''
      tname = 'bfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      bkend = "gdex-quasar"
      drend = 'gdex-quasar-drdata'
      bidx = chksize = 0
      tmpds = None
      if 'QT' not in self.params: self.pglog("Miss File Type per -QT to move Quasar Backup files", self.PGOPT['extlog'])
      if 'OD' not in self.params:
         self.params['OD'] = dsid
      elif self.params['OD'] != dsid:
         tmpds = dsid
      if tmpds:
         self.pglog("Move {} Backup file{} from {} to {} ...".format(self.ALLCNT, s, self.params['OD'], dsid), self.WARNLG)
      else:
         self.pglog("Move {} Backup file{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      self.validate_multiple_options(self.ALLCNT, ["QT", "OT", "RF"])
      if 'RF' not in self.params: self.params['RF'] = self.params['QF']
      if 'OT' not in self.params and 'QT' in self.params: self.params['OT'] = self.params['QT']
      if tmpds: self.params['DS'] = self.params['OD']
      qolds = [None]*self.ALLCNT
      bolds = [None]*self.ALLCNT
      tolds = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         bolds[i] = self.params['RF'][i]
         qolds[i] = "/{}/{}".format(self.params['OD'], bolds[i])
         tolds[i] = self.params['OT'][i]
      if tmpds: self.params['DS'] = tmpds
      qnews = [None]*self.ALLCNT
      bnews = [None]*self.ALLCNT
      tnews = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         bnews[i] = self.params['QF'][i]
         qnews[i] = "/{}/{}".format(dsid, bnews[i])
         tnews[i] = self.params['QT'][i]
      fnames = "FT"
      self.validate_multiple_values(tname, self.ALLCNT, fnames)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Backup file{} moved".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      reorder = self.MODCNT = bcnt = dcnt = 0
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{}/{}, Quasar/Drdata/Record/Proccessed, of {}self.ALLCNT Backup files moved".format(bcnt, dcnt, self.MODCNT, i, self.ALLCNT, s)
         type = tolds[i]
         ntype = tnews[i]
         pgrec = self.pgget(tname, "*", "bfile = '{}' AND dsid = '{}' AND type = '{}'".format(bolds[i], self.params['OD'], type), self.LGEREX)
         if not pgrec:
            self.pglog("{}: Type '{}' Backup File not in RDADB for {}".format(bolds[i], type, self.params['OD']), self.PGOPT['emlerr'])
            continue
         elif pgrec['dsid'] != self.params['OD']:
            self.pglog("{}: Backup File is actually in {}".format(bolds[i], pgrec['dsid']), self.PGOPT['emlerr'])
            continue
         elif type != ntype:
            self.pglog("{}: Type '{}' Backup File cannot be moved to '{}'".format(bolds[i], type, ntype), self.PGOPT['emlerr'])
            continue
         newrec = self.pgget(tname, "*", "bfile = '{}' AND {} AND type = '{}'".format(bnews[i], dcnd, ntype), self.LGEREX)
         if newrec:
            self.pglog("{}: cannot move to existing file {} of {}".format(bolds[i], newrec['bfile'], newrec['dsid']), self.PGOPT['emlerr'])
            continue
         dmove = bmove = 1
         if type == 'B': dmove = 0
         if qolds[i] != qnews[i]:
            if bmove:
               if not self.move_backup_file(qnews[i], qolds[i], bkend, self.PGOPT['emerol']|self.OVERRIDE):
                  self.RETSTAT = 1
                  continue
               if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
               bcnt += 1
            if dmove:
               if not self.move_backup_file(qnews[i], qolds[i], drend, self.PGOPT['emerol']|self.OVERRIDE):
                  self.RETSTAT = 1
                  continue
               if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
               dcnt += 1
         self.set_one_backfile(i, pgrec, bnews[i], fnames, tnews[i], tmpds)
      self.pglog("{}/{}/{}, Quasar/Drdata/Record, of {} Backup file{} moved".format(bcnt, dcnt, self.MODCNT, self.ALLCNT, s), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (self.MODCNT + reorder) > 0:
         self.reset_rdadb_version(dsid)
         if 'OD' in self.params and self.params['OD'] != dsid:
            self.reset_rdadb_version(self.params['OD'])
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""  

   # moving Web files to Saved files, for files both on glade and object store
   def web_to_saved_files(self):
      s = 's' if self.ALLCNT > 1 else ''
      tname = 'sfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      frombucket = self.PGLOG['OBJCTBKT']
      tobucket = "gdex-decsdata"
      bidx = chksize = 0
      tmpds = tmpgs = None
      if 'OD' not in self.params:
         self.params['OD'] = dsid
      elif self.params['OD'] != dsid:
         tmpds = dsid
      if 'GI' not in self.params:
         if 'OG' in self.params:
            self.params['GI'] = self.params['OG']
            self.validate_groups()
      elif 'OG' not in self.params:
           if 'GI' in self.params: self.params['OG'] = self.params['GI']
      elif 'OG' in self.params != self.params['GI']:
         tmpgs = self.params['GI']
      if tmpds:
         self.pglog("Move {} Web to Saved file{} from {} to {} ...".format(self.ALLCNT, s, self.params['OD'], dsid), self.WARNLG)
      else:
         self.pglog("Move {} Web to Saved file{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      if 'RF' not in self.params: self.params['RF'] = (self.params['WF'] if 'WF' in self.params else self.params['SF'])
      if 'OT' not in self.params and 'WT' in self.params: self.params['OT'] = self.params['WT']
      self.validate_multiple_options(self.ALLCNT, ["ST", "OT", "OG", "RF"])
      if tmpds: self.params['DS'] = self.params['OD']
      if tmpgs: self.params['GI'] = self.params['OG']
      aolds = [None]*self.ALLCNT
      wolds = [None]*self.ALLCNT
      oolds = [None]*self.ALLCNT
      tolds = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         type = self.params['OT'][i] if 'OT' in self.params and self.params['OT'][i] else 'D'
         aolds[i] = self.get_web_path(i, self.params['RF'][i], 5, type)
         wolds[i] = self.get_web_path(i, self.params['RF'][i], 4, type)
         oolds[i] = self.join_paths(self.params['DS'], wolds[i])
         tolds[i] = type
      if tmpds: self.params['DS'] = tmpds
      if tmpgs: self.params['GI'] = tmpgs
      if 'SF' not in self.params: self.params['SF'] = (self.params['WF'] if 'WF' in self.params else self.params['RF'])
      self.cache_group_info(self.ALLCNT, 1)
      init = 1 if (tmpds or tmpgs) else 0
      anews = [None]*self.ALLCNT
      snews = [None]*self.ALLCNT
      onews = [None]*self.ALLCNT
      tnews = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         type = self.params['ST'][i] if 'ST' in self.params and self.params['ST'][i] else 'V'
         anews[i] = self.get_saved_path(i, self.params['SF'][i], 5, type, init)
         snews[i] = self.get_saved_path(i, self.params['SF'][i], 4, type)
         onews[i] = self.join_paths(dsid, snews[i])
         tnews[i] = type
         init = 0
      self.validate_multiple_values(tname, self.ALLCNT)
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("bidx of self.ALLCNT Web to Saved files processed for move", self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 4)
            chksize = self.PGLOG['DSCHECK']['size']
      if 'QF' in self.params:
         self.params['QF'] = self.get_bid_numbers(self.params['QF'])
      else:
         self.params['QF'] = [0]*self.ALLCNT
         self.OPTS['QF'][2] |= 2
      if 'GI' not in self.params: self.params['GI'] = [0]*self.ALLCNT
      if 'SZ' not in self.params: self.params['SZ'] = [0]*self.ALLCNT
      if 'VI' not in self.params: self.params['VI'] = [0]*self.ALLCNT
      if 'DF' not in self.params: self.params['DF'] = [None]*self.ALLCNT
      if 'AF' not in self.params: self.params['AF'] = [None]*self.ALLCNT
      if 'LC' not in self.params: self.params['LC'] = [None]*self.ALLCNT
      if 'MC' not in self.params: self.params['MC'] = [None]*self.ALLCNT
      if 'DE' not in self.params: self.params['DE'] = [None]*self.ALLCNT
      fnames = None
      metatotal = metacnt = reorder = wcnt = ocnt = dcnt = self.ADDCNT = self.MODCNT = 0
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if metacnt >= self.PGOPT['RSMAX']:
               metatotal += self.process_meta_delete("W", self.PGOPT['emerol'])
               metacnt = 0
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{}/{}/{}, Disk/Object/RecordDeleted/RecordAdded/Proccessed, of {}self.ALLCNT Web file{} moved".format(wcnt, ocnt, dcnt, self.ADDCNT, i, self.ALLCNT, s)
         type = tolds[i]
         pgrec = self.pgget_wfile(self.params['OD'], "*", "wfile = '{}' AND type = '{}'".format(wolds[i], type), self.LGEREX)
         if not pgrec:
            self.pglog("{}: Type '{}' Web File not in RDADB for {}".format(wolds[i], type, self.params['OD']), self.PGOPT['emlerr'])
            continue
         elif pgrec['status'] == 'D':
            self.pglog("{}: Type '{}' Web File is not active in RDADB for {}".format(wolds[i], type, dsid), self.PGOPT['emlerr'])
            continue
         elif pgrec['locflag'] == 'C':
            self.pglog(wolds[i] + ": Cannot move Web File to Saved file for CGD data", self.PGOPT['extlog'])
         newrec = self.pgget(tname, "*", "sfile = '{}' AND {}".format(snews[i], dcnd), self.LGEREX)
         if newrec:
            self.pglog("{}: cannot move Web to existing Saved file {} of {}".format(wolds[i], newrec['sfile'], newrec['dsid']), self.PGOPT['emlerr'])
            continue
         if (pgrec['gindex'] and not ('GI' in self.params and self.params['GI'][i]) and
             not self.pgget("dsgroup", "", "{} and gindex = {}".format(dcnd, pgrec['gindex']), self.PGOPT['extlog'])):
            self.pglog("Group Index {} is not in RDADB for {}\n".format(pgrec['gindex'], dsid) +
                        "Specify Original/New group index via options -OG/-GI", self.PGOPT['extlog'])
         ofrom = wfrom = smove = 1
         omove = 0
         locflag = pgrec['locflag']
         if locflag == 'O':
            wfrom = 0
         elif locflag == 'G':
            ofrom = 0
   #      if not self.params['LC'][i] or self.params['LC'][i] == 'R':
   #         self.params['LC'][i] = locflag
   #      else:
   #         locflag = self.params['LC'][i]
   #      if locflag == 'O':
   #         smove = 0
   #      elif locflag == 'G':
   #         omove = 0
         if smove:
            if wfrom:
               stat = self.move_local_file(anews[i], aolds[i], self.PGOPT['emerol']|self.OVERRIDE)
               wfrom = 0
            else:
               stat = self.object_copy_local(anews[i], oolds[i], frombucket, self.PGOPT['emerol']|self.OVERRIDE)
            if not stat:
               self.RETSTAT = 1
               continue
            wcnt += 1
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         if omove:
            if wfrom:
               stat = self.local_copy_object(onews[i], aolds[i], tobucket, None, self.PGOPT['emerol']|self.OVERRIDE)
            elif smove:
               stat = self.local_copy_object(onews[i], anews[i], tobucket, None, self.PGOPT['emerol']|self.OVERRIDE)
            else:
               stat = self.move_object_file(onews[i], oolds[i], tobucket, frombucket, self.PGOPT['emerol']|self.OVERRIDE)
               ofrom = 0
            if not stat:
               self.RETSTAT = 1
               continue
            ocnt += 1
            if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         if wfrom: self.delete_local_file(aolds[i], self.PGOPT['emerol'])
         if ofrom: self.delete_object_file(oolds[i], frombucket, self.PGOPT['emerol'])
         if self.PGOPT['GXTYP'].find(type) > -1 and ('DX' in self.params or pgrec and pgrec['meta_link'] and pgrec['meta_link'] != 'N' and 'KM' not in self.params):
            metacnt += self.record_meta_delete('W', dsid, pgrec['wfile'])
         if pgrec['bid'] and not self.params['QF'][i]: self.params['QF'][i] = pgrec['bid']
         if pgrec['gindex'] and not self.params['GI'][i]: self.params['GI'][i] = pgrec['gindex']
         if pgrec['vindex'] and not self.params['VI'][i]: self.params['VI'][i] = pgrec['vindex']
         if pgrec['data_size'] and not self.params['SZ'][i]: self.params['SZ'][i] = pgrec['data_size']
         if pgrec['data_format'] and not self.params['DF'][i]: self.params['DF'][i] = pgrec['data_format']
         if pgrec['file_format'] and not self.params['AF'][i]: self.params['AF'][i] = pgrec['file_format']
         if pgrec['checksum'] and not self.params['MC'][i]: self.params['MC'][i] = pgrec['checksum']
         if pgrec['note'] and not self.params['DE'][i]: self.params['DE'][i] = pgrec['note']
         if not fnames: fnames = self.get_field_keys(tname, None, "G")
         self.record_filenumber(dsid, pgrec['gindex'], 4, (pgrec['type'] if pgrec['status'] == 'P' else ''), -1, -pgrec['data_size'])
         dcnt += self.pgdel_wfile(dsid, "wid = {}".format(pgrec['wid']), self.LGEREX)
         info = self.get_file_origin_info(snews[i], pgrec)
         self.set_one_savedfile(i, None, snews[i], fnames, tnews[i], info, dsid, 12)
         if pgrec['bid']: self.save_move_info(pgrec['bid'], wolds[i], type, 'W', self.params['OD'], snews[i], tnews[i], 'S', dsid)
      self.pglog("{}/{}/{}/{}, Disk/Object/RecordDelete/RecordAdd, of {} Web file{} moved".format(wcnt, ocnt, dcnt, self.ADDCNT, self.ALLCNT, s), self.PGOPT['emllog'])
      if metacnt > 0:
         metatotal += self.process_meta_delete('W', self.PGOPT['emerol'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], 'sfile')
      if (metatotal + reorder + self.ADDCNT + self.MODCNT + dcnt) > 0:
         self.reset_rdadb_version(dsid)
         if 'OD' in self.params and self.params['OD'] != dsid:
            self.reset_rdadb_version(self.params['OD'])
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""  

   # get date/time/size info from given file record
   def get_file_origin_info(self, fname, pgrec):
      info = {'isfile' : (0 if 'fileflag' in pgrec and pgrec['fileflag'] == 'P' else 1), 'data_size' : pgrec['data_size']}
      info['fname'] = op.basename(fname)
      info['date_modified'] = pgrec['date_modified']
      info['time_modified'] = pgrec['time_modified']
      info['date_created'] = pgrec['date_created']
      info['time_created'] = pgrec['time_created']
      return info

   # delete saved files from a given dataset
   def delete_saved_files(self):
      tname = 'sfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      bucket = "gdex-decsdata"
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Delete {} Saved file{} from {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      bidx = chksize = reorder = scnt = ocnt = dcnt = 0
      self.cache_group_info(self.ALLCNT, 0)
      self.validate_multiple_options(self.ALLCNT, ["ST", 'VI', 'QF', 'LC'])
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Saved file{} processed for delete".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            self.set_rearchive_filenumber(dsid, bidx, self.ALLCNT, 8)
            chksize = self.PGLOG['DSCHECK']['size']
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{}/i, Disk/Object/Record/processed, of {} Saved file{} deleted".format(scnt, ocnt, dcnt, i, self.ALLCNT, s)
         sfile = self.params['SF'][i]
         if 'ST' in self.params and self.params['ST'][i]:
            type = self.params['ST'][i]
         else:
            self.pglog("{}-{}: Miss Saved file Type to Delete".format(dsid, sfile), self.PGOPT['emlerr'])
            continue
         sfile = self.get_saved_path(i, sfile, 0, type)
         pgrec = self.pgget(tname, "*", "sfile = '{}' AND type = '{}' AND {}".format(sfile, type, dcnd), self.LGEREX)
         if not pgrec:
            self.pglog("{}-{}: Type '{}' Saved file is not in RDADB".format(dsid, sfile, type), self.PGOPT['errlog'])
            continue
         sdel = oflag = sflag = 1
         odel = 0
         locflag = 'G'
   #      locflag = pgrec['locflag']
   #      if locflag == 'O':
   #         sflag = 0
   #      elif locflag == 'G':
   #         oflag = 0
   #      elif locflag == 'C':
   #         sflag = oflag = 0
         if 'LC' in self.params and self.params['LC'][i]: locflag = self.params['LC'][i]
         if locflag == 'O':
            sdel = 0
         elif locflag == 'G':
            odel = 0
         elif locflag == 'C':
            sdel = odel = 0
         if (sflag+oflag) == (sdel+odel):
            vindex = self.params['VI'][i] if 'VI' in self.params else pgrec['vindex']
            if vindex:
               self.pglog(sfile + ": Saved file is version controlled, add option -vi 0 to force delete", self.PGOPT['errlog'])
               continue
            bid = self.params['QF'][i] if 'QF' in self.params else pgrec['bid']
            if bid:
               self.pglog(sfile + ": Saved file is Quasar backed up, add option -qf '' to force delete", self.PGOPT['errlog'])
               continue
         if sdel:
            afile = self.get_saved_path(i, self.params['SF'][i], 1, type)
            if self.delete_local_file(afile, self.PGOPT['emerol']):
               scnt += 1
               sflag = 0
               if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            elif self.check_local_file(afile) is None:
               sflag = 0
         if odel:
            ofile = self.join_paths(dsid, sfile)
            if self.delete_object_file(ofile, bucket, self.PGOPT['emerol']):
               ocnt += 1
               oflag = 0
               if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
            elif self.check_object_file(ofile, bucket) is None:
               oflag = 0
         if (oflag + sflag) > 0:
            locflag = "O" if oflag else "G"
            self.pgexec("UPDATE sfile SET locflag = '{}' WHERE sid = {}".format(locflag, pgrec['sid']), self.LGEREX)
         else:
            ccnt = self.record_filenumber(dsid, pgrec['gindex'], 8, 'P', -1, -pgrec['data_size'])
            fcnt = self.pgdel_sfile("sid = {}".format(pgrec['sid']), self.LGEREX)
            if fcnt: dcnt += fcnt
            if ccnt: self.save_filenumber(dsid, 8, 1, fcnt)
      if (scnt + ocnt + dcnt) > 0:
         self.pglog("{}/{}/{}, Disk/Object/Record, of {} Saved file{} deleted for {}".format(scnt, ocnt, dcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (dcnt + reorder) > 0:
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # delete Quasar backup files from a given dataset
   def delete_backup_files(self):
      tname = 'bfile'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      brec = {'bid' : 0}
      bkend = "gdex-quasar"
      drend = "gdex-quasar-drdata"
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Delete {} Backup file{} from {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      bidx = chksize = reorder = bcnt = dcnt = delcnt = scnt = wcnt = 0
      self.validate_multiple_options(self.ALLCNT, ["QT"])
      if self.PGLOG['DSCHECK']:
         bidx = self.set_dscheck_fcount(self.ALLCNT, self.PGOPT['extlog'])
         if bidx > 0:
            self.pglog("{} of {} Backup file{} processed for delete".format(bidx, self.ALLCNT, s), self.PGOPT['emllog'])
            if bidx == self.ALLCNT: return
            chksize = self.PGLOG['DSCHECK']['size']
      for i in range(bidx, self.ALLCNT):
         if i > bidx and ((i-bidx)%20) == 0:
            if self.PGLOG['DSCHECK']:
               self.set_dscheck_dcount(i, chksize, self.PGOPT['extlog'])
            if 'EM' in self.params:
               self.PGLOG['PRGMSG'] = "{}/{}/{}/i, Quasar/Drdata/Record/processed, of {} Backup file{} deleted".format(bcnt, dcnt, delcnt, i, self.ALLCNT, s)
         (bfile, qfile) = self.get_backup_filenames(self.params['QF'][i], dsid)
         if 'QT' in self.params and self.params['QT'][i]:
            type = self.params['QT'][i]
         else:
            self.pglog("{}-{}: Miss backup file Type to Delete".format(dsid, bfile), self.PGOPT['emlerr'])
            continue
         pgrec = self.pgget(tname, "*", "bfile = '{}' AND type = '{}' AND {}".format(bfile, type, dcnd), self.LGEREX)
         if not pgrec:
            self.pglog("{}-{}: Type '{}' Backup file is not in RDADB".format(dsid, bfile, type), self.PGOPT['errlog'])
            continue
         bdel = ddel = 1
         if type == 'B': ddel = 0
         if bdel:
            if self.delete_backup_file(qfile, bkend, self.PGOPT['emerol']):
               bcnt += 1
               if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         if ddel:
            if self.delete_backup_file(qfile, drend, self.PGOPT['emerol']):
               dcnt += 1
               if self.PGLOG['DSCHECK']: chksize += pgrec['data_size']
         bcnd = "bid = {}".format(pgrec['bid'])
         if pgrec['scount']:
            scnt += self.pgupdt("sfile", brec, bcnd, self.LGEREX)
         if pgrec['wcount']:
            wcnt += self.pgupdt_wfile_dsids(dsid, pgrec['dsids'], brec, bcnd, self.LGEREX)
         fcnt = self.pgdel(tname, bcnd, self.LGEREX)
         if fcnt: delcnt += fcnt
      if (bcnt + dcnt + delcnt) > 0:
         self.pglog("{}/{}/{}, Quasar/Drdata/Record, of {} Backup file{} deleted for {}".format(bcnt, dcnt, delcnt, self.ALLCNT, s, dsid), self.PGOPT['emllog'])
      if (scnt + wcnt) > 0:
         self.pglog("{}/{} associated Web/Saved files cleaned for {}".format(wcnt, scnt, dsid), self.PGOPT['emllog'])
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_dcount(self.ALLCNT, chksize, self.PGOPT['extlog'])
      if 'ON' in self.params:
         reorder = self.reorder_files(dsid, self.params['ON'], tname)
      if (delcnt + reorder) > 0:
         self.reset_rdadb_version(dsid)
      if 'EM' in self.params: self.PGLOG['PRGMSG'] = ""

   # change existing group indices to new indices.  
   def change_group_index(self):
      tname = 'dsgroup'
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Change {} group{} for {} ...".format(self.ALLCNT, s, dsid), self.WARNLG)
      # cache info of original groups
      tgroups = [1]*self.ALLCNT
      mlrecs = [None]*self.ALLCNT
      for i in range(self.ALLCNT):
         gindex = self.params['GI'][i]
         if gindex == 0:
            self.pglog("Error Change {} to GIndex 0".format(self.params['OG'][i]), self.PGOPT['extlog'])
         if self.pgget(tname, "", "{} AND gindex = {}".format(dcnd, gindex), self.PGOPT['extlog']):
            self.pglog("Error Change {} to existing GIndex {}".format(self.params['OG'][i], gindex), self.PGOPT['extlog'])
         cnd = "{} AND gindex = {}".format(dcnd, self.params['OG'][i])
         pgrec = self.pgget(tname, "meta_link, pindex", cnd, self.LGEREX)
         if not pgrec: self.pglog("Original GIndex {} not exists for {}".format(self.params['OG'][i], dsid), self.PGOPT['extlog'])
         mlrecs[i] = pgrec['meta_link']
         if pgrec['pindex']: tgroups[i] = 0
      # change groups, wfiles, and sfiles
      metacnt = prdcnt = savedcnt = webcnt = rccnt = pgcnt = modcnt = 0
      twcnt = tscnt = 0
      for i in range(self.ALLCNT):
         record = {'gindex' : self.params['GI'][i]}
         gcnd = "gindex = {}".format(self.params['OG'][i])
         cnd = "{} AND {}".format(dcnd, gcnd)
         modcnt += self.pgupdt(tname, record, cnd, self.LGEREX)
         prdcnt += self.pgget("dsperiod", "", cnd, self.LGEREX)
         chgcnt = self.pgupdt_wfile(dsid, record, gcnd, self.LGEREX)
         if chgcnt > 0:
            webcnt += chgcnt
            if re.search(r'^[XBW]$', mlrecs[i]):
               metacnt += self.record_meta_summary('W', dsid, self.params['OG'][i], record['gindex'])
         savedcnt += self.pgupdt("sfile", record, cnd, self.LGEREX)
         rccnt += self.pgupdt("rcrqst", record, cnd, self.LGEREX)
         if tgroups[i]:
            tgrec = {'tindex' : self.params['GI'][i]}
            tcnd = "tindex = {}".format(self.params['OG'][i])
            cnd = "{} AND {}".format(dcnd, tcnd)
            twcnt += self.pgupdt_wfile(dsid, tgrec, tcnd, self.LGEREX)
            tscnt += self.pgupdt("sfile", tgrec, cnd, self.LGEREX)
         pgrec = {'pindex' : self.params['GI'][i]}
         cnd = "pindex = {} AND {}".format(self.params['OG'][i], dcnd)
         pgcnt += self.pgupdt(tname, pgrec, cnd, self.LGEREX)
      self.pglog("{} of {} group{} changed".format(modcnt, self.ALLCNT, s), self.LOGWRN)
      if metacnt: self.process_meta_move('W')
      if modcnt > 0:
         if prdcnt > 0:
            s = 's' if prdcnt > 1 else ''
            self.pglog("Group info of {} period{} changed, modify the periods via metadata editor".format(prdcnt, s), self.LOGWRN)
         if pgcnt > 0:
            s = 's' if pgcnt > 1 else ''
            self.pglog("Parent Group Index info of {} group{} modified".format(pgcnt, s), self.LOGWRN)
         cnt = webcnt + savedcnt
         if cnt > 0:
            s = 's' if cnt > 1 else ''
            self.pglog("{}/{} associated Saved/Web file record{} modified for new group".format(savedcnt, webcnt, s), self.LOGWRN)
         if rccnt > 0:
            s = 's' if rccnt > 1 else ''
            self.pglog("{} associated Request Control record{} modified".format(rccnt, s), self.LOGWRN)
         cnt = twcnt + tscnt
         if cnt > 0:
            s = 's' if cnt > 1 else ''
            self.pglog("{}/{} associated Saved/Web file record{} modified for new top group".format(tscnt, twcnt, s), self.LOGWRN)
         self.reset_rdadb_version(dsid)

   # view specialist defiend key/value pairs for given dsid
   def view_keyvalues(self, dsid, kvalues, getkeys = 0):
      cond = "dsid = '{}' ".format(dsid)
      count = 0
      cnt = len(kvalues) if kvalues else 0
      if cnt == 1 and re.match(r'^all$', kvalues[0], re.I):
         cnt = 0
         getkeys = 1
      if cnt > 0:
         values = {'okeys' : [], 'value' : []}
         for i in range(cnt):
            pgrec = self.pgget("dsokeys", "value", "{}AND okey = '{}'".fomrat(cond, kvalues[i]), self.LGWNEX)
            if pgrec:
               values['okey'].append(kvalues[i])
               values['value'].append(pgrec['value'])
               count += 1
            else:
               self.pglog(kvalues[i] + ": key undefined", self.LOGERR)
      elif getkeys:
         values = self.pgmget("dsokeys", "okey, value", cond + "ORDER BY okey", self.LGWNEX)
         count = len(values['okey']) if values else 0
      if not count: return 0
      if 'FN' not in self.params: self.OUTPUT.write("{}{}{}\n".format(self.OPTS['DS'][1], self.params['ES'], dsid))
      if count == 1:
         self.OUTPUT.write("{}{}{}=>{}\n".format(self.OPTS['KV'][1], self.params['ES'], values['okey'][0], values['value'][0]))
      else:
         self.OUTPUT.write("{}{}\n".format(self.OPTS['KV'][1], self.params['DV']))
         for i in range(count):
            self.OUTPUT.write("{}=>{}{}\n".format(values['okey'][i], values['value'][i], self.params['DV']))
      return count

   # set specialist defiend key/value pairs for given dsid
   def set_keyvalues(self, dsid, kvalues):
      cnt = len(kvalues) if kvalues else 0
      if cnt == 0: return 0
      s = 's' if cnt > 1 else ''
      self.pglog("Set {} key/value pairs for {} ...".format(cnt, dsid), self.WARNLG)
      dcnt = mcnt = acnt = 0
      for i in range(cnt):
         ms= re.search(r'^(.*)=>(.*)$', kvalues[i])
         if ms:
            key = ms.group(1)
            value = ms.group(2)
            if not value: value = None
         else:
            self.pglog(kvalues[i] + ": key Undefined", self.LOGERR)
            continue
         cond = "dsid = '{}' AND okey = '{}'".format(dsid, key)
         pgrec = self.pgget("dsokeys", "value", cond, self.LGWNEX)
         if pgrec:
            if value is None:  # empty value, delete record
               dcnt += self.pgdel("dsokeys", cond, self.LGWNEX)
            elif pgrec['value'] is None or value != pgrec['value']:
               pgrec['value'] = value
               mcnt += self.pgupdt("dsokeys", pgrec, cond, self.LGWNEX)
         else:
            pgrec = {'dsid' : dsid, 'okey' : key, 'value' : value}
            acnt += self.pgadd("dsokeys", pgrec, self.LGWNEX)
      self.pglog("{}/{}/{} of {} key/value pairs added/modified/deleted for {}!".format(acnt, mcnt, dcnt, cnt, dsid), self.LOGWRN)
      return (acnt + mcnt + dcnt)

   # record moved web file info
   def set_web_move(self, pgrec):
      date = self.curdate()
      cond = "wid = {} and date = '{}'".format(pgrec['wid'], date)
      if not self.pgget("wmove", "", cond, self.LGWNEX):
         record = {'dsid' : pgrec['dsid'], 'uid' : self.PGOPT['UID'],
                   'wfile' : pgrec['wfile'], 'wid' : pgrec['wid'], 'date' : date}
         self.pgadd("wmove", record, self.LGWNEX)

   # reset file counts for saved groups
   def reset_group_filenumber(self, dsid, act):
      ucnt = 0
      gindices = sorted(self.CHGGRPS)
      if gindices and gindices[0] != 0:
         for gindex in gindices:
            if gindex: ucnt += self.reset_filenumber(dsid, gindex, act)
      else:
         ucnt += self.reset_filenumber(dsid, 0, act)
      return ucnt

   # reset top group indices for given groups
   def reset_top_group_index(self, dsid, act):
      tcnt = 0
      cgidxs = {}
      if 'GI' in self.params:
         for gindex in self.params['GI']:
            if gindex is None or gindex in cgidxs: continue
            tcnt += self.reset_top_group_index(dsid, gindex, act)
            cgidxs[gindex] = gindex
      else:
         tcnt += self.reset_top_group_index(dsid, 0, act)
      return tcnt

   # set the re-archived file counts for groups
   def set_rearchive_filenumber(self, dsid, bidx, total, act):
      if 'GI' in self.params:
         lmt = bidx + 20
         if lmt > total: lmt = total
         for i in range(bidx, lmt):
            if self.params['GI'][i]: self.CHGGRPS[self.params['GI'][i]] = 1
      self.reset_group_filenumber(dsid, act)
      self.CHGGRPS = {}
   
   # reset group metadata via scm
   def reset_group_metadata(self, dsid, act):
      dcnd = "dsid = '{}'".format(dsid)
      gindices = sorted(self.CHGGRPS)
      if gindices:
         for gindex in gindices:
            if gindex:
               pgrec = self.pgget("dsgroup", "meta_link", "{} AND gindex = {}".format(dcnd, gindex), self.LGEREX)
            else:
               pgrec = self.pgget("dataset", "meta_link", dcnd, self.LGEREX)
               gindex = "all"
            if not pgrec: continue
            if act == 1 or act&4 and re.search(r'(Y|B|W)', pgrec['meta_link']):
               self.pgsystem("{} -d {} -rw {}".format(self.PGOPT['scm'], dsid, gindex))
      else:
         pgrec = self.pgget("dataset", "meta_link", dcnd, self.LGEREX)
         if pgrec:
            if act == 1 or act&4 and re.search(r'(Y|B|W)', pgrec['meta_link']):
               self.pgsystem("{} -d {} -rw all".format(self.PGOPT['scm'], dsid))
   
   # get web file name for given local file name
   def get_archive_filename(self, lfile):
      return lfile if 'KP' in self.params else op.basename(lfile)
   
   # clean up local files and directories after action
   def clean_local_files(self):
      cnt = 0
      if 'DD' in self.params: self.record_delete_directory(None, self.params['DD'])
      for lfile in self.params['LF']:
         if lfile and self.delete_local_file(lfile, self.PGOPT['emerol']): cnt += 1
      if cnt > 0:
         s = ("s" if cnt > 1 else "")
         self.pglog("cnt local files cleaned", self.PGOPT['emerol'])
      if 'DD' in self.params: self.clean_delete_directory(self.PGOPT['wrnlog'])
   
   # transfer cached self.ERRMSG between globally and locally 
   def reset_errmsg(self, errcnt):
      ret = 0
      if errcnt < 0:   # cache self.ERRMSG globally
         self.PGLOG['self.ERRCNT'] += self.ERRCNT
         self.PGLOG['self.ERRMSG'] += self.ERRMSG
         self.ERRCNT = 0
         self.ERRMSG = ''
      else:
         if errcnt > 0:  # cache self.ERRMSG locally
            self.ERRMSG += self.PGLOG['self.ERRMSG']
            self.ERRCNT += errcnt
            ret = 1
         self.PGLOG['self.ERRCNT'] = 0
         self.PGLOG['self.ERRMSG'] = ''
      return ret

   # copy a file to a alternate destination
   def copy_alter_local(self, wfile, ahome):
      afile = wfile
      bproc = self.PGSIG['BPROC']
      afile = re.sub(self.PGLOG['DSDHOME'], ahome, afile)
      if bproc > 1: self.PGSIG['BPROC'] = 1
      self.local_copy_local(afile, wfile, self.PGOPT['emerol']|self.OVERRIDE)
      if bproc != self.PGSIG['BPROC']: self.PGSIG['BPROC'] = bproc
   
   # delete a file at alternate location
   def delete_alter_local(self, wfile, ahome):
      afile = wfile
      bproc = self.PGSIG['BPROC']
      afile = re.sub(self.PGLOG['DSDHOME'], ahome, afile)
      if bproc > 1: self.PGSIG['BPROC'] = 1
      if op.exists(afile): self.delete_local_file(afile, self.PGOPT['emerol'])
      if bproc != self.PGSIG['BPROC']: self.PGSIG['BPROC'] = bproc
   
   # get version information
   def get_version_info(self):
      tname = "dsvrsn"
      dsid = self.params['DS']
      hash = self.TBLHASH[tname]
      self.pglog("Get version control info of {} from RDADB ...".format(dsid), self.WARNLG)
      lens = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      fnames = self.fieldname_string(fnames, self.PGOPT['dsvrsn'], self.PGOPT['vrsnall'])
      onames = self.params['ON'] if 'ON' in self.params else "V"
      condition = self.get_condition(tname)
      ocnd = self.get_order_string(onames, tname)
      pgrecs = self.pgmget(tname, "*", condition + ocnd, self.PGOPT['extlog'])
      self.OUTPUT.write("{}{}{}\n".format(self.OPTS['DS'][1], self.params['ES'], dsid))
      if pgrecs and 'FO' in self.params: lens = self.all_column_widths(pgrecs, fnames, hash)
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         s = 's' if cnt > 1 else ''
         self.pglog("{} version control{} retrieved".format(cnt, s), self.PGOPT['wrnlog'])
      else:
         self.pglog("No version control information retrieved", self.PGOPT['wrnlog'])

   # get a DOI number for given dsid
   def get_new_version(self, nrec, doi):
      nrec['doi'] = doi
      if not ('start_date' in nrec and nrec['start_date']): nrec['start_date'] = self.curdate()
      if not ('start_time' in nrec and nrec['start_time']): nrec['start_time'] = self.curtime()
      nrec['end_date'] = nrec['end_time'] = None
      nrec['status'] = 'A' if doi else 'P'
      nrec['iversion'] = 1
      return nrec

   # replace old version record with a new one
   def transfer_version_info(self, nrec, orec, doi):
      dsid = self.params['DS']
      vinfo = "Version Control {}".format(orec['vindex'])
      if not ('start_date' in nrec and nrec['start_date']): nrec['start_date'] = self.curdate()
      if not ('start_time' in nrec and nrec['start_time']): nrec['start_time'] = self.curtime()
      if self.cmptime(orec['start_date'], orec['start_time'], nrec['start_date'], nrec['start_time']) >= 0:
         self.action_error("New Version Control must start later than {} {} of {}".format(orec['start_date'], orec['start_time'], vinfo))
      record = {}
      record['end_date'] = nrec['start_date']
      record['end_time'] = nrec['start_time']
      record['status'] = "H"
      if doi == orec['doi']:
         nrec['doi'] = orec['doi']
         nrec['iversion'] = orec['iversion'] + 1
         nrec['status'] = 'A'
         if 'eversion' not in nrec and orec['eversion']: nrec['eversion'] = orec['eversion']
         if 'note' not in nrec: self.action_error("DOI {}: Miss a brief reason via Option -DE for new Version Control".format(doi))
      else:
         self.pglog("DOI {}: Superseded by {} for {} {}".format(orec['doi'], doi, dsid, vinfo), self.PGOPT['wrnlog'])
      self.pgupdt("dsvrsn", record, "vindex = {}".format(orec['vindex']), self.PGOPT['extlog'])
      self.pglog("{} {}: Set status to 'H'".format(dsid, vinfo), self.PGOPT['wrnlog'])
      return nrec

   # add or modify version control information
   def set_version_info(self):
      tname = "dsvrsn"
      dsid = self.params['DS']
      dcnd = "dsid = '{}'".format(dsid)
      msg = "{} version control".format(self.ALLCNT)
      if self.ALLCNT > 1: msg += "s"
      self.pglog("Set information of {} ...".format(msg), self.WARNLG)
      addcnt = modcnt = 0
      fnames = self.get_field_keys(tname, None, 'V')
      self.validate_multiple_values(tname, self.ALLCNT, fnames)
      for i in range(self.ALLCNT):
         vidx = self.params['VI'][i]
         doi = self.params['DN'][i] if 'DN' in self.params else ''
         actrec = None
         if vidx > 0:
            cnd = "vindex = {}".format(vidx)
            vinfo = "Version Control {}".format(vidx)
            pgrec = self.pgget(tname, "*", cnd, self.PGOPT['extlog'])
            if not pgrec: self.action_error(vinfo + ": Not in RDADB")
            if pgrec['doi']:
               if not doi:
                  doi = pgrec['doi']
               elif doi != pgrec['doi']:
                  self.action_error("{}: DOI {} exists, cannot change to {}".format(vinfo, pgrec['doi'], doi))
         else:
            pgrec = self.pgget(tname, "vindex", dcnd + " AND status = 'P'", self.PGOPT['extlog'])
            if pgrec:
               self.action_error("Cannot add new Version Control for Pending Version Control {} exists".format(pgrec['vindex']))
            else:
               actrec = self.pgget(tname, "*", dcnd + " AND status = 'A'", self.PGOPT['extlog'])
               if actrec and not doi: doi = actrec['doi']
         record = self.build_record(fnames, pgrec, tname, i)
         if not vidx:
            record = self.get_new_version(record, doi)
            if actrec: record = self.transfer_version_info(record, actrec, doi)
         if record:
            vidx = 0
            if pgrec:
               if 'status' in record:
                  if record['status'] == "H":
                     self.action_error(vinfo + ": Cannot set status to 'H', use Action -TV to terminate")
                  elif pgrec['status'] == 'H':
                     self.action_error("{}: Cannot set status to '{}' from 'H'".format(vinfo, record['status']))
                  elif record['status'] == "A":
                     if not doi: self.action_error(vinfo + ": Cannot set status to 'A' for missing DOI")
                  elif record['status'] == "P":
                     if doi: self.action_error(vinfo + ": Cannot set status to 'P' for DOI set")
               if 'end_date' in record and record['end_date'] or 'end_time' in record and record['end_time']:
                  if 'end_date' in record and pgrec['end_date']:
                     self.action_error(vinfo + ": Cannot change ending date/time")
                  else:
                     self.action_error(vinfo + ": Cannot set ending date/time, use Action -TV to terminate")
               if not self.pgupdt(tname, record, cnd, self.PGOPT['extlog']): continue
               modcnt += 1
               if 'doi' in record: vidx = pgrec['vindex']
            else:
               if 'status' in record:
                  if record['status'] == "H":
                     self.action_error("Cannot add new Version Control with status 'H'")
                  elif record['status'] == "A":
                     if not doi: self.action_error("Cannot add new Version Control with status 'A' for missing DOI")
               if 'end_date' in record and record['end_date'] or 'end_time' in record and record['end_time']:
                  self.action_error("Cannot add new Version Control with ending date/time")
               record['dsid'] = dsid
               vidx = self.pgadd(tname, record, self.PGOPT['extlog']|self.AUTOID|self.DODFLT)
               if not vidx: continue
               vinfo = "Version Control {}".format(vidx)
               addcnt += 1
               if not doi: vidx = 0
            if vidx:
               vrec = {'vindex' : vidx}
               vcnd = "type = 'D' AND vindex = 0"
               fcnt = self.pgupdt_wfile(dsid, vrec, vcnd, self.PGOPT['extlog'])
               if fcnt > 0:
                  s = 's' if fcnt > 1 else ''
                  self.pglog("{}: Linked {} Web file record{}".format(vinfo, fcnt, s), self.PGOPT['wrnlog'])
               fcnt = self.pgupdt("sfile", vrec, "{} AND {}".format(dcnd, vcnd), self.PGOPT['extlog'])
               if fcnt > 0:
                  s = 's' if fcnt > 1 else ''
                  self.pglog("{}: Linked {} Saved file record{}".format(vinfo, fcnt, s), self.PGOPT['wrnlog'])
               vcnd = "type = 'D' AND vindex <> {}".format(vidx)
               fcnt = self.pgupdt_wfile(dsid, vrec, vcnd, self.PGOPT['extlog'])
               if fcnt > 0:
                  s = 's' if fcnt > 1 else ''
                  self.pglog("{}: Relinked {} Web file record{}".format(vinfo, fcnt, s), self.PGOPT['wrnlog'])
               fcnt = self.pgupdt("sfile", vrec, "{} AND {}".format(dcnd, vcnd), self.PGOPT['extlog'])
               if fcnt > 0:
                  s = 's' if fcnt > 1 else ''
                  self.pglog("{}: Relinked {} Saved file record{}".format(vinfo, fcnt, s), self.PGOPT['wrnlog'])
      self.pglog("{}/{} of {} added/modified in RDADB!".format(addcnt, modcnt, msg), self.PGOPT['wrnlog'])
   
   # terminate version control information for given version indices
   def terminate_version_info(self):
      msg = "{} Version Control".format(self.ALLCNT)
      if self.ALLCNT > 1: msg += "s"
      self.pglog("Terminate {} ...".format(msg), self.WARNLG)
      dsid = self.params['DS']
      self.validate_multiple_options(self.ALLCNT, ["ED", "ET"])
      doicnt = modcnt = delcnt = 0
      for i in range(self.ALLCNT):
         vidx = self.params['VI'][i]
         cnd = "vindex = {}".format(vidx)
         vinfo = "{} Version Control {}".format(dsid, vidx)
         pgrec = self.pgget("dsvrsn", "doi, status", cnd, self.PGOPT['extlog'])
         if not pgrec:
            self.pglog(vinfo + ": Not in RDADB", self.LOGERR)
            continue
         elif pgrec['status'] == 'H':
            self.pglog(vinfo + ": is in status 'H'", self.LOGERR)
            continue
         else:
            cnt = self.pgget_wfile(dsid, "", cnd, self.PGOPT['extlog'])
            if cnt:
               s = 's' if cnt > 1 else ''
               self.pglog("{}: Cannot terminate for {} associated Web data file{}".format(vinfo, cnt, s), self.LOGERR)
               continue
         if pgrec['doi']:
            orec = self.pgget("dsvrsn", "vindex", "doi = '{}' AND vindex <> {}".format(pgrec['doi'], vidx), self.PGOPT['extlog'])
            if orec:
               self.pglog("{}: Cannot terminate for DOI {} is asscoated to Version control {}".format(vinfo, pgrec['doi'], orec['vindex']), self.LOGERR)
               continue
            doicnt += 1
            record = {'status' : "H"}
            record['end_date'] = self.params['ED'][i] if 'ED' in self.params else self.curdate()
            record['end_time'] = self.params['ET'][i] if 'ET' in self.params else self.curtime()
            if self.pgupdt("dsvrsn", record, cnd, self.PGOPT['extlog']):
               self.pglog(vinfo + ": Set status to 'H' to terminate", self.PGOPT['wrnlog'])
               modcnt += 1
         elif self.pgdel("dsvrsn", cnd, self.PGOPT['extlog']):
            self.pglog(vinfo + ": Deleted for no DOI to terminate", self.PGOPT['wrnlog'])
            delcnt += 1
      self.pglog("{}/{} of {} terminated for DOI/Version Control".format(doicnt, modcnt, msg), self.PGOPT['wrnlog'])
      if delcnt: self.pglog("{}/{} of {} deleted".format(delcnt, modcnt, msg), self.PGOPT['wrnlog'])

# main function to excecute this script
def main():
   object = DsArch()
   object.read_parameters()
   object.start_actions()
   object.pgexit(0)

# call main() to start program
if __name__ == "__main__": main()
