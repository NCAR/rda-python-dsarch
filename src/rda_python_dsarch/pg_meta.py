###############################################################################
#     Title :pg_meta.py
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 09/18/2020
#             2025-01-22 transferred to package rda_python_dsarch from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-12-09 convert to class PgMeta
#   Purpose : python library module to handle file counts and metadata gathering
#    Github : https://github.com/NCAR/rda-python-dsarch.git
###############################################################################
"""
pg_meta.py - File-count management and metadata-gathering helpers for dsarch.

Provides the PgMeta class which tracks in-memory changes to dataset/group file
counts (web and saved), flushes them to RDADB in batch, and manages queued
metadata-XML operations (gather, delete, move, and group-summary) executed via
external tools (gatherxml, dcm, rcm, scm, sml).
"""
import os
import re
from os import path as op
from rda_python_common.pg_split import PgSplit
from rda_python_common.pg_cmd import PgCMD

class PgMeta(PgCMD, PgSplit):
   """
   Mixin providing dataset/group file-count bookkeeping and metadata-XML queueing.

   Inherits from PgCMD (command/database execution) and PgSplit (data-splitting
   utilities).  All dsarch action classes inherit from this class indirectly via
   DsArch → PgArch → PgMeta.

   Counts array layout (used internally by reset_filenumber / record_filenumber):
      Index  Short  Description
      -----  -----  -----------
        0    pms    primary MSS count
        1    cpm    cached primary MSS count
        2    pm     public MSS count
        3    mc     MSS count
        4    cdc    cached D-type web count
        5    dc     D-type (public data) web count
        6    wc     total web count
        7    ds     D-type web data size (bytes)
        8    nc     N-type (NCAR-only) web count
        9    ns     N-type web data size (bytes)
       10    sc     saved file count
       11    ss     saved file data size (bytes)
       12    tu/flg total-updated count (reset_filenumber) or group-type flag
                    (record_filenumber: 1 = Public group, 0 = Internal)

   Instance attributes set in __init__:
      GCOUNTS  -- nested dict {dsid: {gindex: counts_list}} for pending file-count changes
      META     -- dict of queued metadata operations keyed by 'GW','DW','RW','SW'
                  (gather/delete/move/summary for web files; 'GM' etc. for MSS, no-ops)
      TGIDXS   -- dict caching {gindex: top_gindex} to avoid repeated DB lookups
      TIDXS    -- dict of unique top-group indices encountered during metadata gather
      CMD      -- dict mapping short keys to external command names
      logfile  -- saved copy of PGLOG['LOGFILE'] used by switch_logfile()
      errfile  -- saved copy of PGLOG['ERRFILE'] used by switch_logfile()
   """

   def __init__(self):
      """Initialize PgMeta, setting up file-count caches and metadata-command mappings."""
      super().__init__()  # initialize parent class
      self.GCOUNTS = {}
      self.META = {}     # keys: GM, GW, DM, DW, RM, RW, SM, and SW
      self.TGIDXS = {}   # cache value for function get_top_gindex()
      self.TIDXS = {}    # cache all unique top group index
      self.CMD = {
         'GX' : "gatherxml",
         'DX' : "dcm",
         'RX' : "rcm",
         'SX' : "scm",
         'SL' : "sml"
      }
      self.logfile = self.PGLOG['LOGFILE']
      self.errfile = self.PGLOG['ERRFILE']

   def switch_logfile(self, logname=None):
      """
      Redirect PGLOG log/error file paths to a named file, or restore the originals.

      Call with a *logname* to save the current PGLOG paths and switch to
      ``<logname>.log`` / ``<logname>.err``.  Call with no argument (or
      ``logname=None``) to restore the previously saved paths.

      Args:
         logname -- base name for the new log files, or None to restore
      """
      if logname:
         self.logfile = self.PGLOG['LOGFILE']
         self.errfile = self.PGLOG['ERRFILE']
         self.PGLOG['LOGFILE'] = logname + '.log'
         self.PGLOG['ERRFILE'] = logname + '.err'
      else:
         self.PGLOG['LOGFILE'] = self.logfile
         self.PGLOG['ERRFILE'] = self.errfile
   
   def get_group_levels(self, dsid, gindex, level):
      """
      Compute the nesting depth of *gindex* within the group hierarchy.

      Recursively walks the pindex chain in the dsgroup table and increments
      *level* for each ancestor found.

      Args:
         dsid   -- dataset ID string
         gindex -- group index to measure depth for
         level  -- starting depth (typically 1 for a direct child of the dataset)

      Returns:
         Integer depth level (1 = top-level group, 2 = sub-group, etc.)
      """
      pgrec = self.pgget("dsgroup", "pindex", "dsid = '{}' AND gindex = {}".format(dsid, gindex), self.LGEREX)
      if pgrec and pgrec['pindex']:
         level = self.get_group_levels(dsid, pgrec['pindex'], level + 1)
      return level
   
   def reset_filenumber(self, dsid, gindex, act, level=0, gtype=None, limit=0):
      """
      Recompute and write file counts for a group and its ancestors/descendants.

      Traverses the group hierarchy from *gindex* and rewrites file-count
      columns (webcnt, savedcnt, etc.) in both dsgroup and dataset tables by
      querying current file records.

      Traversal direction is controlled by *limit*:
         limit == 0  -- descend into all child groups, then propagate up
         limit  > 0  -- descend only to depth *limit*, then propagate up
         limit  < 0  -- propagate upward only (used in recursive up-passes)

      The *act* bitmask selects which counts to recompute:
         act & 4  -- web file counts (dwebcnt, webcnt, nwebcnt, size columns)
         act & 8  -- saved file counts (savedcnt, saved_size)
         act == 1 -- equivalent to act = 14 (both web and saved)

      Args:
         dsid   -- dataset ID string
         gindex -- group index to start from (0 = dataset level)
         act    -- bitmask selecting which file-type counts to update
         level  -- current recursion depth (managed internally; pass 0 externally)
         gtype  -- group type string ('P' or 'I'); resolved from DB when None
         limit  -- traversal depth limit (see above)

      Returns:
         Total number of database records updated.
      """
      wcnd = "gindex = {}".format(gindex)
      dcnd = "dsid = '{}'".format(dsid)
      gcnd = "{} AND {}".format(dcnd, wcnd)
              # 0-pms,1-cpm,2-pm,3-mc,4-cdc,5-dc,6-wc,7-ds,8-nc,9-ns,10-sc,11-ss,12-tu
      counts = [0,    0,    0,   0,   0,    0,   0,   0,   0,   0,   0,    0,    0]
      pidx = None
      retcnt = 0
      if not gtype:
         retcnt = 1
         if gindex:
            pgrec = self.pgget("dsgroup", "pindex, grptype", gcnd, self.LGEREX)
            if not pgrec or abs(pgrec['pindex']) >= abs(gindex): return 0
            gtype = pgrec['grptype']
            pidx = pgrec['pindex']
         else:
            gtype = 'P'
         if not level:
            if act == 1: act = 14
            if gindex:
               level += 1
               if pidx: level = self.get_group_levels(dsid, pidx, level+1)
               if limit > 0: limit += level
      if not level: self.pgget("dataset", "dsid", dcnd, self.LGEREX|self.DOLOCK)
      # get file counts at the current group
      if act&4:
         if gtype == 'P':
            pgrec = self.pgget_wfile(dsid, "SUM(data_size) dsize, COUNT(wid) dcount", 
                                     wcnd + " AND type = 'D' AND status = 'P'", self.LGEREX)
            if pgrec:
               counts[5] = counts[4] = pgrec['dcount']
               if pgrec['dsize']: counts[7] = pgrec['dsize']
            pgrec = self.pgget_wfile(dsid, "SUM(data_size) nsize, COUNT(wid) ncount",
                                     wcnd + " AND type = 'N' AND status = 'P'", self.LGEREX)
            if pgrec:
               counts[8] = pgrec['ncount']
               if pgrec['nsize']: counts[9] = pgrec['nsize']
         counts[6] = self.pgget_wfile(dsid, "", wcnd, self.LGEREX)
      if act&8:
         pgrec = self.pgget("sfile", "SUM(data_size) ssize, COUNT(sid) scount",
                            gcnd + " AND status <> 'D'", self.LGEREX)
         if pgrec:
            counts[10] = pgrec['scount']
            if pgrec['ssize']: counts[11] = pgrec['ssize']
      pcnd = "{} AND pindex = {}".format(dcnd, gindex)
      if limit < 0:
         flds = "gindex"
         if act&4: flds += ", dwebcnt, webcnt, nwebcnt, dweb_size, nweb_size"
         if act&8: flds += ", savedcnt, saved_size"
         grecs = self.pgmget("dsgroup", flds, pcnd, self.LGEREX)
         gcnt =  len(grecs['gindex']) if grecs else 0
         if gcnt:
            for i in range(gcnt):
               if act&4:
                  if gtype == 'P':
                     counts[5] += grecs['dwebcnt'][i]
                     counts[7] += grecs['dweb_size'][i]
                     counts[8] += grecs['nwebcnt'][i]
                     counts[9] += grecs['nweb_size'][i]
                  counts[6] += grecs['webcnt'][i]
               if act&8:
                  counts[10] += grecs['savedcnt'][i]
                  counts[11] += grecs['saved_size'][i]
         cnt = self.update_filenumber(dsid, gindex, act, counts, level)
         if cnt:
            if pidx != None: cnt += self.reset_filenumber(dsid, pidx, act, (level-1), None, -1)
            counts[11] += cnt
      elif limit == 0 or level < limit:
         grecs = self.pgmget("dsgroup", "gindex, grptype", pcnd, self.LGEREX)
         gcnt = len(grecs['gindex']) if grecs else 0
         for i in range(gcnt):
            gidx = grecs['gindex'][i]
            if abs(gidx) <= abs(gindex): continue
            subcnts = self.reset_filenumber(dsid, gidx, act, level+1, grecs['grptype'][i], limit)
            if gtype == 'P':
               counts[0] += subcnts[0]
               counts[2] += subcnts[2]
               counts[5] += subcnts[5]
               counts[7] += subcnts[7]
               counts[8] += subcnts[8]
               counts[9] += subcnts[9]
            counts[3] += subcnts[3]
            counts[6] += subcnts[6]
            counts[10] += subcnts[10]
            counts[11] += subcnts[11]
            counts[12] += subcnts[12]
         cnt = self.update_filenumber(dsid, gindex, act, counts, level)
         if cnt:
            if pidx != None: cnt += self.reset_filenumber(dsid, pidx, act, level-1, None, -1)
            counts[12] += cnt
      if level == 0: self.endtran()
      return (counts[12] if retcnt else counts)
   
   def update_filenumber(self, dsid, gindex, act, counts, level=0):
      """
      Write recomputed file counts for one group or dataset record to RDADB.

      Compares each count column in *counts* against the existing database
      values and issues an UPDATE only when differences are found.  Also
      recalculates the derived status flags wfstat (web-file status) and
      dfstat (display-flag status) based on the updated counts.

      wfstat values:  'D' = has public data files,  'N' = NCAR-only files,
                      'W' = other web files,         'E' = empty
      dfstat values:  'P' = publicly visible,  'N' = NCAR-only,  'E' = empty

      Args:
         dsid   -- dataset ID string
         gindex -- group index (0 = dataset row in the dataset table)
         act    -- bitmask selecting which count columns to update (4=web, 8=saved)
         counts -- counts list with recomputed values (see class docstring for layout)
         level  -- nesting depth; when non-zero, also updates the group's level column

      Returns:
         1 if any database column was updated, 0 otherwise.
      """
      flds = "mfstat, wfstat, dfstat"
      if gindex:
         table = "dsgroup"
         cnd = "dsid = '{}' AND gindex = {}".format(dsid, gindex)
         msg = "{}-G{}".format(dsid, gindex)
         if level: flds += ", level"
      else:
         table = "dataset"
         cnd = "dsid = '{}'".format(dsid)
         msg = dsid +":"
      if act&4: flds += ", dwebcnt, webcnt, cdwcnt, nwebcnt, dweb_size, nweb_size"
      if act&8: flds += ", saved_size, savedcnt"
      pgrec = self.pgget(table, flds, cnd, self.LGEREX)
      if not pgrec: return self.pglog("Group Index {} not exists for '{}'".format(gindex, dsid), self.LOGWRN)
      record = {}
      if act&4:
         if pgrec['dweb_size'] != counts[7]:
            record['dweb_size'] = counts[7]
            msg += " DS-{}".format(record['dweb_size'])
         if pgrec['nweb_size'] != counts[9]:
            record['nweb_size'] = counts[9]
            msg += " NS-{}".format(record['nweb_size'])
         if pgrec['cdwcnt'] != counts[4]:
            record['cdwcnt'] = counts[4]
            msg += " CDC-{}".format(record['cdwcnt'])
         if pgrec['dwebcnt'] != counts[5]:
            record['dwebcnt'] = counts[5]
            msg += " DC-{}".format(record['dwebcnt'])
         if pgrec['nwebcnt'] != counts[8]:
            record['nwebcnt'] = counts[8]
            msg += " NC-{}".format(record['nwebcnt'])
         if pgrec['webcnt'] != counts[6]:
            record['webcnt'] = counts[6]
            msg += " WC-{}".format(record['webcnt'])
         stat = 'D' if counts[5] else ('N' if counts[8] else ('W' if counts[6] else 'E'))
         if stat != pgrec['wfstat']:
            pgrec['wfstat'] = record['wfstat'] = stat
            msg += " WF-" + stat
      if act&8:
         if pgrec['savedcnt'] != counts[10]:
            record['savedcnt'] = counts[10]
            msg += " SC-{}".format(record['savedcnt'])
         if pgrec['saved_size'] != counts[11]:
            record['saved_size'] = counts[11]
            msg += " SS-{}".format(record['saved_size'])
      if level:
         if level != pgrec['level']:
            record['level'] = level
         else:
            level = None
      if record:
         stat = ('P' if (pgrec['wfstat'] == 'D' or pgrec['mfstat'] == 'P') else
                 ('N' if (pgrec['wfstat'] == 'W' or pgrec['wfstat'] == 'N' or pgrec['mfstat'] == 'M') else 'E'))
         if stat != pgrec['dfstat']:
            record['dfstat'] = stat
            msg += " DF-" + stat
         if level:
            msg += " LVL-{}".format(level)
         if self.pgupdt(table, record, cnd, self.LGEREX):
            self.pglog(msg, self.LOGWRN)
            return 1
      return 0
   
   def record_savedfile_changes(self, dsid, gindex, record, pgrec=None):
      """
      Queue saved-file count adjustments in GCOUNTS for a pending DB write.

      Examines the old record (*pgrec*) and the new record being written
      (*record*) and calls record_filenumber() with the appropriate delta
      (+1 for add, -1 for remove, 0 for size-only change).

      Handles four cases:
        - New file (no pgrec, or old status was 'D')
        - File moved between datasets or groups
        - Same type, size changed (only for type 'P')
        - Type changed (from/to type 'P')

      Args:
         dsid   -- target dataset ID string
         gindex -- target group index
         record -- new/updated field dict being written to RDADB
         pgrec  -- existing database record dict, or None for a new file

      Returns:
         Number of record_filenumber() calls made (used as a change indicator).
      """
      ret = 0
      ostat = pgrec['status'] if pgrec else ""
      otype = pgrec['type'] if pgrec else ""
      stat = record['status'] if 'status' in record and record['status'] else ostat
      type = record['type'] if 'type' in record and record['type'] else otype
      size = record['data_size'] if 'data_size' in record and record['data_size'] else (pgrec['data_size'] if pgrec else 0)
      if otype == 'P' and ostat != 'P': otype = ''
      if type == 'P' and stat != 'P': type = ''
      if not pgrec or pgrec['status'] == 'D' or pgrec['dsid'] == self.PGLOG['DEFDSID']:   # new file record
         ret += self.record_filenumber(dsid, gindex, 8, type, 1, size)
      elif dsid != pgrec['dsid'] or gindex != pgrec['gindex']:   # file moved from one dataset/group to another
         ret += self.record_filenumber(dsid, gindex, 8, type, 1, size)
         ret += self.record_filenumber(pgrec['dsid'], pgrec['gindex'], 8, otype, -1, -pgrec['data_size'])
      elif type == otype:   # same type data
         if size != pgrec['data_size'] and type == "P":   # P type data size changed
            ret += self.record_filenumber(dsid, gindex, 8, "P", 0, (size - pgrec['data_size']))
      elif type == 'P' or otype == 'P':   # different types
         ret += self.record_filenumber(dsid, gindex, 8, type, 1, size)
         ret += self.record_filenumber(dsid, gindex, 8, otype, -1, -pgrec['data_size'])
      return ret
   
   def record_webfile_changes(self, dsid, gindex, record, pgrec=None):
      """
      Queue web-file count adjustments in GCOUNTS for a pending DB write.

      Mirrors record_savedfile_changes() for the wfile table.  Only counts
      files whose status is 'P' (public); non-public files contribute to the
      total webcnt but not to dwebcnt/nwebcnt.

      Handles four cases:
        - New file (no pgrec, or old status was 'D')
        - File moved between datasets or groups
        - Same type, size changed (only for types 'D' or 'N')
        - Type changed (from/to 'D' or 'N')

      Args:
         dsid   -- target dataset ID string
         gindex -- target group index
         record -- new/updated field dict being written to RDADB
         pgrec  -- existing database record dict, or None for a new file

      Returns:
         Number of record_filenumber() calls made (used as a change indicator).
      """
      ret = 0
      ostat = pgrec['status'] if pgrec else ""
      otype = pgrec['type'] if pgrec else ""
      stat = record['status'] if 'status' in record and record['status'] else ostat
      type = record['type'] if 'type' in record and record['type'] else otype
      size = record['data_size'] if 'data_size' in record and record['data_size'] else (pgrec['data_size'] if pgrec else 0)
      if ostat != 'P': otype = ''
      if stat != 'P': type = ''
      if not pgrec or pgrec['status'] == 'D':   # new file record
         ret += self.record_filenumber(dsid, gindex, 4, type, 1, size)
      elif dsid != pgrec['dsid'] or gindex != pgrec['gindex']:   # file moved from one dataset/group to another
         ret += self.record_filenumber(dsid, gindex, 4, type, 1, size)
         ret += self.record_filenumber(pgrec['dsid'], pgrec['gindex'], 2, otype, -1, -pgrec['data_size'])
      elif type == otype:   # same type data
         if size != pgrec['data_size'] and (type == "D" or type == "N"):   # D or N type data size changed
            ret += self.record_filenumber(dsid, gindex, 4, type, 0, (size - pgrec['data_size']))
      elif type == 'D' or type == 'N' or otype == 'D' or otype == 'N':   # different types
         ret += self.record_filenumber(dsid, gindex, 4, type, 1, size)
         ret += self.record_filenumber(dsid, gindex, 4, otype, -1, -pgrec['data_size'])
      return ret
   
   def record_filenumber(self, dsid, gindex, act, type, cnt, size):
      """
      Apply an incremental file-count delta to the in-memory GCOUNTS cache.

      Lazily creates the GCOUNTS[dsid][gindex] entry on first access and
      looks up the group's type ('P' or not) from the database.  The cached
      counts are later flushed to RDADB by save_filenumber() or add_filenumber().

      Counts array slots updated (see class docstring for full layout):
         act & 4, type 'D' → slots 4, 5, 7 (D-type web count/size)
         act & 4, type 'N' → slots 8, 9  (N-type web count/size)
         act & 4           → slot 6       (total web count always)
         act & 8           → slots 10, 11 (saved count/size)
         act == 1          → all of the above (act is treated as 12)

      Args:
         dsid   -- dataset ID string
         gindex -- group index (0 = dataset level)
         act    -- bitmask: 1=all, 4=web, 8=saved
         type   -- file type character ('D', 'N', 'P', etc.) or empty
         cnt    -- delta count (+1 for add, -1 for remove, 0 for size-only)
         size   -- delta size in bytes (positive for add, negative for remove)

      Returns:
         Always 1 (signals that a change was recorded).
      """
      if dsid not in self.GCOUNTS: self.GCOUNTS[dsid] = {}
      if gindex not in self.GCOUNTS[dsid]:
         # 0-pms,1-cpm,2-pm,3-mc,4-cdc,5-dc,6-wc,7-ds,8-nc,9-ns,10-sc,11-ss # 12-flag(1-group type is P)
         counts = self.GCOUNTS[dsid][gindex] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
         gcnd = "dsid = '{}' AND gindex = {} AND grptype = 'P'".format(dsid, gindex)
         if gindex and not self.pgget("dsgroup", "", gcnd, self.LGEREX): counts[12] = 0
      else:
         counts = self.GCOUNTS[dsid][gindex]
      if type and not counts[12]: type = ''
      if act == 1: act = 12  # for all
      if act&4:
         if type == 'D':
            counts[4] += cnt
            counts[5] += cnt
            if size: counts[7] += size
         if type == 'N':
            counts[8] += cnt
            if size: counts[9] += size
         counts[6] += cnt
      if act&8:
         counts[10] += cnt
         if size: counts[11] += size
      return 1
   
   def save_filenumber(self, dsid, act, reset, dosave=0):
      """
      Flush all pending GCOUNTS entries for *dsid* to RDADB.

      Iterates over every (dsid, gindex) pair in GCOUNTS, propagates counts
      up the group hierarchy via group_filenumber(), then writes each updated
      group/dataset row with add_filenumber().  Clears the processed entries
      from GCOUNTS after writing.

      Args:
         dsid   -- dataset ID to flush, or None to flush all datasets
         act    -- bitmask controlling which count columns to update (4=web, 8=saved)
         reset  -- if non-zero, calls reset_rdadb_version() when any record is updated
         dosave -- if 0, discard the cached counts without writing (dry-run / cancel)

      Returns:
         Total number of database records updated across all flushed datasets.
      """
      dsids = [dsid] if dsid else list(self.GCOUNTS)
      ret = 0
      for dsid in dsids:
         if dsid not in self.GCOUNTS: continue
         if not dosave:
            del self.GCOUNTS[dsid]
            continue
         gindexs = sorted(self.GCOUNTS[dsid])
         if not gindexs: continue
         for gindex in gindexs:
            if not gindex: continue
            gcnts = self.GCOUNTS[dsid][gindex]
            while gindex:
               # loop to pass the group file counts upto the dataset
               gindex = self.group_filenumber(dsid, gindex, act, gcnts)
         # now update the file numbers
         gindexs = sorted(self.GCOUNTS[dsid])
         gcnt = len(gindexs)
         s = 's' if gcnt > 1 else ''
         self.pglog("Reset file counts for {} Groups of {} ...".format(gcnt, dsid), self.WARNLG)
         gcnt = 0
         for gindex in gindexs:
            gcnt += self.add_filenumber(dsid, gindex, act, self.GCOUNTS[dsid][gindex])
         del self.GCOUNTS[dsid]
         s = 's' if gcnt > 1 else ''
         self.pglog("{} Dataset/Group Record{} set for file counts of {}".format(gcnt, s, dsid), self.WARNLG)
         if reset and gcnt: self.reset_rdadb_version(dsid)
         ret += gcnt
      return ret
   
   def group_filenumber(self, dsid, gindex, act, gcnts):
      """
      Propagate a child group's file counts into its parent's GCOUNTS entry.

      Looks up the parent index (pindex) and group type for *gindex*, creates
      the parent entry in GCOUNTS[dsid] if needed, and accumulates *gcnts*
      into the parent's counts array.  Only merges D/N-type web counts and
      sizes when the parent group type is 'P'.

      Args:
         dsid   -- dataset ID string
         gindex -- child group index whose counts are being propagated
         act    -- bitmask: 4=web counts, 8=saved counts
         gcnts  -- counts list from GCOUNTS[dsid][gindex] to merge upward

      Returns:
         Parent group index (pidx), or 0 if the group record is not found.
      """
      pgrec = self.pgget("dsgroup", "pindex, grptype", "dsid = '{}' AND gindex = {}".format(dsid, gindex), self.LGEREX)
      if not pgrec: return 0    # should not happen
      pidx = pgrec['pindex']
      pflag = (1 if pgrec['grptype'] == 'P' else 0)
      if pidx not in self.GCOUNTS[dsid]: self.GCOUNTS[dsid][pidx] = [0]*13
      pcnts = self.GCOUNTS[dsid][pidx]
      pcnts[12] = pflag
      if act&4:
         if pflag:
            pcnts[5] += gcnts[5]
            pcnts[7] += gcnts[7]
            pcnts[8] += gcnts[8]
            pcnts[9] += gcnts[9]
         pcnts[6] += gcnts[6]
      if act&8:
         pcnts[10] += gcnts[10]
         pcnts[11] += gcnts[11]
      return pidx
   
   def add_filenumber(self, dsid, gindex, act, counts):
      """
      Apply incremental file-count deltas from *counts* to a database row.

      Reads the current column values for the group or dataset row, adds the
      non-zero deltas from *counts*, and issues a single UPDATE statement for
      all changed columns.  Also updates wfstat and dfstat when web counts
      change.

      This is the incremental counterpart to update_filenumber(): it uses
      ``col = col + delta`` SQL expressions rather than absolute values.

      Args:
         dsid   -- dataset ID string
         gindex -- group index (0 = dataset table row)
         act    -- bitmask: 2=MSS, 4=web, 8=saved (selects which fields to touch)
         counts -- counts list with delta values to add (see class docstring for layout)

      Returns:
         1 if the UPDATE was executed, 0 if no columns needed changing.
      """
      fields = ['primary_size', 'cpmcnt', 'pmsscnt', 'msscnt', 'cdwcnt', 'dwebcnt',
                'webcnt', 'dweb_size', 'nwebcnt', 'nweb_size', 'savedcnt', 'saved_size']
      shorts = ['PS', 'CPC', 'PC', 'MC', 'CDC', 'DC', 'WC', 'DS', 'NC', 'NS', 'SC', 'SS']
      i = 0 if act&2 else (4 if act&4 else 10)
      j = 12 if act&8 else (10 if act&4 else 4)
      if i >= j: return 0    # should not happen
      if gindex:
         table = "dsgroup"
         cnd = "dsid = '{}' AND gindex = {}".format(dsid, gindex)
         msg = "{}-G{}".format(dsid, gindex)
         flds = "gidx, "
      else:
         table = "dataset"
         cnd = "dsid = '{}'".format(dsid) 
         msg = dsid + ":"
         flds = ''
      flds += ', '.join(fields[i:j]) + ", mfstat, wfstat, dfstat"
      pgrec = self.pgget(table, flds, cnd, self.LGEREX)
      if not pgrec: return self.pglog("Group Index {} not exists for '{}'".format(gindex, dsid), self.LOGWRN)
      sqlary = []
      while i < j:
         if counts[i]:
            sqlary.append("{} = {} + {}".format(fields[i], fields[i], counts[i]))
            counts[i] += pgrec[fields[i]]
            msg += " {}-{}".format(shorts[i], counts[i])
         else:
            counts[i] = pgrec[fields[i]]
         i += 1
      if act&4:
         stat = 'D' if counts[5] else ('W' if counts[6] else 'E')
         if stat != pgrec['wfstat']:
            sqlary.append("wfstat = '{}'".format(stat))
            pgrec['wfstat'] = stat
            msg += " WF-" + stat
      if not sqlary: return 0   # nothing needs change
      stat = ('P' if (pgrec['wfstat'] == 'D' or pgrec['mfstat'] == 'P') else
              ('N' if (pgrec['wfstat'] == 'W' or pgrec['wfstat'] == 'N' or pgrec['mfstat'] == 'M') else 'E'))
      if stat != pgrec['dfstat']:
         sqlary.append("dfstat = '{}'".format(stat))
         pgrec['dfstat'] = stat
         msg += " DF-" + stat
      if gindex: cnd = "gidx = {}".format(pgrec['gidx'])
      if self.pgexec("UPDATE {} SET {} WHERE {}".format(table,  ', '.join(sqlary), cnd), self.LGEREX):
         self.pglog(msg, self.LOGWRN)
         return 1
      return 0
   
   def record_meta_gather(self, cate, dsid, file, fmt, lfile=None, mfile=None):
      """
      Queue a metadata-XML gather operation for a web or MSS file.

      Appends an entry to META['GW'] (web) or META['GM'] (MSS, no-op).
      The queued operations are executed later by process_meta_gather().

      Args:
         cate  -- file category: 'W' for web files, 'M' for MSS (skipped)
         dsid  -- dataset ID string
         file  -- archived file path (used as the gatherxml target)
         fmt   -- data format string (e.g. 'netcdf', 'grib'); required
         lfile -- optional local file path for gatherxml -l flag
         mfile -- optional metadata file path for gatherxml -m flag

      Returns:
         1 if the operation was queued, 0 if skipped (MSS or missing format).
      """
      if cate == 'M': return 0
      if not fmt: return self.pglog("{}-{}: Miss Data Format for 'gatherxml'".format(dsid, file), self.LOGERR)
      c = "G" + cate
      if c not in self.META: self.META[c] = {}
      d = self.metadata_dataset_id(dsid)
      if d not in self.META[c]: self.META[c][d] = []
      f = fmt.lower()
      if f == "netcdf": f = "cf" + f
      if not lfile: lfile = op.basename(file)
      self.META[c][d].append([file, f, lfile, mfile])
      return 1
   
   def record_meta_delete(self, cate, dsid, file):
      """
      Queue a metadata-XML deletion operation for a web or MSS file.

      Appends an entry to META['DW'] (web) or META['DM'] (MSS, no-op).
      Duplicate entries for the same file are silently ignored.
      The queued operations are executed later by process_meta_delete().

      Args:
         cate -- file category: 'W' for web files, 'M' for MSS (skipped)
         dsid -- dataset ID string
         file -- archived file path whose metadata XML should be deleted

      Returns:
         1 if the operation was newly queued, 0 if already queued or MSS.
      """
      if cate == 'M': return 0
      c = "D" + cate
      if c not in self.META: self.META[c] = {}
      d = self.metadata_dataset_id(dsid)
      if d not in self.META[c]: self.META[c][d] = {}
      if file not in self.META[c][d]:
         self.META[c][d][file] = ''
         return 1
      return 0
   
   def record_meta_move(self, cate, dsid, ndsid, file, nfile):
      """
      Queue a metadata-XML rename/move operation for a web or MSS file.

      Appends an entry to META['RW'] (web) or META['RM'] (MSS, no-op).
      Duplicate entries for the same source file are silently ignored.
      The queued operations are executed later by process_meta_move().

      Args:
         cate  -- file category: 'W' for web files, 'M' for MSS (skipped)
         dsid  -- source dataset ID string
         ndsid -- destination dataset ID (may equal dsid for same-dataset moves)
         file  -- original archived file path
         nfile -- new archived file path

      Returns:
         1 if the operation was newly queued, 0 if already queued or MSS.
      """
      if cate == 'M': return 0
      c = "R" + cate
      if c not in self.META: self.META[c] = {}
      d = self.metadata_dataset_id(dsid)
      if d not in self.META[c]: self.META[c][d] = {}
      if file not in self.META[c][d]:
         n = None if ndsid == dsid else self.metadata_dataset_id(ndsid)
         self.META[c][d][file] = [n, nfile]
         return 1
      return 0
   
   def record_meta_summary(self, cate, dsid, gindex, gindex1=None):
      """
      Queue a group-level metadata-summary operation for a web or MSS file.

      Appends group indices to META['SW'] (web) or META['SM'] (MSS, no-op).
      Duplicate group indices are silently ignored.
      The queued operations are executed later by process_meta_summary().

      Args:
         cate    -- file category: 'W' for web files, 'M' for MSS (skipped)
         dsid    -- dataset ID string
         gindex  -- primary group index to summarise
         gindex1 -- optional second group index (e.g. old group after a move)

      Returns:
         Number of group indices newly queued (0, 1, or 2).
      """
      if cate == 'M': return 0
      ret = 0
      c = "S" + cate
      if c not in self.META: self.META[c] = {}
      d = self.metadata_dataset_id(dsid)
      if d not in self.META[c]: self.META[c][d] = {}
      if gindex not in self.META[c][d]:
         self.META[c][d][gindex] = 1
         ret += 1
      if  not (gindex1 is None or gindex1 in self.META[c][d]):
         self.META[c][d][gindex1] = 1
         ret += 1
      return ret
   
   def process_metadata(self, cate, metacnt, logact=None):
      """
      Execute all queued metadata operations for file category *cate*.

      Dispatches to process_meta_delete(), process_meta_move(),
      process_meta_summary(), and process_meta_gather() in that order.
      No-ops when *cate* is 'M' (MSS files have no metadata XML).

      Args:
         cate     -- file category: 'W' for web files, 'M' for MSS (skipped)
         metacnt  -- current queued operation count (informational, unused internally)
         logact   -- log action flags; defaults to self.LOGWRN

      Returns:
         Total number of metadata operations executed.
      """
      if logact is None: logact = self.LOGWRN
      if cate == 'M': return 0
      cnt = 0
      cnt += self.process_meta_delete(cate, logact)
      cnt += self.process_meta_move(cate, logact)
      cnt += self.process_meta_summary(cate, logact)
      cnt += self.process_meta_gather(cate, logact)
      return cnt
   
   def process_meta_gather(self, cate, logact=None):
      """
      Execute all queued gatherxml operations from META['GW'].

      Iterates over the queued (file, format, lfile, mfile) entries and runs
      the ``gatherxml`` command for each via start_background().  When a
      single dataset has more than two files queued, also runs the ``scm``
      group-summary command for each unique top-group index tracked in
      self.TIDXS.  Clears META['GW'] and self.TIDXS on completion.

      Args:
         cate   -- file category: 'W' for web files, 'M' for MSS (skipped)
         logact -- log action flags; defaults to self.LOGWRN

      Returns:
         Number of gatherxml commands launched.
      """
      if logact is None: logact = self.LOGWRN
      if cate == 'M': return 0
      c = 'G' + cate
      if c not in self.META: return 0
      opt = 5
      act = logact
      if act&self.EXITLG:
         act &= ~self.EXITLG
         if self.PGLOG['DSCHECK']: opt |= 1280  # 256 + 1024
      self.PGLOG['ERR2STD'] = ["Warning: ", "already up-to-date", "process currently running",
                                "rsync", "No route to host", "''*'"]
      self.switch_logfile("gatherxml")
      dary = list(self.META[c])
      dcnt = len(dary)
      cnt = 0
      sx = ''
      rs = self.PGLOG['RSOptions']
      if not rs and dcnt == 1:
         if len(self.META[c][dary[0]]) > 2:
            sx = "{} -d {} -r{} ".format(self.CMD['SX'], dary[0], ('m' if cate == 'M' else 'w'))
            rs = " -S -R"
      for d in dary:
         for ary in self.META[c][d]:
            cmd = "{} -d {} -f {}{} ".format(self.CMD['GX'], d, ary[1], rs)
            if cate == 'M':
               if ary[2] and op.exists(ary[2]): cmd += "-l {} ".format(ary[2])
               if ary[3]: cmd += "-m {} ".format(ary[3])
            cmd += ary[0]
            if self.start_background(cmd, act, opt, 1):
               cnt += 1
               if self.PGLOG['DSCHECK'] and cnt > 0 and (cnt%10) == 0:
                  self.add_dscheck_dcount(10, 0, logact)
            elif self.PGLOG['SYSERR']:
               self.record_dscheck_error(self.PGLOG['SYSERR'], act)
      self.PGLOG['ERR2STD'] = []
      if cnt > 0:
         if self.PGSIG['BPROC'] > 1: self.check_background(None, 0, logact, 1)
         if cnt > 1: self.pglog("Metadata gathered for {} files".format(cnt), self.WARNLG)
         if sx:
            if 'all' in self.TIDXS:
               self.pgsystem(sx + 'all', act, opt)
            else:
               for tidx in self.TIDXS:
                  self.pgsystem("{}{}".format(sx, tidx), act, opt)
      del self.META[c]
      self.TIDXS = {}
      self.switch_logfile()
      return cnt
   
   def process_meta_delete(self, cate, logact=None):
      """
      Execute all queued ``dcm`` (delete metadata) operations from META['DW'].

      Groups files by dataset and runs one ``dcm -d <dsid> <files...>`` command
      per dataset via start_background().  Clears META['DW'] on completion.

      Args:
         cate   -- file category: 'W' for web files, 'M' for MSS (skipped)
         logact -- log action flags; defaults to self.LOGWRN

      Returns:
         Number of files for which metadata deletion was launched.
      """
      if logact is None: logact = self.LOGWRN
      if cate == 'M': return 0
      c = 'D' + cate
      if c not in self.META: return 0
      cnt = 0
      opt = 5
      act = logact
      if act&self.EXITLG:
         act &= ~self.EXITLG
         if self.PGLOG['DSCHECK']: opt |= 1280  # 256 + 1024
      self.PGLOG['ERR2STD'] = ["Warning: "]
      self.switch_logfile("gatherxml")
      for d in self.META[c]:
         cmd = "{} -d {}".format(self.CMD['DX'], d)
         dcnt = 0
         for file in self.META[c][d]:
            cmd += " " + file
            dcnt += 1
         if dcnt > 0:
            if self.start_background(cmd, act, opt, 1):
               cnt += dcnt
            elif self.PGLOG['SYSERR']:
               self.record_dscheck_error(self.PGLOG['SYSERR'], act)
      self.PGLOG['ERR2STD'] = []
      if cnt > 0:
         if self.PGSIG['BPROC'] > 1: self.check_background(None, 0, logact, 1)
         if cnt > 1: self.pglog("Metadata deleted for {} files".format(cnt), self.WARNLG)
      del self.META[c]
      self.switch_logfile()
      return cnt
   
   def delete_file_metadata(self, dsid, file, logact=None):
      """
      Immediately delete the metadata XML for a single file (not queued).

      Runs ``dcm -d <dsid> <file>`` synchronously via pgsystem().  Use
      record_meta_delete() + process_meta_delete() for batched deletions.

      Args:
         dsid   -- dataset ID string
         file   -- archived file path whose metadata XML should be deleted
         logact -- log action flags; defaults to self.LOGWRN
      """
      if logact is None: logact = self.LOGWRN
      d = self.metadata_dataset_id(dsid)
      opt = 5
      if logact&self.EXITLG:
         logact &= ~self.EXITLG
         if self.PGLOG['DSCHECK']: opt |= 1280  # 256 + 1024
      self.PGLOG['ERR2STD'] = ["Warning: "]
      self.switch_logfile("gatherxml")
      if not self.pgsystem("{} -d {} {}".format(self.CMD['DX'], d, file), logact, opt) and self.PGLOG['SYSERR']:
         self.record_dscheck_error(self.PGLOG['SYSERR'], logact)
      self.PGLOG['ERR2STD'] = []
      self.switch_logfile()
   
   def process_meta_move(self, cate, logact=None):
      """
      Execute all queued ``rcm`` (rename/move metadata) operations from META['RW'].

      Runs one ``rcm -d <dsid> [-nd <newdsid>] [-C] <oldfile> <newfile>`` command
      per file via start_background().  The ``-C`` flag is added for all but the
      last file in a dataset batch to allow caching.  Clears META['RW'] on completion.

      Args:
         cate   -- file category: 'W' for web files, 'M' for MSS (skipped)
         logact -- log action flags; defaults to self.LOGWRN

      Returns:
         Number of metadata rename/move commands launched.
      """
      if logact is None: logact = self.LOGWRN
      if cate == 'M': return 0
      c = 'R' + cate
      if c not in self.META: return 0
      cnt = 0
      opt = 5
      act = logact
      if act&self.EXITLG:
         act &= ~self.EXITLG
         if self.PGLOG['DSCHECK']: opt |= 1280  # 256 + 1024
      self.PGLOG['ERR2STD'] = ["Warning: "]
      self.switch_logfile("gatherxml")
      for d in self.META[c]:
         dmeta = self.META[c][d]
         files = list(dmeta)
         fcnt = len(files)
         for i in range(fcnt):
            file = files[i]
            cmd = "{} -d {} ".format(self.CMD['RX'], d)
            n = dmeta[file][0]
            if n:
               cmd += "-nd {} ".format(n)
            elif i < fcnt - 1:
               cmd += "-C "
            cmd += "{} {}".format(file, dmeta[file][1])
            if self.start_background(cmd, act, opt, 1):
               cnt += 1
            elif self.PGLOG['SYSERR']:
               self.record_dscheck_error(self.PGLOG['SYSERR'], act)
      self.PGLOG['ERR2STD'] = []
      if cnt > 0:
         if self.PGSIG['BPROC'] > 1: self.check_background(None, 0, logact, 1)
         if cnt > 1: self.pglog("Metadata moved for {} files".format(cnt), self.WARNLG)
      del self.META[c]
      self.switch_logfile()
      return cnt
   
   def process_meta_summary(self, cate, logact=None):
      """
      Execute all queued ``scm`` (group-summary metadata) operations from META['SW'].

      Runs ``scm -d <dsid> -w <gindex>`` (or ``-m`` for MSS) for each queued
      group index via start_background().  Clears META['SW'] on completion.

      Args:
         cate   -- file category: 'W' for web files, 'M' for MSS (skipped)
         logact -- log action flags; defaults to self.LOGWRN

      Returns:
         Number of group-summary commands launched.
      """
      if logact is None: logact = self.LOGWRN
      if cate == 'M': return 0
      c = 'S' + cate
      if c not in self.META: return 0
      cnt = 0
      opt = 5
      ctype = cate.lower()
      act = logact
      if act&self.EXITLG:
         act &= ~self.EXITLG
         if self.PGLOG['DSCHECK']: opt |= 1280  # 256 + 1024
      self.PGLOG['ERR2STD'] = ["Warning: "]
      self.switch_logfile("gatherxml")
      for d in self.META[c]:
         for gindex in self.META[c][d]:
            cmd = "{} -d {} -{} {}".format(self.CMD['SX'], d, ctype, gindex)
            if self.start_background(cmd, act, opt, 1):
               cnt += 1
            elif self.PGLOG['SYSERR']:
               self.record_dscheck_error(self.PGLOG['SYSERR'], act)
      self.PGLOG['ERR2STD'] = []
      if cnt > 0:
         if self.PGSIG['BPROC'] > 1: self.check_background(None, 0, logact, 1)
         if cnt > 1: self.pglog("Group Metadata summarized for {} groups".format(cnt), self.WARNLG)
      del self.META[c]
      self.switch_logfile()
      return cnt
   
   def get_top_gindex(self, dsid, gindex, logact=None):
      """
      Return the top-level ancestor group index for *gindex*, with caching.

      Walks the pindex chain in the dsgroup table until reaching a group
      with no parent (pindex is 0/None).  Results are stored in self.TGIDXS
      so subsequent calls for the same gindex avoid extra database queries.

      Args:
         dsid   -- dataset ID string
         gindex -- starting group index
         logact -- log action flags; defaults to self.LGEREX

      Returns:
         Top-level ancestor group index (returns *gindex* itself if it has no parent).
      """
      if logact is None: logact = self.LGEREX
      if gindex in self.TGIDXS: return self.TGIDXS[gindex]
      gcnd = "dsid = '{}' AND gindex = {}".format(dsid, gindex)
      pgrec = self.pgget("dsgroup", "pindex", gcnd, logact)
      if pgrec and pgrec['pindex']:
         tindex = self.get_top_gindex(dsid, pgrec['pindex'])
      else:
         tindex = gindex
      self.TGIDXS[gindex] = tindex
      return tindex
   
   def cache_meta_tindex(self, dsid, id, type, logact=None):
      """
      Record the top-group index for a recently archived file into self.TIDXS.

      Looks up the tindex and gindex for the web file record with wid == *id*,
      resolves the top-level ancestor via get_top_gindex() if tindex is not
      set, and stores the result in self.TIDXS so that process_meta_gather()
      can later run ``scm`` for the affected top-level groups.  When no
      top-group is found (gindex == 0), stores the sentinel key 'all'.

      Args:
         dsid   -- dataset ID string
         id     -- web file record ID (wid)
         type   -- file category ('W' for web; reserved for future MSS use)
         logact -- log action flags; defaults to self.LGEREX
      """
      if logact is None: logact = self.LGEREX
      pgrec = self.pgget_wfile(dsid, "tindex, gindex", "wid = {}".format(id), logact)
      if pgrec:
         if pgrec['tindex']:
            tidx = pgrec['tindex']
         else:
            tidx = self.get_top_gindex(dsid, pgrec['gindex'], logact)
      else:
         tidx = 0
      if tidx:
         self.TIDXS[tidx] = 1
      else:
         self.TIDXS['all'] = 1
   
   def reset_top_gindex(self, dsid, gindex, act):
      """
      Recompute and write the tindex column for all files in *gindex* and its sub-groups.

      Finds the top-level ancestor of *gindex* via get_top_gindex(), then
      updates wfile.tindex (act & 4) and/or sfile.tindex (act & 8) for every
      file whose tindex currently differs.  Recurses into child groups.

      act bitmask:
         act & 4  -- update web files (wfile table)
         act & 8  -- update saved files (sfile table)
         act == 1 -- equivalent to act = 12 (both)

      Args:
         dsid   -- dataset ID string
         gindex -- group index to start from
         act    -- bitmask selecting which file tables to update

      Returns:
         Total number of file records updated.
      """
      tcnt = 0
      if act == 1: act = 12
      tindex = self.get_top_gindex(dsid, gindex)
      record = {'tindex' : tindex}
      tcnd = "gindex = {} AND tindex <> {}".format(gindex, tindex) 
      dcnd = "dsid = '{}'".format(dsid)
      if act&4:
         cnt = self.pgupdt_wfile(dsid, record, tcnd, self.LGEREX)
         if cnt > 0:
            s = 's' if cnt > 1 else ''
            self.pglog("set tindex {} for {} web file{} in gindex {} of {}".format(tindex, cnt, s, gindex, dsid), self.WARNLG)
            tcnt += cnt
      if act&8:
         cnt = self.pgupdt("sfile", record, dcnd + ' AND ' + tcnd, self.LGEREX)
         if cnt > 0:
            s = 's' if cnt > 1 else ''
            self.pglog("set tindex {} for {} saved file{} in gindex {} of {}".format(tindex, cnt, s, gindex, dsid), self.WARNLG)
            tcnt += cnt
      pcnd = "{} AND pindex = {}".format(dcnd, gindex)
      pgrecs = self.pgmget("dsgroup", "gindex", pcnd, self.LGEREX)
      cnt = len(pgrecs['gindex']) if pgrecs else 0
      for i in range(cnt):
         tcnt += self.reset_top_gindex(dsid, pgrecs['gindex'][i], act)
      return tcnt
   
   def set_meta_link(self, dsid, fname):
      """
      Update the meta_link column for a web file via the ``sml`` command.

      Runs ``sml -d <dsid> <fname>`` to set or refresh the metadata link
      field in the wfile RDADB table.

      Args:
         dsid  -- dataset ID string
         fname -- web file name to update the meta_link for

      Returns:
         Return value from pgsystem() (0 = success, non-zero = failure).
      """
      return self.pgsystem("{} -d {} {}".format(self.CMD['SL'], dsid, fname))
