#
###############################################################################
#
#     Title : PgMeta.py
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 09/18/2020
#             2025-01-22 transferred to package rda_python_dsarch from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module to handle file counts and metadata gathering
#
#    Github : https://github.com/NCAR/rda-python-dsarch.git
#
###############################################################################
#
import os
import re
from os import path as op
from rda_python_common import PgLOG
from rda_python_common import PgSIG
from rda_python_common import PgCMD
from rda_python_common import PgDBI
from rda_python_common import PgUtil
from rda_python_common import PgSplit

GCOUNTS = {}
META = {}     # keys: GM, GW, DM, DW, RM, RW, SM, and SW
TGIDXS = {}   # cache value for function get_top_gindex()
TIDXS = {}    # cache all unique top group index

CMD = {
   'GX' : "gatherxml",
   'DX' : "dcm",
   'RX' : "rcm",
   'SX' : "scm",
   'SL' : "sml"
}

logfile = PgLOG.PGLOG['LOGFILE']
errfile = PgLOG.PGLOG['ERRFILE']

#
# switch to log file name provided
#
def switch_logfile(logname = None):

   global logfile, errfile

   if logname:
      logfile = PgLOG.PGLOG['LOGFILE']
      errfile = PgLOG.PGLOG['ERRFILE']
      PgLOG.PGLOG['LOGFILE'] = logname + '.log'
      PgLOG.PGLOG['ERRFILE'] = logname + '.err'
   else:
      PgLOG.PGLOG['LOGFILE'] = logfile
      PgLOG.PGLOG['ERRFILE'] = errfile

#
# get sub group level
#
def get_group_levels(dsid, gindex, level):

   pgrec = PgDBI.pgget("dsgroup", "pindex", "dsid = '{}' AND gindex = {}".format(dsid, gindex), PgLOG.LGEREX)
   if pgrec and pgrec['pindex']:
      level = get_group_levels(dsid, pgrec['pindex'], level + 1)

   return level

#
# reset datset/group file counts. Set all the way through if index is 0
# act == 4/8 change web/saved file counts, respectively
# go up if limit < 0 
#
def reset_filenumber(dsid, gindex, act, level = 0, gtype = None, limit = 0):

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
         pgrec = PgDBI.pgget("dsgroup", "pindex, grptype", gcnd, PgLOG.LGEREX)
         if not pgrec or abs(pgrec['pindex']) >= abs(gindex): return 0
         gtype = pgrec['grptype']
         pidx = pgrec['pindex']
      else:
         gtype = 'P'

      if not level:
         if act == 1: act = 14
         if gindex:
            level += 1
            if pidx: level = get_group_levels(dsid, pidx, level+1)
            if limit > 0: limit += level

   if not level: PgDBI.pgget("dataset", "dsid", dcnd, PgLOG.LGEREX|PgLOG.DOLOCK)

   # get file counts at the current group
   if act&4:
      if gtype == 'P':
         pgrec = PgSplit.pgget_wfile(dsid, "SUM(data_size) dsize, COUNT(wid) dcount", 
                            wcnd + " AND type = 'D' AND status = 'P'", PgLOG.LGEREX)
         if pgrec:
            counts[5] = counts[4] = pgrec['dcount']
            if pgrec['dsize']: counts[7] = pgrec['dsize']

         pgrec = PgSplit.pgget_wfile(dsid, "SUM(data_size) nsize, COUNT(wid) ncount",
                            wcnd + " AND type = 'N' AND status = 'P'", PgLOG.LGEREX)
         if pgrec:
            counts[8] = pgrec['ncount']
            if pgrec['nsize']: counts[9] = pgrec['nsize']
      counts[6] = PgSplit.pgget_wfile(dsid, "", wcnd, PgLOG.LGEREX)

   if act&8:
      pgrec = PgDBI.pgget("sfile", "SUM(data_size) ssize, COUNT(sid) scount",
                         gcnd + " AND status <> 'D'", PgLOG.LGEREX)
      if pgrec:
         counts[10] = pgrec['scount']
         if pgrec['ssize']: counts[11] = pgrec['ssize']

   pcnd = "{} AND pindex = {}".format(dcnd, gindex)
   if limit < 0:
      flds = "gindex"
      if act&4: flds += ", dwebcnt, webcnt, nwebcnt, dweb_size, nweb_size"
      if act&8: flds += ", savedcnt, saved_size"

      grecs = PgDBI.pgmget("dsgroup", flds, pcnd, PgLOG.LGEREX)
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
      cnt = update_filenumber(dsid, gindex, act, counts, level)
      if cnt:
         if pidx != None: cnt += reset_filenumber(dsid, pidx, act, (level-1), None, -1)
         counts[11] += cnt
   elif limit == 0 or level < limit:
      grecs = PgDBI.pgmget("dsgroup", "gindex, grptype", pcnd, PgLOG.LGEREX)
      gcnt = len(grecs['gindex']) if grecs else 0
      for i in range(gcnt):
         gidx = grecs['gindex'][i]
         if abs(gidx) <= abs(gindex): continue
         subcnts = reset_filenumber(dsid, gidx, act, level+1, grecs['grptype'][i], limit)
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

      cnt = update_filenumber(dsid, gindex, act, counts, level)
      if cnt:
         if pidx != None: cnt += reset_filenumber(dsid, pidx, act, level-1, None, -1)
         counts[12] += cnt

   if level == 0: PgDBI.endtran()
   return (counts[12] if retcnt else counts)

#
# update group/dataset file counts
#
def update_filenumber(dsid, gindex, act, counts, level = 0):

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

   pgrec = PgDBI.pgget(table, flds, cnd, PgLOG.LGEREX)
   if not pgrec: return PgLOG.pglog("Group Index {} not exists for '{}'".format(gindex, dsid), PgLOG.LOGWRN)

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

      if PgDBI.pgupdt(table, record, cnd, PgLOG.LGEREX):
         PgLOG.pglog(msg, PgLOG.LOGWRN)
         return 1

   return 0

#
# cache changes of the saved files
#
def record_savedfile_changes(dsid, gindex, record, pgrec = None):

   ret = 0
   ostat = pgrec['status'] if pgrec else ""
   otype = pgrec['type'] if pgrec else ""
   stat = record['status'] if 'status' in record and record['status'] else ostat
   type = record['type'] if 'type' in record and record['type'] else otype
   size = record['data_size'] if 'data_size' in record and record['data_size'] else (pgrec['data_size'] if pgrec else 0)

   if otype == 'P' and ostat != 'P': otype = ''
   if type == 'P' and stat != 'P': type = ''

   if not pgrec or pgrec['status'] == 'D' or pgrec['dsid'] == PgLOG.PGLOG['DEFDSID']:   # new file record
      ret += record_filenumber(dsid, gindex, 8, type, 1, size)
   elif dsid != pgrec['dsid'] or gindex != pgrec['gindex']:   # file moved from one dataset/group to another
      ret += record_filenumber(dsid, gindex, 8, type, 1, size)
      ret += record_filenumber(pgrec['dsid'], pgrec['gindex'], 8, otype, -1, -pgrec['data_size'])
   elif type == otype:   # same type data
      if size != pgrec['data_size'] and type == "P":   # P type data size changed
         ret += record_filenumber(dsid, gindex, 8, "P", 0, (size - pgrec['data_size']))
   elif type == 'P' or otype == 'P':   # different types
      ret += record_filenumber(dsid, gindex, 8, type, 1, size)
      ret += record_filenumber(dsid, gindex, 8, otype, -1, -pgrec['data_size'])

   return ret

#
# cache changes of web files
#
def record_webfile_changes(dsid, gindex, record, pgrec = None):

   ret = 0
   ostat = pgrec['status'] if pgrec else ""
   otype = pgrec['type'] if pgrec else ""
   stat = record['status'] if 'status' in record and record['status'] else ostat
   type = record['type'] if 'type' in record and record['type'] else otype
   size = record['data_size'] if 'data_size' in record and record['data_size'] else (pgrec['data_size'] if pgrec else 0)

   if ostat != 'P': otype = ''
   if stat != 'P': type = ''

   if not pgrec or pgrec['status'] == 'D':   # new file record
      ret += record_filenumber(dsid, gindex, 4, type, 1, size)
   elif dsid != pgrec['dsid'] or gindex != pgrec['gindex']:   # file moved from one dataset/group to another
      ret += record_filenumber(dsid, gindex, 4, type, 1, size)
      ret += record_filenumber(pgrec['dsid'], pgrec['gindex'], 2, otype, -1, -pgrec['data_size'])
   elif type == otype:   # same type data
      if size != pgrec['data_size'] and (type == "D" or type == "N"):   # D or N type data size changed
         ret += record_filenumber(dsid, gindex, 4, type, 0, (size - pgrec['data_size']))
   elif type == 'D' or type == 'N' or otype == 'D' or otype == 'N':   # different types
      ret += record_filenumber(dsid, gindex, 4, type, 1, size)
      ret += record_filenumber(dsid, gindex, 4, otype, -1, -pgrec['data_size'])

   return ret

#
# record group file counts act&4 for webfile and act&8 for savedfile
#
def record_filenumber(dsid, gindex, act, type, cnt, size):

   if dsid not in GCOUNTS: GCOUNTS[dsid] = {}
   if gindex not in GCOUNTS[dsid]:
      # 0-pms,1-cpm,2-pm,3-mc,4-cdc,5-dc,6-wc,7-ds,8-nc,9-ns,10-sc,11-ss # 12-flag(1-group type is P)
      counts = GCOUNTS[dsid][gindex] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
      gcnd = "dsid = '{}' AND gindex = {} AND grptype = 'P'".format(dsid, gindex)
      if gindex and not PgDBI.pgget("dsgroup", "", gcnd, PgLOG.LGEREX): counts[12] = 0
   else:
      counts = GCOUNTS[dsid][gindex]
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

#
# save the recorded COUNTS of group files into RDADB for given dsid
#
def save_filenumber(dsid, act, reset, dosave = 0):

   dsids = [dsid] if dsid else list(GCOUNTS)
   ret = 0
   for dsid in dsids:
      if dsid not in GCOUNTS: continue
      if not dosave:
         del GCOUNTS[dsid]
         continue

      gindexs = sorted(GCOUNTS[dsid])
      if not gindexs: continue
      for gindex in gindexs:
         if not gindex: continue
         gcnts = GCOUNTS[dsid][gindex]
         while gindex:
            # loop to pass the group file counts upto the dataset
            gindex = group_filenumber(dsid, gindex, act, gcnts)

      # now update the file numbers
      gindexs = sorted(GCOUNTS[dsid])
      gcnt = len(gindexs)
      s = 's' if gcnt > 1 else ''
      PgLOG.pglog("Reset file counts for {} Groups of {} ...".format(gcnt, dsid), PgLOG.WARNLG)
      gcnt = 0
      for gindex in gindexs:
         gcnt += add_filenumber(dsid, gindex, act, GCOUNTS[dsid][gindex])
      del GCOUNTS[dsid]
      s = 's' if gcnt > 1 else ''
      PgLOG.pglog("{} Dataset/Group Record{} set for file counts of {}".format(gcnt, s, dsid), PgLOG.WARNLG)
      if reset and gcnt: PgDBI.reset_rdadb_version(dsid)
      ret += gcnt

   return ret

#
#  add group file counts to parent group
#
def group_filenumber(dsid, gindex, act, gcnts):

   pgrec = PgDBI.pgget("dsgroup", "pindex, grptype", "dsid = '{}' AND gindex = {}".format(dsid, gindex), PgLOG.LGEREX)
   if not pgrec: return 0    # should not happen

   pidx = pgrec['pindex']
   pflag = (1 if pgrec['grptype'] == 'P' else 0)
   if pidx not in GCOUNTS[dsid]: GCOUNTS[dsid][pidx] = [0]*13
   pcnts = GCOUNTS[dsid][pidx]
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

#
# add group/dataset file counts to RDADB
def add_filenumber(dsid, gindex, act, counts):

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
   pgrec = PgDBI.pgget(table, flds, cnd, PgLOG.LGEREX)
   if not pgrec: return PgLOG.pglog("Group Index {} not exists for '{}'".format(gindex, dsid), PgLOG.LOGWRN)

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
   if PgDBI.pgexec("UPDATE {} SET {} WHERE {}".format(table,  ', '.join(sqlary), cnd), PgLOG.LGEREX):
      PgLOG.pglog(msg, PgLOG.LOGWRN)
      return 1

   return 0

#
# record actions of gathering xml metadata for mssfile/webfile
# cate: M or W
#
def record_meta_gather(cate, dsid, file, fmt, lfile = None, mfile = None):

   if cate == 'M': return 0

   if not fmt: return PgLOG.pglog("{}-{}: Miss Data Format for 'gatherxml'".format(dsid, file), PgLOG.LOGERR)

   c = "G" + cate
   if c not in META: META[c] = {}
   d = PgUtil.metadata_dataset_id(dsid)
   if d not in META[c]: META[c][d] = []
   f = fmt.lower()
   if f == "netcdf": f = "cf" + f
   if not lfile: lfile = op.basename(file)
   META[c][d].append([file, f, lfile, mfile])

   return 1

#
# record actions of deleting xml metadata for mssfile/webfile
# cate: M or W
#
def record_meta_delete(cate, dsid, file):

   if cate == 'M': return 0
   c = "D" + cate
   if c not in META: META[c] = {}
   d = PgUtil.metadata_dataset_id(dsid)
   if d not in META[c]: META[c][d] = {}
   if file not in META[c][d]:
      META[c][d][file] = ''
      return 1

   return 0

#
# record actions of moving xml metadata for mssfile/webfile
# cate: M or W
#
def record_meta_move(cate, dsid, ndsid, file, nfile):

   if cate == 'M': return 0
   c = "R" + cate
   if c not in META: META[c] = {}
   d = PgUtil.metadata_dataset_id(dsid)
   if d not in META[c]: META[c][d] = {}
   if file not in META[c][d]:
      n = None if ndsid == dsid else PgUtil.metadata_dataset_id(ndsid)
      META[c][d][file] = [n, nfile]
      return 1

   return 0

#
# record actions of getting group summary xml metadata for mssfile/webfile
# cate: M or W
#
def record_meta_summary(cate, dsid, gindex, gindex1 = None):

   if cate == 'M': return 0
   ret = 0
   c = "S" + cate
   if c not in META: META[c] = {}
   d = PgUtil.metadata_dataset_id(dsid)
   if d not in META[c]: META[c][d] = {}
   if gindex not in META[c][d]:
      META[c][d][gindex] = 1
      ret += 1
   if  not (gindex1 is None or gindex1 in META[c][d]):
      META[c][d][gindex1] = 1
      ret += 1

   return ret

#
# process all cached metadata actions cat == M or W
#
def process_metadata(cate, metacnt, logact = PgLOG.LOGWRN):

   if cate == 'M': return 0
   cnt = 0
   cnt += process_meta_delete(cate, logact)
   cnt += process_meta_move(cate, logact)
   cnt += process_meta_summary(cate, logact)
   cnt += process_meta_gather(cate, logact)

   return cnt

#
# process cached metadata gathering actions cate: M or W
#
def process_meta_gather(cate, logact = PgLOG.LOGWRN):

   global TIDXS

   if cate == 'M': return 0
   c = 'G' + cate
   if c not in META: return 0
   opt = 5
   act = logact
   if act&PgLOG.EXITLG:
      act &= ~PgLOG.EXITLG
      if PgLOG.PGLOG['DSCHECK']: opt |= 256

   PgLOG.PGLOG['ERR2STD'] = ["Warning: ", "already up-to-date", "process currently running",
                             "rsync", "No route to host", "''*'"]
   switch_logfile("gatherxml")
   dary = list(META[c])
   dcnt = len(dary)
   cnt = 0
   sx = ''
   rs = PgLOG.PGLOG['RSOptions']
   if not rs and dcnt == 1:
      if len(META[c][dary[0]]) > 2:
         sx = "{} -d {} -r{} ".format(CMD['SX'], dary[0], ('m' if cate == 'M' else 'w'))
         rs = " -S -R"
    
   for d in dary:
      for ary in META[c][d]:
         cmd = "{} -d {} -f {}{} ".format(CMD['GX'], d, ary[1], rs)
         if cate == 'M':
            if ary[2] and op.exists(ary[2]): cmd += "-l {} ".format(ary[2])
            if ary[3]: cmd += "-m {} ".format(ary[3])
         cmd += ary[0]
         if PgSIG.start_background(cmd, act, opt, 1):
            cnt += 1
            if PgLOG.PGLOG['DSCHECK'] and cnt > 0 and (cnt%20) == 0:
               PgCMD.add_dscheck_dcount(10, 0, logact)
         elif PgLOG.PGLOG['SYSERR']:
            PgDBI.record_dscheck_error(PgLOG.PGLOG['SYSERR'])
   PgLOG.PGLOG['ERR2STD'] = []

   if cnt > 0:
      if PgSIG.PGSIG['BPROC'] > 1: PgSIG.check_background(None, 0, logact, 1)
      if cnt > 1: PgLOG.pglog("Metadata gathered for {} files".format(cnt), PgLOG.WARNLG)
      if sx:
         if 'all' in TIDXS:
            PgLOG.pgsystem(sx + 'all', act, opt)
         else:
            for tidx in TIDXS:
               PgLOG.pgsystem("{}{}".format(sx, tidx), act, opt)

      if PgLOG.PGLOG['DSCHECK'] and (cnt%20) > 0:
         PgCMD.add_dscheck_dcount(int((cnt%20)/2), 0, logact)

   del META[c]
   TIDXS = {}
   switch_logfile()

   return cnt

#
# process cached metadata deleting actions cate: M or W
#
def process_meta_delete(cate, logact = PgLOG.LOGWRN):

   if cate == 'M': return 0
   c = 'D' + cate
   if c not in META: return 0
   cnt = 0
   opt = 5
   act = logact
   if act&PgLOG.EXITLG:
      act &= ~PgLOG.EXITLG
      if PgLOG.PGLOG['DSCHECK']: opt |= 256

   PgLOG.PGLOG['ERR2STD'] = ["Warning: "]
   switch_logfile("gatherxml")
   for d in META[c]:
      cmd = "{} -d {}".format(CMD['DX'], d)
      dcnt = 0
      for file in META[c][d]:
         cmd += " " + file
         dcnt += 1

      if dcnt > 0:
         if PgSIG.start_background(cmd, act, opt, 1):
            cnt += dcnt
         elif PgLOG.PGLOG['SYSERR']:
            PgDBI.record_dscheck_error(PgLOG.PGLOG['SYSERR'])
   PgLOG.PGLOG['ERR2STD'] = []

   if cnt > 0:
      if PgSIG.PGSIG['BPROC'] > 1: PgSIG.check_background(None, 0, logact, 1)
      if cnt > 1: PgLOG.pglog("Metadata deleted for {} files".format(cnt), PgLOG.WARNLG)

   del META[c]
   switch_logfile()
   return cnt

#
# delete metadata for given file
#
def delete_file_metadata(dsid, file, logact = PgLOG.LOGWRN):

   d = PgUtil.metadata_dataset_id(dsid)
   opt = 5
   if logact&PgLOG.EXITLG:
      logact &= ~PgLOG.EXITLG
      if PgLOG.PGLOG['DSCHECK']: opt |= 256

   PgLOG.PGLOG['ERR2STD'] = ["Warning: "]
   switch_logfile("gatherxml")
   if not PgLOG.pgsystem("{} -d {} {}".format(CMD['DX'], d, file), logact, opt) and PgLOG.PGLOG['SYSERR']:
      PgDBI.record_dscheck_error(PgLOG.PGLOG['SYSERR'])
   PgLOG.PGLOG['ERR2STD'] = []
   switch_logfile()

#
# process cached metadata moving actions cate: M or W
#
def process_meta_move(cate, logact = PgLOG.LOGWRN):

   if cate == 'M': return 0
   c = 'R' + cate
   if c not in META: return 0
   cnt = 0
   opt = 5
   act = logact
   if act&PgLOG.EXITLG:
      act &= ~PgLOG.EXITLG
      if PgLOG.PGLOG['DSCHECK']: opt |= 256

   PgLOG.PGLOG['ERR2STD'] = ["Warning: "]
   switch_logfile("gatherxml")
   for d in META[c]:
      dmeta = META[c][d]
      files = list(dmeta)
      fcnt = len(files)
      for i in range(fcnt):
         file = files[i]
         cmd = "{} -d {} ".format(CMD['RX'], d)
         n = dmeta[file][0]
         if n:
            cmd += "-nd {} ".format(n)
         elif i < fcnt - 1:
            cmd += "-C "

         cmd += "{} {}".format(file, dmeta[file][1])
         if PgSIG.start_background(cmd, act, opt, 1):
            cnt += 1
         elif PgLOG.PGLOG['SYSERR']:
            PgDBI.record_dscheck_error(PgLOG.PGLOG['SYSERR'])
   PgLOG.PGLOG['ERR2STD'] = []

   if cnt > 0:
      if PgSIG.PGSIG['BPROC'] > 1: PgSIG.check_background(None, 0, logact, 1)
      if cnt > 1: PgLOG.pglog("Metadata moved for {} files".format(cnt), PgLOG.WARNLG)

   del META[c]
   switch_logfile()

   return cnt

#
# process cached metadata of getting group summary actions cate: M or W
#
def process_meta_summary(cate, logact = PgLOG.LOGWRN):

   if cate == 'M': return 0
   c = 'S' + cate
   if c not in META: return 0
   cnt = 0
   opt = 5
   ctype = cate.lower()
   act = logact
   if act&PgLOG.EXITLG:
      act &= ~PgLOG.EXITLG
      if PgLOG.PGLOG['DSCHECK']: opt |= 256

   PgLOG.PGLOG['ERR2STD'] = ["Warning: "]
   switch_logfile("gatherxml")
   for d in META[c]:
      for gindex in META[c][d]:
         cmd = "{} -d {} -{} {}".format(CMD['SX'], d, ctype, gindex)
         if PgSIG.start_background(cmd, act, opt, 1):
            cnt += 1
         elif PgLOG.PGLOG['SYSERR']:
            PgDBI.record_dscheck_error(PgLOG.PGLOG['SYSERR'])
   PgLOG.PGLOG['ERR2STD'] = []

   if cnt > 0:
      if PgSIG.PGSIG['BPROC'] > 1: PgSIG.check_background(None, 0, logact, 1)
      if cnt > 1: PgLOG.pglog("Group Metadata summarized for {} groups".format(cnt), PgLOG.WARNLG)

   del META[c]
   switch_logfile()

   return cnt

#
# find a top group index if given group index is not otherwise return itself
# cache the results for later use
#
def get_top_gindex(dsid, gindex, logact = PgLOG.LGEREX):

   if gindex in TGIDXS: return TGIDXS[gindex]

   gcnd = "dsid = '{}' AND gindex = {}".format(dsid, gindex)
   pgrec = PgDBI.pgget("dsgroup", "pindex", gcnd, logact)
   if pgrec and pgrec['pindex']:
      tindex = get_top_gindex(dsid, pgrec['pindex'])
   else:
      tindex = gindex
   TGIDXS[gindex] = tindex

   return tindex

#
# cache file top group index for meta gathering 
#
def cache_meta_tindex(dsid, id, type, logact = PgLOG.LGEREX):
   
   pgrec = PgSplit.pgget_wfile(dsid, "tindex, gindex", "wid = {}".format(id), logact)

   if pgrec:
      if pgrec['tindex']:
         tidx = pgrec['tindex']
      else:
         tidx = get_top_gindex(dsid, pgrec['gindex'], logact)
   else:
      tidx = 0

   if tidx:
      TIDXS[tidx] = 1
   else:
      TIDXS['all'] = 1

#
# reset the top group index values for the current and sub groups of
# given dsid and/or group index 
# act == 2/4 reset for mss/web files, respectively 1 for both
#
def reset_top_gindex(dsid, gindex, act):

   tcnt = 0
   if act == 1: act = 12
   tindex = get_top_gindex(dsid, gindex)
   record = {'tindex' : tindex}
   tcnd = "gindex = {} AND tindex <> {}".format(gindex, tindex) 
   dcnd = "dsid = '{}'".format(dsid)
   if act&4:
      cnt = PgSplit.pgupdt_wfile(dsid, record, tcnd, PgLOG.LGEREX)
      if cnt > 0:
         s = ('s' if cnt > 1 else '')
         PgLOG.pglog("set tindex {} for {} web files in gindex {} of {}".format(tindex, cnt, gindex, dsid), PgLOG.WARNLG)
         tcnt += cnt
   if act&8:
      cnt = PgDBI.pgupdt("sfile", record, dcnd + ' AND ' + tcnd, PgLOG.LGEREX)
      if cnt > 0:
         s = ('s' if cnt > 1 else '')
         PgLOG.pglog("set tindex {} for {} saved files in gindex {} of {}".format(tindex, cnt, gindex, dsid), PgLOG.WARNLG)
         tcnt += cnt

   pcnd = "{} AND pindex = {}".format(dcnd, gindex)
   pgrecs = PgDBI.pgmget("dsgroup", "gindex", pcnd, PgLOG.LGEREX)
   cnt = len(pgrecs['gindex']) if pgrecs else 0

   for i in range(cnt):
      tcnt += reset_top_gindex(dsid, pgrecs['gindex'][i])

   return tcnt

#
# reset metalink in table wfile
#
def set_meta_link(dsid, fname):
   
   return PgLOG.pgsystem("{} -d {} {}".format(CMD['SL'], dsid, fname))
