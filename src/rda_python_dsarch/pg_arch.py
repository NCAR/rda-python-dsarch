###############################################################################
#     Title : pg_arch.py  -- package for holding dsarch gobal variables and functions
#    Author : Zaihua Ji,  zjiucar.edu
#      Date : 09/22/2020
#             2025-01-22 transferred to package rda_python_dsarch from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-12-09 conver to class PgArch
#   Purpose : python library module for holding some global variables and
#             functions for dsarch utility
#    Github : https://github.com/NCAR/rda-python-dsarch.git
# 
###############################################################################
import time
import re
import os
from os import path as op
from rda_python_common.pg_meta import PgSplit
from rda_python_common.pg_cmd import PgCMD
from rda_python_common.pg_opt import PgOPT

class PgArch(PgOPT, PgCMD, PgSplit):

   def __init__(self):
      super().__init__()  # initialize parent class
      self.RTPATH = {
         'dsid' : None,   # dsid for the current caching info
         'A' : '',         # ARCO reference file
         'D' : '',         # normal web online data
         'N' : '',         # online data for ncar users only
         'I' : '',         # internal online data
         'U' : '',         # unknown type of online data
         'O' : '',         # document path
         'S' : '',         # software path
         'HD' : '',         # dataset help document path
         'HS' : '',         # dataset help software path
         'SP' : '',         # dataset savedpath
      }
      self.ebpaths = {}
      self.savedpaths = {}
      self.grouptypes = {}
      self.CORDERS = {}
      self.CTYPE = ''
      self.OPTS.update({                         # (!= 0) - setting actions
         'AS' : [0x00000004, 'ArchiveSavedFile', 1],
         'AH' : [0x00000008, 'ArchiveHelpFile', 1],
         'AW' : [0x00000010, 'ArchiveWebFile', 1],
         'CG' : [0x00000040, 'ChangeGroup',    1],
         'TV' : [0x00000100, 'TerminateVersion', 1],
         'DG' : [0x00000200, 'DeleteGroup',    1],
         'DL' : [0x00000400, 'Delete',         1],
         'GS' : [0x00000800, 'GetSavedFile',   0],
         'GD' : [0x00001000, 'GetDataset',     0],
         'GG' : [0x00002000, 'GetGroup',       0],
         'GH' : [0x00004000, 'GetHelpFile',     0],
         'GW' : [0x00008000, 'GetWebFile',     0],
         'GV' : [0x00010000, 'GetVersion',     0],
         'MV' : [0x00040000, 'MoveFile',       1],
         'SS' : [0x00080000, 'SetSavedFile',   1],
         'SD' : [0x00100000, 'SetDataset',     4],
         'SG' : [0x00200000, 'SetGroup',       1],
         'SH' : [0x00400000, 'SetHelpFile',     1],
         'SW' : [0x00800000, 'SetWebFile',     1],
         'SV' : [0x01000000, 'SetVersion',     1],
         'UC' : [0x04000000, 'UpdateCache',    1],
         'UW' : [0x08000000, 'UpdateWeb',      1],
         'AQ' : [0x10000000, 'ArchiveQuasarFile', 1],
         'GQ' : [0x20000000, 'GetQuasarFile',   0],
         'SQ' : [0x40000000, 'SetQuasarFile',   1],
         'RQ' : [0x80000000, 'RestoreQuasarFile',  1],
         'GA' : [0x2000F800, 'GetALL',         0], # GD, GG, GH, GS, GW & GQ
         'SA' : [0x40F80000, 'SetALL',         2], # SD, SG, SH, SS, SW & SQ
         'BG' : [0, 'BackGround',    0],
         'CL' : [0, 'CleanLocal',    0],
         'FO' : [0, 'FormatOutput',  0],
         'DX' : [0, 'DeleteXML',     2],
         'EM' : [0, 'EmailNotice',   0],
         'GF' : [0, 'GrowingFile',   1],
         'GX' : [0, 'GatherXML',     3],
         'GZ' : [0, 'GMTZone',       0],
         'KM' : [0, 'KeepMetadata',  1],
         'KP' : [0, 'KeepPath',      1],
         'MD' : [0, 'MyDataset',     3],
         'NE' : [0, 'NoEmail',       0],
         'NT' : [0, 'NoTrim',        0],
         'NV' : [0, 'NewVersion',    1],
         'OE' : [0, 'OverrideExist', 1],
         'PE' : [0, 'ShowPeriod',    0],
         'RA' : [0, 'RetryArchive',  1],
         'RD' : [0, 'RemoveDir',     2],
         'RG' : [0, 'RecursiveGroup', 0],
         'RN' : [0, 'RelativeName',  1],
         'RO' : [0, 'ResetOrder',    2],
         'RS' : [0, 'GXRSOptions',   3],
         'RT' : [0, 'ResetTIndex',   3],
         'SC' : [0, 'SetChecksum',   2],
         'TO' : [0, 'TarOnly',       1],
         'TS' : [0, 'ToSaved',       2],
         'TT' : [0, 'TotalSummary',  0],
         'TW' : [0, 'ToWeb',         2],
         'UZ' : [0, 'UnzipData',     3],
         'WC' : [0, 'WithChecksum',  0],
         'WM' : [0, 'WithMetadata',  3],
         'WN' : [0, 'WithFileNumber',3],
         'XC' : [0, 'CrossCopy',     1],
         'XM' : [0, 'CrossMove',     1],
         'ZD' : [0, 'ZipData',       3],
         'AL' : [1, 'AsyncLimit',     17],
         'AO' : [1, 'ActionOption',    1],  # default to <!>
         'BL' : [1, 'ButtonLimit',    17],
         'DD' : [1, 'DeleteDir',       1],
         'DS' : [1, 'Dataset',         0],
         'DV' : [1, 'Divider',         1], # default to <:>
         'ES' : [1, 'EqualSign',       1], # default to <=>
         'FL' : [1, 'FileLimit',      16],
         'FN' : [1, 'FieldNames',      0],
         'GL' : [1, 'GroupLevel',     16],
         'LD' : [1, 'LocalDirectory',  0],
         'LL' : [1, 'LocalFileList',   0],
         'LN' : [1, 'LoginName',       1],
         'OD' : [1, 'OriginDataset',   0],
         'OF' : [1, 'OutputFile',      0],
         'ON' : [1, 'OrderNames',      0],
         'PO' : [1, 'PatternOffset',  16],
         'QS' : [1, 'QsubOptions',     1],
         'UD' : [1, 'UseDSARCH',       0,  'YNIPW'],  # Internal, Public, Web only, Y - ready for dsarch 
         'VS' : [1, 'ValidSize',      17],  # default to self.PGLOG['MINSIZE']
         'WI' : [1, 'WaitInternval',   1],
         'AF' : [2, 'ArchiveFormat', 1],
         'BD' : [2, 'BeginDate',   256],
         'BF' : [2, 'BackupFlag',    1, 'BDNP'],
         'BP' : [2, 'BatchProcess',  0, ''],
         'BS' : [2, 'BackStatus',    1, 'NTA'],
         'BT' : [2, 'BeginTime',    32],
         'DA' : [2, 'DataAccess',    1, 'BCFN'],
         'DB' : [2, 'Debug',         0],
         'DE' : [2, 'Description',  65],
         'DF' : [2, 'DataFormat',    1],
         'DN' : [2, 'DOINumber',     1],
         'DO' : [2, 'DisplayOrder', 16],
         'ED' : [2, 'EndDate',     256],
         'ET' : [2, 'EndTime',      32],
         'EV' : [2, 'ExternalVersion', 1],
         'FD' : [2, 'FileDate',    257],
         'FF' : [2, 'FileFlag',      1, 'FP'],
         'FS' : [2, 'FileStatus',    1, 'PI'],
         'FT' : [2, 'FileTime',     33],
         'GI' : [2, 'GroupIndex',   17],
         'GN' : [2, 'GroupName',     1],
         'GP' : [2, 'GroupPattern',  1],
         'GT' : [2, 'GroupType',     1, 'PI'],
         'HF' : [2, 'HelpFile',      0],
         'HT' : [2, 'HelpFileType',  129, self.PGOPT['HFTYP']],
         'ID' : [2, 'InitialDate', 257],
         'IF' : [2, 'InputFile',     0],
         'IV' : [2, 'InternalVersion', 0],
         'KV' : [2, 'KeyValue',      0],
         'LC' : [2, 'Location',      1, 'CGOBR'],
         'LF' : [2, 'LocalFile',     0],
         'MC' : [2, 'MD5Checksum',   0],
         'ML' : [2, 'MetaLink',      1],
         'ND' : [2, 'NoteDocument', 65],
         'NI' : [2, 'NoteInternal', 65],
         'NS' : [2, 'NoteSoftware', 65],
         'NW' : [2, 'NoteWeb',      65],
         'OB' : [2, 'OrderBy',       0],
         'OG' : [2, 'OriginGroup',   1],
         'OT' : [2, 'OriginType',    1],
         'PI' : [2, 'ParentIndex',  17],
         'QF' : [2, 'QuasarFile',    0],
         'QT' : [2, 'QuasarFileType',  129, 'BD'],
         'RF' : [2, 'OriginFile',    0],
         'SF' : [2, 'SavedFile',     0],
         'SP' : [2, 'SavedPath',     1],
         'SR' : [2, 'Source',        1],
         'ST' : [2, 'SavedFileType',   1, self.PGOPT['SDTYP']],
         'SZ' : [2, 'Size',          16],
         'TG' : [2, 'TopGroupIndex', 17],
         'TI' : [2, 'Title',          0],
         'TL' : [2, 'ThreddLink',     1],
         'VI' : [2, 'VersionIndex',  17],
         'VT' : [2, 'VersionStatus',  0, "PAH"],
         'WF' : [2, 'WebFile',        0],
         'WH' : [2, 'WebHome',        1],
         'WP' : [2, 'WebPath',        1],
         'WT' : [2, 'WebFileType',    1,  self.PGOPT['WDTYP']],
         'WU' : [2, 'WebURL',         1],
      })
      self.ALIAS.update({
         'AF' : ['FileFormat', 'ExternalFormat'],
         'AL' : ['AsynchronousLimit'],
         'AO' : ['ActOption'],
         'AH' : ['ArchiveHelp', 'ArchHelp', 'ArchHelpFile'],
         'AQ' : ['ArchiveQuasar', 'ArchiveBackupFile', 'ArchiveBackup'],
         'AS' : ['ArchiveSaved', 'ArchSaved', 'ArchSavedFile'],
         'AW' : ['ArchiveWeb', 'ArchWeb', 'ArchWebFile'],
         'BG' : ['b'],
         'BP' : ['d', 'DelayedMode'],
         'CL' : ['CleanLocFile', 'CleanLocalFile'],
         'DA' : ['DataAccessflag', 'FileListFlag'],
         'DD' : ['DeleteDirLevel', 'DeleteEmptyDir'],
         'DE' : ['Desc', 'Note', 'FileDesc', 'FileDescription'],
         'DF' : ['TF', 'ContentFormat'],
         'DL' : ['RM', 'Remove', 'DeleteFile', 'RemoveFile'],
         'DN' : ["DOI", "DOIName"],
         'DS' : ['Dsid', 'DatasetID'],
         'DV' : ['Delimiter', 'Separater'],
         'FL' : ['FileCountLimit'],
         'GF' : ['GrowingDataFile'],
         'GN' : ['GroupID'],
         'GM' : ['GetMSS'],
         'GQ' : ['GetQuasar', 'GetBackupFile', 'GetBackup'],
         'GS' : ['GetSaved'],
         'GT' : ['GroupDataType'],
         'GW' : ['GetWeb'],
         'GX' : ['Grid2XML'],
         'GZ' : ['GMT', 'GreenwichZone', 'UTC'],
         'KM' : ['KeepMeta'],
         'KP' : ['KeepLocalPath'],
         'HT' : ['HelpType'],
         'LC' : ['LocationFlag'],
         'LF' : ['LocFile', 'SourceFile'],
         'LL' : ['LocalList'],
         'ML' : ['MetadataLink'],
         'MX' : ['HtarMemberXML', 'MemberfileXML'],
         'ND' : ['DocumentNote', 'DescDocument'],
         'NI' : ['InternalNote', 'DescInternal', 'DI'],
         'NS' : ['SoftwareNote', 'DescSoftware'],
         'NW' : ['WebNote', 'DescWeb', 'DW'],
         'OB' : ['OrderByPattern'],
         'PI' : ['ParentGroupIndex'],
         'QS' : ['PBSOptions'],
         'QT' : ['QuasarType', 'BackupType'],
         'RD' : ['RemoveDirectory', 'RemoveEmptyDir'],
         'RG' : ['RepeatGroup'],
         'RN' : ['RelativePathName',  'RelativeFileName'],
         'RO' : ['Reorder'],
         'RQ' : ['RestoreQuasar', 'RestoreBackupFile', 'RestoreBackup'],
         'RT' : ['ResetTopGroup'],
         'SH' : ['SetHelp'],
         'SQ' : ['SetQuasar', 'SetBackupFile', 'SetBackup'],
         'SS' : ['SetSaved'],
         'ST' : ['SavedType', 'SavedArchiveType'],
         'SW' : ['SetWeb'],
         'TG' : ['TopGroup'],
         'TN' : ['TargetFileName'],
         'TS' : ['ToSavedFile', 'MovedToSaved'],
         'TW' : ['ToWebFile', 'MovedToWeb'],
         'UC' : ['UpdateCacheNumber'],
         'UD' : ['UseRDADB'],
         'HD' : ['UpdateHTML', 'GenerateHTML'],
         'UZ' : ['Uncompress', 'UncompressData', 'Unzip'],
         'WC' : ['ValidateChecksum', "WithMD5", "ValidateMD5"],
         'WH' : ['WebDataHome'],
         'WL' : ['WebLocal'],
         'WM' : ['WithMeta'],
         'WN' : ['WithNumber'],
         'WT' : ['WebType', 'WebArchiveType'],
         'WU' : ['URL', 'WebAddress'],
         'ZD' : ['Compress', 'CompressData', 'Zip']
      })
      # single letter short names for option 'FN' (Field Names) to retrieve info
      # from RDADB; only the fields can be manipulated by this application are listed
      #  SHORTNM KEYS(self.OPTS) DBFIELD
      self.TBLHASH['dataset'] = {         # condition flag, 0-int, 1-string, -1-exclude
         'T' : ['TI', "title",        1],
         'S' : ['SP', "savedpath",    1],
         'H' : ['WH', "webhome",      1],
         'W' : ['WP', "webpath",      1],
         'F' : ['DF', "data_format",  1],
         'U' : ['UD', "use_rdadb",    1],
         'L' : ['LC', "locflag",      1],
         'Q' : ['BF', "backflag",     1],
         'A' : ['DA', "accessflag",   1],
         'V' : ['GL', "grouplevel",   0],
         'C' : ['FL', "filelimit",    0],
         'B' : ['BL', "buttonlimit",  0],
         'M' : ['ML', "meta_link",   -1],
         'P' : ['NW', "wnote",       -1],
         'I' : ['NI', "inote",       -1],
         'D' : ['ND', "dnote",       -1],
         'N' : ['NS', "snote",       -1]
      }
      self.TBLHASH['dsperiod'] = {
         'G' : ['GI', "dsperiod.gindex", 0],
         'J' : ['BD', "date_start",      1],
         'K' : ['BT', "time_start",      1],
         'X' : ['ED', "date_end",        1],
         'Y' : ['ET', "time_end",        1]
      }
      self.TBLHASH['dsgroup'] = {
         'I' : ['GI', "gindex",    0],
         'G' : ['GN', "grpid",     1],
         'X' : ['PI', "pindex",    0],
         'T' : ['TI', "title",     1],
         'R' : ['GT', "grptype",   1],
         'Q' : ['BF', "backflag",  1],
         'A' : ['DA', "accessflag",   1],
         'P' : ['GP', "pattern",   1],
         'S' : ['SP', "savedpath", 1],
         'W' : ['WP', "webpath",   1],
         'M' : ['ML', "meta_link", 1],
         'D' : ['NW', "wnote",     1],
         'N' : ['NI', "inote",     1]
      }
      self.TBLHASH['wfile'] = {
         'F' : ['WF', "wfile",               1],
         'T' : ['WT', "wtype",               1, "wfile.type"],
         'C' : ['MC', "wfile.checksum",      1],
         'I' : ['GI', "gindex",              0],
         'X' : ['TG', "tindex",              0],
         'V' : ['VI', "vindex",              0],
         'M' : ['AF', "wfile.file_format",   1],
         'N' : ['DF', "wfile.data_format",   1],
         'O' : ['DO', "wfile.disp_order",   -1],
         'B' : ['QF', "wfile.bid",           0],
         'Q' : ['QT', "btype",               1, "bfile.type"],
         'L' : ['LC', "locflag",             1],
         'H' : ['FS', "wfile.status",        1],
         'P' : ['FF', "fileflag",            1],
         'S' : ['SZ', "wfile.data_size",     0],
         'J' : ['FD', "wfile.date_modified", 1],
         'K' : ['FT', "wfile.time_modified", 1],
         'A' : ['ML', "wfile.meta_link",     1],
         'E' : ['TL', "thredd_link",         1],
         'D' : ['DE', "wfile.note",          1]
      }
      self.TBLHASH['sfile'] = {
         'F' : ['SF', "sfile",               1],
         'T' : ['ST', "stype",               1, "sfile.type"],
         'C' : ['MC', "sfile.checksum",      1],
         'I' : ['GI', "gindex",              0],
         'X' : ['TG', "tindex",              0],
         'V' : ['VI', "vindex",              0],
         'M' : ['AF', "sfile.file_format",   1],
         'N' : ['DF', "sfile.data_format",   1],
         'O' : ['DO', "sfile.disp_order",   -1],
         'B' : ['QF', "sfile.bid",           0],
         'Q' : ['QT', "btype",               1, "bfile.type"],
         'L' : ['LC', "locflag",             1],
         'H' : ['FS', "sfile.status",        1],
         'P' : ['FF', "fileflag",            1],
         'S' : ['SZ', "sfile.data_size",     0],
         'J' : ['FD', "sfile.date_modified", 1],
         'K' : ['FT', "sfile.time_modified", 1],
         'D' : ['DE', "sfile.note",          1]
      }
      self.TBLHASH['bfile'] = {
         'F' : ['QF', "bfile",          1],
         'C' : ['MC', "checksum",       1],
         'M' : ['AF', "file_format",    1],
         'N' : ['DF', "data_format",    1],
         'O' : ['DO', "disp_order",    -1],
         'T' : ['QT', "type",           1],
         'H' : ['BS', "status",         1],
         'S' : ['SZ', "data_size",      0],
         'J' : ['FD', "date_modified",  1],
         'K' : ['FT', "time_modified",  1],
         'D' : ['DE', "note",           1]
      }
      self.TBLHASH['hfile'] = {
         'F' : ['HF', "hfile",           1],
         'T' : ['HT', "type",            1],
         'L' : ['LC', "locflag",         1],
         'C' : ['MC', "checksum",        1],
         'M' : ['AF', "file_format",     1],
         'N' : ['DF', "data_format",     1],
         'O' : ['DO', "disp_order",     -1],
         'H' : ['FS', "status",          1],
         'P' : ['FF', "fileflag",        1],
         'S' : ['SZ', "data_size",       0],
         'R' : ['SR', "source",          1],
         'I' : ['ID', "init_date",       1],
         'J' : ['FD', "date_modified",   1],
         'K' : ['FT', "time_modified",   1],
         'U' : ['WU', "url",             1],
         'D' : ['DE', "note",            1]
      }
      self.TBLHASH['dsvrsn'] = {
         'V' : ['VI', "vindex",       0],
         'I' : ['IV', "iversion",     0],
         'E' : ['EV', "eversion",     1],
         'D' : ['DN', "doi",          1],
         'S' : ['VT', "status",       1],
         'J' : ['BD', "start_date",   1],
         'K' : ['BT', "start_time",  -1],
         'X' : ['ED', "end_date",     1],
         'Y' : ['ET', "end_time",    -1],
         'N' : ['DE', "note",        -1],
      }
      # global info to be used by the whole application
      self.PGOPT['MSET']  = "SA"
      #default fields for getting info
      self.PGOPT['dataset'] = "SWFULQA"
      self.PGOPT['dsperiod'] = "GJKXY"
      self.PGOPT['dsgroup'] = "IGXTPQASW"
      self.PGOPT['wfile'] = "FTIVMNLHPS"
      self.PGOPT['sfile'] = "FTIVMNLHPS"
      self.PGOPT['bfile'] = "FNMTHS"
      self.PGOPT['hfile'] = 'FMNLTHPSU'
      self.PGOPT['dsvrsn'] = "VIEDSJX"
      self.PGOPT['UACTS'] = (self.OPTS['AQ'][0]|self.OPTS['SD'][0]|self.OPTS['DL'][0])
      
      #all fields for getting info
      self.PGOPT['dsall'] = "TSHWFULQAVCBMPIDNGJKXYIJKXY"   # include 'pdall'
      self.PGOPT['pdall'] = "GJKXY"
      self.PGOPT['gpall'] = "IGXTRQAPSWMDN"
      self.PGOPT['wfall'] = "FTCIXVMNOBQLHPSJKAED"
      self.PGOPT['sfall'] = "FTCIXVMNOBQLHPSJKD"
      self.PGOPT['bfall'] = "FNMOTHSCJKD"
      self.PGOPT['hfall'] = 'FTLCMNOHPSRIJKUD'
      self.PGOPT['vrsnall'] = "VIEDSJKXYN"
      
      # self.PGOPT['ofile'] reserved for original type 'B' hpps file name
      self.params['AL'] = 0   # max number of asynchronous background processes
      self.params['WI'] = 30   # wait interval continue update record in queue, in seconds

   # get condition
   def get_condition(self, tname, include = None, exclude = None, dojoin = 0):
      if tname not in self.TBLHASH: return ""   # should not happen
      hash = self.TBLHASH[tname]
      lname = (tname + '.') if dojoin or tname == 'dsperiod' else ''
      condition = "{}dsid = '{}'".format(lname, self.params['DS'])
      noand = 0
      if tname == 'wfile':
         types = self.params['WT'] if 'WT' in self.params else None
         condition = ''
         noand = 1
      for key in hash:
         flg = hash[key][2]
         if flg < 0: continue
         if include and include.find(key) < 0: continue
         if exclude and exclude.find(key) > -1: continue
         opt = hash[key][0]
         fld = hash[key][3] if len(hash[key]) == 4 else hash[key][1]
         if opt in self.params:
            if opt == 'QF' and self.PGOPT['CACT'] != 'GQ':
               values = self.get_bid_numbers(self.params[opt])
            else:
               values = self.params[opt]
            condition += self.get_field_condition(fld, values, flg, noand)
            noand = 0
      return condition
   
   # check if enough information entered on command line and/or input file
   # for given action(s)
   def check_enough_options(self, cact, acts):
      errmsg = [
         "Miss original group index per -OG(-OriginGroup)",
         "miss file names per -SF(-SavedFile), -WF(-WebFile) or -QF(-QuasarFile)",
         "miss Quasar file names per -QF(-QuasarFile)",
         "Miss group index per -GI(-GroupIndex)",
         "Miss dataset number per -DS(-Dataset)",
         "Miss local file names per -LF(-LocalFile)",
         "miss Help file names per -HF(-HelpFile)",
         "Miss original dataset number per -OD(-OriginDataset)",
         "Miss file names per -SF(-SavedFile), -WF(-WebFile), -HF(-HelpFile) Or -QF(-QuasarFile)",
         "Miss Help file types per -HT(-HelpFileType)",
         "Miss Quasar file types per -QT(-QuasarFileType)",
         "Miss GDEX Server file names per -WF(-WebFile)",
         "Miss Saved file types per -ST(-SavedFileType)",
         "Cannot Cross Copy/Move (-XC/-XM) Saved Files",
         "14",
         "Miss Use-RDADB flag, ('Y', 'P', 'W', 'I'), per -UD(-UseRDADB) for ",
         "16",
         "miss Saved file names per -SF(-SavedFile)",
         "Miss Version Control Index per -VI(-VersionIndex)",
         "Cannot specify field names per -FN(-FieldNames) for Action -GA(-GetAll)",
         "20",
         "Miss orignal File Names via -RF (-OriginFile) for Action -MV (-MoveFile)",
         "22",
         '23',
         "Miss Data Format per -DF(-DataFormat)",
         "25",
         "Numbers of orignal/new groups per -OG(-OriginGroup)/-GI(-GroupIndex) do not match",
         "Miss Compression Format, Z|GZ|ZIP|BZ2, per -AF(-ArchiveFormat)",
         "Cannot Set Compression Format, Z|GZ|ZIP|BZ2, per -AF(-ArchiveFormat)",
         "Orignal File Names provided via -RF (-OriginFile) for Action other than -MV (-MoveFile)",
         "Miss order field name string per option -ON (-OrderNames) for Re-ordering",
         '31'
      ]
      erridx = -1
      if 'SC' in self.params and 'MC' in self.params: del self.params['SC']
      if ('DS' in self.params and ('GI' in self.params or 'GN' in self.params) and
          (acts&(self.OPTS['SG'][0]|self.OPTS['CG'][0])) == 0): self.validate_groups()
      if 'LL' in self.params:
         self.set_localfile_list(cact)
      elif 'LD' in self.params:
         self.build_localfile_list(cact, self.params['LD'])
      if acts == self.OPTS['AH'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif 'RF' in self.params:
            erridx = 29
         elif 'XC' in self.params or 'XM' in self.params:
            if 'HF' not in self.params:
               erridx = 6
         elif 'LF' not in self.params:
            erridx = 5
         elif 'HT' not in self.params:
            erridx = 9
         elif 'DF' not in self.params:
            erridx = 24
      elif acts == self.OPTS['AQ'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif 'QF' not in self.params:
            erridx = 2
         elif 'RF' in self.params:
            erridx = 29
         elif 'XC' not in self.params:
            if 'QT' not in self.params:
               erridx = 10
            elif 'QF' not in self.params:
               erridx = 2
      elif acts == self.OPTS['AS'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif 'RF' in self.params:
            erridx = 29
         elif 'ST' not in self.params:
            erridx = 12
         elif 'XC' in self.params or 'XM' in self.params:
            erridx = 13
   #         if 'SF' not in self.params:
   #            erridx = 17
         elif 'LF' not in self.params:
            erridx = 5
         elif 'DF' not in self.params:
            erridx = self.default_data_format(24)
      elif acts == self.OPTS['AW'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif 'RF' in self.params:
            erridx = 29
         elif 'XC' in self.params or 'XM' in self.params:
            if 'WF' not in self.params:
               erridx = 11
         elif 'LF' not in self.params:
            erridx = 5
         elif 'DF' not in self.params:
            erridx = self.default_data_format(24)
      elif acts == self.OPTS['CG'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif 'OG' not in self.params:
            erridx = 0
         elif 'GI' not in self.params:
            erridx = 3
         elif len(self.params['OG']) != len(self.params['GI']):
            erridx = 26
      elif acts == self.OPTS['DG'][0]:
         if 'GI' not in self.params:
            erridx = 3
         elif 'DS' not in self.params:
            erridx = 4
      elif acts == self.OPTS['DL'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif not ('HF' in self.params or 'SF' in self.params or 'QF' in self.params or 'WF' in self.params):
            if 'GI' in self.params and ('ST' in self.params or 'WT' in self.params):
               self.gather_delete_files()
            else:
               erridx = 8
      elif acts&self.OPTS['GA'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif (acts == self.OPTS['GA'][0] and 'FN' in self.params and
               not re.match(r'^all$', self.params['FN'], re.I)):
            erridx = 19
      elif acts == self.OPTS['GV'][0]:
         if 'DS' not in self.params: erridx = 4
      elif acts == self.OPTS['MV'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif not ('HF' in self.params or 'SF' in self.params or 'QF' in self.params or
                   'WF' in self.params or 'RF' in self.params):
            erridx = 8
         elif not ('RF' in self.params or 'OD' in self.params or 'OT' in self.params):
            if not ('TS' in self.params or 'TW' in self.params): erridx = 21
      elif acts == self.OPTS['RQ'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif not ('SF' in self.params or 'WF' in self.params or 'QF' in self.params):
            erridx = 1
         elif 'SF' in self.params and 'ST' not in self.params:
            erridx = 12
      elif acts == self.OPTS['SA'][0]:
         if 'DS' not in self.params:
            erridx = 4
      elif acts == self.OPTS['SD'][0]:
         if 'DS' not in self.params: erridx = 4
      elif acts == self.OPTS['SG'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif 'GI' not in self.params:
            erridx = 3
      elif acts == self.OPTS['SH'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif 'HT' not in self.params:
            erridx = 9
         elif not ('HF' in self.params or 'ON' in self.params):
            erridx = 30 if 'RO' in self.params else 6
         elif 'RF' in self.params:
            erridx = 29
      elif acts == self.OPTS['SQ'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif not ('QF' in self.params or 'ON' in self.params or 'WN' in self.params):
            erridx = 30 if 'RO' in self.params else 2
         elif 'RF' in self.params:
            erridx = 29
      elif acts == self.OPTS['SS'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif not ('SF' in self.params or 'ON' in self.params or 'RD' in self.params or 'WN' in self.params or 'WM' in self.params or 'RT' in self.params):
            erridx = 30 if 'RO' in self.params else 17
         elif 'RF' in self.params:
            erridx = 29
      elif acts == self.OPTS['SV'][0]:
         if 'DS' not in self.params:
            erridx = 4
         else:
            self.validate_versions()
            if 'VI' not in self.params: erridx = 18
      elif acts == self.OPTS['SW'][0]:
         if 'DS' not in self.params:
            erridx = 4
         elif not ('WF' in self.params or 'ON' in self.params or 'RD' in self.params or 'WN' in self.params or 'WM' in self.params or 'RT' in self.params):
            erridx = 30 if 'RO' in self.params else 11
         elif 'RF' in self.params:
            erridx = 29
      elif acts == self.OPTS['TV'][0]:
         if 'VI' not in self.params:
            erridx = 18
         elif 'DS' not in self.params:
            erridx = 4
         self.validate_versions()
      elif acts == self.OPTS['UC'][0] or acts == self.OPTS['UW'][0]:
         if 'DS' not in self.params: erridx = 4
      if erridx < 0:
         if(self.OPTS[cact][2] > 0 and acts&self.PGOPT['UACTS'] == 0 and
            self.use_rdadb(self.params['DS'], self.PGOPT['extlog']) == 'N' and
            ('UD' not in self.params or self.params['UD'] == 'N')):
            erridx = 15
            errmsg[erridx] += self.params['DS']
      if erridx >= 0: self.action_error(errmsg[erridx], cact)
      self.set_uid("dsarch")   # set uid before any action
      if 'VS' in self.params: self.PGLOG['MINSIZE'] = self.params['VS']   # minimal size for a file to be valid for archive
      self.PGLOG['RSOptions'] = " -R -S" if 'RS' in self.params else ''
      if 'NE' in self.params:
         self.PGLOG['LOGMASK'] &= ~self.EMLALL   # turn off all email acts
         if 'EM' in self.params: del self.params['EM']
      if 'BP' in self.params:
         if 'AL' in self.params: self.params['AL'] = 0
         if 'EM' not in self.params: self.PGLOG['LOGMASK'] &= ~self.EMLALL   # turn off email if not yet
         self.PGOPT['emerol'] |= self.EXITLG
         # set command line Batch options
         self.set_qsub_walltime()
         self.set_batch_options(self.params, 1)
         self.init_dscheck(0, '', "dsarch", self.params['DS'], cact, self.PGLOG['CURDIR'], self.params['LN'],
                            self.params['BP'], self.PGOPT['emerol'])
      if 'EM' in self.params: self.PGOPT['emlerr'] = self.LOGERR|self.EMEROL
      self.start_none_daemon('dsarch', self.PGOPT['CACT'], self.params['LN'], 1, self.params['WI'], 1, self.params['AL'])
   
   # set qsub walltime if needed
   def set_qsub_walltime(self):
      fcnt = 0
      if 'LF' in self.params:
         fcnt = len(self.params['LF'])
      elif 'WF' in self.params:
         fcnt = len(self.params['WF'])
      else:
         return
      if 'GX' in self.params:
         fcnt *= 300
      elif 'DX' in self.params:
         fcnt *= 10
      hr = int(fcnt/30000) + 1
      if hr == 6: return
      if hr < 2:
         hr = 2
      elif hr > 24:
         hr = 24
      self.set_one_boption('qoptions', '-l walltime={}:00:00'.format(hr), 1)
      return
   
   # build local file list for given local directory and reset file sizes
   def build_localfile_list(self, cact, dir):
      chkopt = 32 if 'SC' in self.params else 0
      if 'LF' in self.params:
         self.action_error("Both Value Options -LF (-LocalFile) and -LD (-LocalDirecotry) present", cact)
      locfiles = self.recursive_files((dir))
      i = 0
      for locfile in locfiles:
         locinfo = self.check_local_file(locfile, chkopt, self.PGOPT['extlog'])
         if locinfo:
            self.params['LF'][i] = locfile
            if chkopt: self.params['MC'][i] = locinfo['checksum']
            self.params['SZ'][i] = locinfo['data_size']
            i += 1
         else:
            self.action_error("locfile: local file not exists", cact)
      if chkopt: del self.params['SC']
      if i == 0: self.action_error("NO local file found in Local Directory " + dir, cact)
   
   # set local files for given local filelist and reset file sizes
   def set_localfile_list(self, cact):
      absolute = 0
      listfile = self.params['LL']
      chkopt = 32 if 'SC' in self.params else 0
      if 'LF' in self.params:
         self.action_error("Both Value Options -LF (-LocalFile)  and -LL (-LocalFileList) present", cact)
      i = 0
      hi = open(listfile, 'r')
      locfile = hi.realine()
      while locfile:
         locfile = locfile.strip()
         locinfo = self.check_local_file(locfile, chkopt, self.PGOPT['extlog'])
         if locinfo:
            self.params['LF'][i] = locfile
            if chkopt: self.params['MC'][i] = locinfo['checksum']
            self.params['SZ'][i] = locinfo['data_size']
            i += 1
            if not absolute and re.match(r'^/', locfile): absolute = 1
         else:
            self.action_error(locfile + ": local file not exists", cact)
      hi.close()
      if i == 0: self.action_error("NO local file found in " + listfile, cact)
      if chkopt: del self.params['SC']
      if absolute: del self.params['LL']
   
   # compress local file list and reset file sizes
   def compress_localfile_list(self, cact, cnt):
      chkopt = 32 if 'SC' in self.params else 0
      locfiles = self.compress_files(self.params['LF'], self.params['AF'], cnt)
      for i in range(cnt):
         locfile = locfiles[i]
         if locfile == self.params['LF'][i]: continue
         locinfo = self.check_local_file(locfile, chkopt, self.PGOPT['extlog'])
         if locinfo:
            self.params['LF'][i] = locfile
            self.params['SZ'][i] = locinfo['data_size']
            if chkopt: self.params['MC'][i] = locinfo['checksum']
         else:
            self.action_error(locfile + ": local file not exists", cact)
      if chkopt: del self.params['SC']
   
   # get continue display order of an archived data file of given dataset (and group/mssid)
   # 
   def get_next_disp_order(self, dsid = None, index = 0, table = None, type = None):
      if not dsid:
         self.CORDERS = {}   # reinitialize cached display orders
         self.CTYPE = ''
         return
      if index not in self.CORDERS or (type and type != self.CTYPE):
         self.CORDERS[index] = 0
         if type: self.CTYPE = type
         if table:
            cnd = "dsid = '{}'".format(dsid)
            if table == 'wfile':
               cnd = "gindex = {}".format(index)
            elif table == 'sfile':
               cnd += " AND gindex = {}".format(index)
            if type: cnd += " AND type = '{}'".format(type)
            if table == 'wfile':
               pgrec = self.pgget_wfile(dsid, "max(disp_order) max_order", cnd, self.PGOPT['extlog'])
            else:
               pgrec = self.pgget(table, "max(disp_order) max_order", cnd, self.PGOPT['extlog'])
            if pgrec and pgrec['max_order']: self.CORDERS[index] = pgrec['max_order']
      self.CORDERS[index] += 1
      return self.CORDERS[index]
   
   # identify and return matching data Ids from file name provided
   def get_dsids(self, file, table, dsids):
      fields = "dsid, " + table
      if file.find('%') > -1:
         fcnd = "{} LIKE '{}'"
      else:
         fcnd = "{} = '{}'"
      pgrecs = self.pgmget(table, fields, fcnd.format(table, file), self.PGOPT['extlog'])
      if not pgrecs and not re.match(r'^/', file):
         pgrecs = self.pgmget(table, fields, "{} LIKE '%/{}'".format(table, file), self.PGOPT['extlog'])
      if pgrecs:
         count = len(pgrecs['dsid'])
         while count > 0:
            count -= 1
            dsid = pgrecs['dsid'][count]
            if dsid not in dsids:
               dsids[dsid] = [pgrecs[table][count]]
            else:
               dsids[dsid].append(pgrecs[table][count])
   
   # get one dataset number; failure if not or multiple
   def get_dsid(self, files, table):
      dsids = {}
      for file in files:
         self.get_dsids(file, table, dsids)
      count = len(dsids)
      if count == 1:
         for dsid in dsids: return dsid  
      if count == 0:
         self.pglog("No Dataset identified for given file information", self.PGOPT['extlog'])
      else:
         errmsg = "{} Datasets identified for given file information:".format(count)
         for dsid in sorted(dsids):
            pgrec = self.pgget('dataset', 'title', "dsid = '{}'".format(dsid), self.PGOPT['extlog'])
            errmsg += "\n{} - {}".format(dsid, pgrec['title'])
            cnt = len(dsids[dsid])
            for j in range(cnt):
               errmsg += "\n  {}. {}".format(j+1, dsids[dsid][j])
         self.pglog(errmsg + "\nProvide one Dataset via Info option -DS(-Dataset) to run 'dsarch'!", self.PGOPT['extlog'])
      return None
   
   # reorder the files for group/dataset
   def reorder_files(self, dsid, onames, table):
      chkgrp = 1
      gcnd = cnd = "dsid = '{}'".format(dsid)
      if table == "wfile":
         type = "Web"
         idfld = "wid"
         gcnd = "webcnt > 0"
         cnd = "status <> 'D'"
      elif table == "sfile":
         type = "Saved"
         idfld = "sid"
         gcnd += " AND savedcnt > 0"
      elif table == "bfile":
         type = "Backup"
         idfld = "bid"
         chkgrp = 0
      elif table == "hfile":
         type = "Help"
         idfld = "hid"
         chkgrp = 0
      self.pglog("Reorder {} files for {} ...".format(type, dsid), self.PGOPT['wrnlog'])
      fields = "{}, disp_order".format(idfld)
      ocnd = ''
      if onames:
         flds = self.append_order_fields(onames, 'IO', table)
         fields += ', ' + self.get_string_fields(flds, table)
         if 'OB' not in self.params:
            ocnd = self.get_order_string(onames, table, "I")
      elif 'OB' not in self.params:
         ocnd = " ORDER BY " + idfld
      groups = [0]
      if chkgrp:
         if 'GI' in self.params:
            groups = self.params['GI']
         else:
            pgrecs = self.pgmget("dsgroup", "gindex", gcnd, self.PGOPT['extlog'])
            if pgrecs: groups.extend(pgrecs['gindex'])
         cnd += " AND gindex = "
      grpcnt = len(groups)
      ccnt = changed =  0
      for i in range(grpcnt):
         gindex = groups[i]
         if chkgrp:
            if i and gindex == groups[i-1]: continue
            fcnd = "{}{}{}".format(cnd, gindex, ocnd)
         else:
            fcnd = "{}{}".format(cnd, ocnd)
         if table == 'wfile':
            pgrecs = self.pgmget_wfile(dsid, fields, fcnd, self.PGOPT['extlog'])
         else:
            pgrecs = self.pgmget(table, fields, fcnd, self.PGOPT['extlog'])
         cnt = len(pgrecs[idfld]) if pgrecs else 0
         if cnt < 2: continue
         if 'OB' in self.params or re.search(r'P', onames, re.I):
            pgrecs = self.sorthash(pgrecs, flds, self.TBLHASH[table], self.params['OB'])
         gchng = 0
         for j in range(cnt):
            if not (pgrecs['disp_order'][j] and (j+1) == pgrecs['disp_order'][j]):
               record = {'disp_order' : j+1}
               if table == 'wfile':
                  gchng += self.pgupdt_wfile(dsid, record, "{} = {}".format(idfld, pgrecs[idfld][j]), self.PGOPT['extlog'])
               else:
                  gchng += self.pgupdt(table, record, "{} = {}".format(idfld, pgrecs[idfld][j]), self.PGOPT['extlog'])
         if gchng > 0:
            s = 's' if gchng > 1 else ''
            gmsg = "-G{}".format(gindex) if gindex else ''
            self.pglog("{} {} file{} reordered for {}{}!".format(gchng, type, s, dsid, gmsg), self.PGOPT['wrnlog'])
            changed += gchng
            ccnt += 1
      if ccnt > 1: 
         self.pglog("{} {} files reordered for {}!".format(changed, type, dsid), self.PGOPT['wrnlog'])
      elif ccnt == 0: 
         self.pglog("No {} file needs reorder for {}!".format(type, dsid), self.PGOPT['wrnlog'])
      return changed
   
   # set default data format and return -1
   def default_data_format(self, erridx):
      pgrec = self.pgget("dataset", "data_format", "dsid = '{}'".format(self.params['DS']), self.PGOPT['extlog'])
      if pgrec['data_format']:
         self.params['DF'] = [pgrec['data_format']]
         erridx =  -1
         self.OPTS['DF'][2] |= 2   # set auto-generated flag
      return erridx
   
   # dump out total file size and count
   def print_statistics(self, sizes):
      count = len(sizes)
      total = 0
      for i in range(count):
         total += sizes[i]
      self.OUTPUT.write("#File  Count: {}\n".format(count))
      self.OUTPUT.write("#Total Bytes: {}\n".format(total))
   
   # get a group type for given index
   def get_group_type(self, dsid, gindex):
      if not gindex: return 'P'   # default to P
      if gindex not in self.grouptypes:   # try to cache it
         pgrec = self.pgget("dsgroup", "grptype", "dsid = '{}' AND gindex = {}".format(self.params['DS'], gindex), self.PGOPT['extlog'])
         self.grouptypes[gindex] = 'I' if pgrec and pgrec['grptype'] == 'I' else 'P'
      return self.grouptypes[gindex]
   
   # intialize the group info of patterns, self.webpaths and self.savedpaths for given dataset ID
   # and set group IDs if missed
   def cache_group_info(self, count, noauto = 0):
      pgrec = self.pgget("dataset", "webpath, savedpath", "dsid = '{}'".format(self.params['DS']), self.PGOPT['extlog'])
      if not pgrec: self.pglog("Cannot get dataset info for '{}'".format(self.params['DS']), self.PGOPT['extlog'])
      self.webpaths = {0 : pgrec['webpath']}
      self.savedpaths = {0 : pgrec['savedpath']}
      self.grouptypes = {}
      if not count or 'GI' in self.params: return
      self.params['GI'] = [0]*count   # initialize group index to 0
      if 'PO' in self.params:
         mcnt = count
      else:
         if 'WF' in self.params:
            files = self.params['WF']
            fname = "wfile"
            mcnt = count = len(files)
         elif 'SF' in self.params:
            files = self.params['SF']
            fname = "sfile"
            mcnt = count = len(files)
         else:
            fname = None
            mcnt = count
         if fname:
            gidx = 0
            for i in range(count):
               if fname == 'wfile':
                  pgrec = self.pgget_wfile(self.params['DS'], "gindex", "{} = '{}'".format(fname, files[i]), self.PGOPT['extlog'])
               else:
                  pgrec = self.pgget(fname, "gindex", "dsid = '{}' AND {} = '{}'".format(self.params['DS'], fname, files[i]), self.PGOPT['extlog'])
               if pgrec and pgrec['gindex']:
                  if(pgrec['gindex'] != gidx and
                     not self.pgget("dsgroup", "", "dsid = '{}' and gindex = {}".format(self.params['DS'], pgrec['gindex']), self.PGOPT['extlog'])):
                     self.pglog("Group Index {} found for {} of {} is not in RDADB".format(pgrec['gindex'], fname, self.params['DS']), self.PGOPT['extlog'])
                  self.params['GI'][i] = gidx = pgrec['gindex']
                  mcnt -= 1
      if mcnt == count: self.OPTS['GI'][2] |= 2   # set auto-generated flag
      if noauto or mcnt == 0: return   # stop if no auto matching
      pgrecs = self.pgmget("dsgroup", "gindex, pattern", "dsid = '{}' AND pattern > ' ' ORDER BY pattern".format(self.params['DS']), self.PGOPT['extlog'])
      pcnt = len(pgrecs['gindex']) if pgrecs else 0
      if not pcnt:
         if 'PO' in self.params:
            self.pglog("Missing Group Patterns for set groups of " + self.params['DS'], self.PGOPT['emlerr'])
         return
      if 'WF' in self.params:
         files = self.params['WF']
      elif 'SF' in self.params:
         files = self.params['SF']
      elif 'LF' in self.params:
         files = self.params['LF']
      else:
         return
      offset = (self.params['PO'] if 'PO' in self.params else -1)
      count = len(files)
      tmpfiles = [None]*count
      for i in range(count):
         if self.params['GI'][i]: continue
         tmpfiles[i] = op.basename(files[i])
         if offset > 0: tmpfiles[i] = tmpfiles[i][offset:]
      files = tmpfiles
      s = 's' if mcnt > 1 else ''
      if pcnt > 5:
         self.pglog("Initialize group index for {} file{} via ".format(mcnt, s) +
                     "{} pattern match ...".format("Binary" if offset > -1 else "Simple"), self.PGOPT['wrnlog'])
      k = int(pcnt/2)   # initial index for pattern search
      mcnt = 0
      for i in range(count):
         if not files[i]: continue   # skip group index found already
         j = -1
         bname = files[i]
         # search adjecent three pattern first
         if re.match(r'^{}'.format(pgrecs['pattern'][k]), bname):
            j = k
         elif k < (pcnt-1) and re.search(pgrecs['pattern'][k+1], bname):
            j = k+1
         elif k > 0 and re.search(pgrecs['pattern'][k-1], bname):
            j = k-1
         elif offset < 0:   # simple searh
            j = k+2
            while j < pcnt:   # search up
               if re.search(pgrecs['pattern'][j], bname): break
               j += 1
            if j >= pcnt:
               j = k-2
               while j >= 0:   # search down
                  if re.search(pgrecs['pattern'][j], bname): break
                  j -= 1
         else:  # binary search
            if k < (pcnt - 2): # search top section
               j = self.psearch(k+2, pcnt, bname, pgrecs['pattern'])
            if j < 0 and k > 1: # search bottom section
               j = self.psearch(0, k-1, bname, pgrecs['pattern'])
         if j > -1:  # matched
            k = j
            self.params['GI'][i] = pgrecs['gindex'][k]
            mcnt += 1
      s = 's' if count > 1 else ''
      self.pglog("{} of {} file{} found matching group index of {}".format( mcnt, count, s, self.params['DS']), self.PGOPT['emlerr'])
      if mcnt > 0:
         self.OPTS['GI'][2] &= ~(2)   # remove auto-generated flag
         self.OPTS['GI'][2] |= 16   # set writable auto-generated flag
   
   # change a wfile permission on disk according to the file status
   def change_wfile_mode(self, dsid, wfile, type, ostat, nstat):
      if ostat == nstat: return
      if not self.RTPATH['dsid']: self.cache_rtpath(dsid)
      nm = 0o600 if nstat == 'I' else self.PGLOG['FILEMODE']
      self.set_local_mode(self.join_paths(self.RTPATH[type], wfile), nmode = nm)
   
   # get group WEB file path for given file index and/or file name
   # opt = 0 - relative path to rtpath
   #       1 - absolute path
   #       2 - relative path to rtpath/webpath
   #       3 - full url path as self.PGLOG['DSSURL']...
   #       4 - bit for ignore web path
   def get_web_path(self, i, fname, opt = 0, type = None, init = 0):
      addwp = 1
      rtpath = None
      if init or not self.RTPATH['dsid']: self.cache_rtpath(self.params['DS'], init)
      if not type: type = self.params['WT'][i] if 'WT' in self.params and self.params['WT'][i] else 'D'
      rtpath = self.RTPATH[type]
      if opt > 3:
         addwp = 0
         opt -= 4
      if fname and re.match(r'^/', fname):
         if opt == 3:
            return self.get_url_path(fname)
         elif opt == 1 or not re.match(r'^{}/'.format(rtpath), fname):
            return fname
         fname = re.sub(r'^{}/'.format(rtpath), '', fname,1)   # remove rtpath if exists         
     # find datset/group/user-defined subpath
      if 'WP' not in self.params:
         gindex = self.params['GI'][i] if 'GI' in self.params and self.params['GI'][i] else 0
         if gindex in self.webpaths:
            webpath = self.webpaths[gindex]
         else:
            webpath = self.get_group_field_path(gindex, self.params['DS'], 'webpath')
            self.webpaths[gindex] = webpath if webpath else ""
      elif len(self.params['WP']) == 1:
         webpath = self.params['WP'][0]
      else:
         webpath = self.params['WP'][i]
      if webpath and re.match(r'^{}/'.format(rtpath), webpath) and opt != 1:
         webpath = re.sub(r'^{}/'.format(rtpath), '', webpath, 1)   # remove rtpath if exists
      if fname:
         if opt == 2:
            fname = self.join_paths(webpath, fname, 1)   # remove webpath if exists 
         else:
            if addwp: fname = self.join_paths(webpath, fname)
            if opt != 0: fname = self.join_paths(rtpath, fname)
      elif opt == 0:
         if webpath: fname = webpath
      elif opt != 2:
         if addwp: fname = self.join_paths(rtpath, webpath)
      if opt == 3: fname = self.get_url_path(fname)
      return fname
   
   # get help file path for given file name
   # opt = 0 - relative path to rtpath
   #       1 - absolute path
   #       2 - full url path
   def get_help_path(self, i, fname, opt = 0, type = None, url = None, init = 0):
      rtpath = None
      if init or not self.RTPATH['dsid']: self.cache_rtpath(self.params['DS'], init)
      if not type: type = self.params['HT'][i] if 'HT' in self.params and self.params['HT'][i] else 'D'
      rtpath = self.RTPATH['H' + type]
      if fname and re.match(r'^/', fname):
         if opt == 2: return self.get_url_path(fname, url)
         if opt == 1 or not re.match(r'^{}/'.format(rtpath), fname): return fname
         fname = re.sub(r'^{}/'.format(rtpath), '', fname, 1)   # remove rtpath if exists         
      if opt == 1:
         fname = self.join_paths(rtpath, fname)
      elif opt == 2:
         fname = self.get_url_path(fname, url)
      return fname
   
   # get group saved file path for given file index and/or file name
   # opt = 0 - relative path to rtpath
   #       1 - absolute path
   #       2 - relative path to rtpath/savedpath
   #       4 - bit for ignore saved path
   def get_saved_path(self, i, fname, opt = 0, type = None, init = 0):
      addsp = 1
      rtpath = None
      if init or not self.RTPATH['dsid']: self.cache_rtpath(self.params['DS'], init)
      if not type: type = self.params['ST'][i] if 'ST' in self.params and self.params['ST'][i] else 'P'
      rtpath = "{}/{}".format(self.RTPATH['SP'], type)
      if opt > 3:
         addsp = 0
         opt -= 4
      if fname and re.match(r'^/', fname):
         if opt == 1 or not re.match(r'^{}/'.format(rtpath), fname): return fname
         fname = re.sub(r'^{}/'.format(rtpath), '', fname, 1)   # remove rtpath if exists         
     # find datset/group/user-defined subpath
      if 'SP' not in self.params:
         gindex = self.params['GI'][i] if 'GI' in self.params and self.params['GI'][i] else 0
         if gindex in self.savedpaths:
            savedpath = self.savedpaths[gindex]
         else:
            savedpath = savedpath = self.get_group_field_path(gindex, self.params['DS'], "savedpath")
            self.savedpaths[gindex] = savedpath if savedpath else ""
      elif len(self.params['SP']) == 1:
         savedpath = self.params['SP'][0]
      else:
         savedpath = self.params['SP'][i]
      if savedpath and opt != 1:
         ms = re.match(r'^(({}|{})/)'.format(rtpath, type), savedpath)
         if ms: savedpath = re.sub(ms.group(1), '', savedpath, 1)   # remove rtpath or leading type if exists
      if fname:
         if opt == 2:
            fname = self.join_paths(savedpath, fname, 1)   # remove savedpath if exists 
         else:
            if addsp: fname = self.join_paths(savedpath, fname)
            if opt != 0: fname = self.join_paths(rtpath, fname)
      elif opt == 0:
         if savedpath: fname = savedpath
      elif opt != 2:
         if addsp: fname = self.join_paths(rtpath, savedpath)
      return fname
   
   # get web file path with leading url
   def get_url_path(self, fname, url = None):
      if re.match(r'^{}'.format(self.PGLOG['DSSDATA']), fname):
         fname = re.sub(r'^{}'.format(self.PGLOG['DSSDATA']), '', fname)   # remove DSSDATA from file name
      elif re.match(r'^{}'.format(self.PGLOG['DSSWEB']), fname):
         fname = re.sub(r'^{}'.format(self.PGLOG['DSSWEB']), '', fname)   # remove DSSWEB from file name
      if not url: url = self.PGLOG['DSSURL']
      return self.join_paths(url, fname)
   # get get object path
   def get_object_path(self, fname, dsid, hpath = None):
      opath = "web/datasets/{}/{}".format(dsid, hpath) if hpath else dsid
      return self.join_paths(opath, fname)
   
   # view file counts for given dsid/gindex
   def view_filenumber(self, dsid, gindex, cnt = 0):
      if cnt:
         grecs = gindex
      else:
         cnt = 1
         grecs = [gindex]
      fields = ("dwebcnt, webcnt, cdwcnt, nwebcnt, dweb_size, nweb_size, savedcnt, saved_size")
      for i in range(cnt):
         if grecs[i] == 0:
            table = "dataset"
            cnd = "dsid = '{}'".format(dsid)
            head = dsid
         else:
            table = "dsgroup"
            cnd = "dsid = '{}' AND gindex = {}".format(dsid, grecs[i])
            head = "{}-G{}".format(dsid, grecs[i])
         pgrec = self.pgget(table, fields, cnd, self.PGOPT['extlog'])
         if pgrec:
            msg = ''
            if pgrec['webcnt']:
               if pgrec['dwebcnt']: msg += " DS-{} DC-{}".format(pgrec['dweb_size'], pgrec['dwebcnt'])
               if pgrec['cdwcnt']: msg += " CDC-{}".format(pgrec['cdwcnt'])
               if pgrec['nwebcnt']: msg += " NS-{} NC-{}".format(pgrec['nweb_size'], pgrec['nwebcnt'])
               msg += " WC-{}".format(pgrec['webcnt'])
            if pgrec['savedcnt']:
               msg += " SS-{} SC-{}".format(pgrec['saved_size'], pgrec['savedcnt'])
            if msg: print("{}:{}".format(head, msg))
   
   # get quasar backup file id numbers for given backup file names
   def get_bid_numbers(self, bfiles):
      bids = []
      if not bfiles: return bids
      dcnd = "dsid = '{}'".format(self.params['DS'])
      chksign = 1
      for bfile in bfiles:
         if not bfile:
            bids.append(0)
            chksign = 0
         elif isinstance(bfile, int):
            bids.append(bfile)
            chksign = 0
         elif re.match(r'^\d+$', bfile):
            bids.append(int(bfile))
            chksign = 0
         elif chksign and bfile in self.PGSIGNS:
            bids.append(bfile)
         else:
            chksign = 0
            bcnd = "bfile = '{}'".format(bfile)
            pgrec = self.pgget("bfile", "bid", "{} AND {}".format(dcnd, bcnd), self.PGOPT['extlog'])
            if not pgrec: pgrec = self.pgget("bfile", "bid", bcnd, self.PGOPT['extlog'])
            if pgrec:
               bids.append(pgrec['bid'])
            else:
               bids.append(0)
      return bids
   
   # build a backup filelist from given web/saved files
   def build_backup_filelist(self, ):
      dsid = self.params['DS']
      dscnd = "dsid = '{}'".format(dsid)
      if 'QF' not in self.params:
         self.params['QF'] = []
         bids = []
      else:
         bids = self.get_bid_numbers(self.params['QF'])
      if 'WF' in self.params:
         fcnt = len(self.params['WF'])
         if 'WT' not in self.params: self.params['WT'] = ['D']*fcnt
         for i in range(fcnt):
            type = self.params['WT'][i]
            file = self.get_web_path(i, self.params['WF'][i], 0, type)
            fstr = "{}: Type {} Web File of {}".format(file, type, dsid)
            typecnd = "type = '{}'".format(type)
            pgrec = self.pgget_wfile(dsid, 'bid', "{} AND wfile = '{}'".format(typecnd, file))
            if not pgrec: self.pglog(fstr + "not in RDADB", self.LGEREX)
            bid = pgrec['bid']
            if not bid: self.pglog(fstr + "not backed up", self.LGEREX)
            if file != self.params['WF'][i]: self.params['WF'][i] = file
            if bid in bids: continue
            pgrec = self.pgget('bfile', 'bfile', "bid = '{}'".format(bid))
            if not pgrec: self.pglog("{}: Backup Id {} not in RDADB".format(bid), self.LGEREX)
            bids.append(bid)
            self.params['QF'].append(pgrec['bfile'])
      if 'SF' in self.params:
         fcnt = len(self.params['SF'])
         for i in range(fcnt):
            type = self.params['ST'][i]
            file = self.get_saved_path(i, self.params['SF'][i], 0, type)
            fstr = "{}: Type {} Saved File of {}".format(file, type, dsid)
            typecnd = "type = '{}'".format(type)
            pgrec = self.pgget('sfile', 'bid', "{} AND {} AND sfile = '{}'".format(dscnd, typecnd, file))
            if not pgrec: self.pglog(fstr + "not in RDADB", self.LGEREX)
            bid = pgrec['bid']
            if not bid: self.pglog(fstr + "not backed up", self.LGEREX)
            if file != self.params['SF'][i]: self.params['SF'][i] = file
            if bid in bids: continue
            pgrec = self.pgget('bfile', 'bfile', "bid = '{}'".format(bid))
            if not pgrec: self.pglog("{}: Backup Id {} not in RDADB".format(fstr, bid), self.LGEREX)
            bids.append(bid)
            self.params['QF'].append(pgrec['bfile'])
   
   # get Quasar backup files for given ids
   def get_quasar_backfiles(self, bids):
      count = len(bids) if bids else 0
      bfiles = ['']*count
      for i in range(count):
         if not bids[i]: continue
         if not isinstance(bids[i], int):
            if not re.match(r'^\d+$', bids[i]):
               bfiles[i] = bids[i]
               continue
         pgrec = self.pgget("bfile", "bfile", "bid = {}".format(bids[i]), self.PGOPT['extlog'])
         if pgrec:  bfiles[i] = pgrec['bfile']
      return bfiles
   
   # get file names for reset group
   # 
   def get_filenames(self, tname):
      tmpgs = self.params['GI']
      self.params['GI'] = self.params['OG']
      self.pglog("Get file names of {} for reset group ...".format(self.params['DS']), self.PGOPT['wrnlog'])
      if 'GI' in self.params or 'GN' in self.params: self.validate_groups()
      if tname == 'wfile':
         pgrecs = self.pgmget_wfile(self.params['DS'], tname, self.get_condition(tname), self.PGOPT['extlog'])
      else:
         pgrecs = self.pgmget(tname, tname, self.get_condition(tname), self.PGOPT['extlog'])
      self.params['GI'] = tmpgs
      return (pgrecs[tname] if pgrecs else None)
   
   # get mss/web file names with relative path
   def get_relative_names(self, files, gindices, types, saved = 0):
      savegi = self.params['GI'] if 'GI' in self.params else None
      count = len(files)
      self.params['GI'] = gindices
      for i in range(count):
         if saved:
            files[i] = self.get_saved_path(i, files[i], 2, types[i])
         else:
            files[i] = self.get_web_path(i, files[i], 2, types[i])
      self.params['GI'] = savegi
      return files
   
   # reorder filelist
   def reorder_filelist(self, table):
      cnt = self.reorder_files(self.params['DS'], self.params['ON'], table)
      if cnt > 0: self.reset_rdadb_version(self.params['DS'])
   
   # 
   # get public group type
   def get_public_gtype(self, types, ptype, ptype2 = None):
      if not types: return None
      for type in types:
         if type != ptype and not (ptype2 and type == ptype2): return None
      return 'P'
   
   # get the data/doc/sofeware directory for given dataset id
   def cache_rtpath(self, dsid, force = 0):
      if not force and self.RTPATH['dsid'] and self.RTPATH['dsid'] == dsid: return
      self.RTPATH['dsid'] = dsid
      self.RTPATH['D'] = "{}/{}".format(self.PGLOG['DSDHOME'], dsid)
      self.RTPATH['A'] = self.RTPATH['N'] = self.RTPATH['I'] = self.RTPATH['U'] = self.RTPATH['D']
      self.RTPATH['H'] = "{}/{}".format(self.PGLOG['DSHHOME'], dsid)
      self.RTPATH['HD'] = self.RTPATH['O'] = "{}/{}".format(self.RTPATH['H'], self.HPATH['D'])
      self.RTPATH['HS'] = self.RTPATH['S'] = "{}/{}".format(self.RTPATH['H'], self.HPATH['S'])
      self.RTPATH['SP'] = "{}/{}".format(self.PGLOG['DECSHOME'], dsid)
   
   # gather file list for deleting
   def gather_delete_files(self, ):
      pgrecs = None
      if 'ST' in self.params:
         if 'RG' in self.params: self.get_subgroups("GS")
         pgrecs = self.pgmget("sfile", "sfile SF, gindex GI, type ST", self.get_condition('sfile', "IT"))
      elif 'WT' in self.params:
         if 'RG' in self.params: self.get_subgroups("GW")
         pgrecs = self.pgmget_wfile(self.params['DS'], "wfile WF, gindex GI, type WT", self.get_condition('wfile', "IT"))
      if not pgrecs: self.pglog("No file to delete for given condition", self.PGOPT['extlog'])
      for opt in pgrecs:
         self.params[opt.upper()] = pgrecs[opt]
   
   # gather subgroups
   def get_subgroups(self, cact):
      gindices = []
      dcnd = "dsid = '{}'".format(self.params['DS'])
      gtype = None
      if cact == "GG":
         if 'GT' in self.params: gtype = self.get_public_gtype(self.params['GT'], 'P')
         for gidx in self.params['GI']:
            subs = self.get_all_subgroups(dcnd, gidx, gtype)
            gindices.extend(subs)
      else:
         if cact == "GS":
            cfld = "savedcnt"
         elif cact == 'GW':
            cfld = 'webcnt'
            if 'WT' in self.params:
               if self.get_public_gtype(self.params['WT'], 'D'):
                  cfld = "dwebcnt"
               elif self.get_public_gtype(self.params['WT'], 'N'):
                  cfld = "nwebcnt"
         for gidx in self.params['GI']:
            subs = self.get_data_subgroups(dcnd, gidx, cfld)
            if subs: gindices.extend(subs)
      if gindices: self.params['GI'] = gindices
   
   #  clean empty directories for given dataset and groups
   def clean_dataset_directory(self, saved):
      cnt = len(self.params['GI']) if 'GI' in self.params else 1
      types = ()
      if saved:
         if 'ST' in self.params: types = set(self.params['ST'])
      else:
         if 'WT' in self.params: types = set(self.params['WT'])
      if types:
         cdirs = ()
         for i in range(cnt):
            if i > 0 and self.params['GI'][i] == self.params['GI'][i-1]: continue
            for type in types:
               if saved:
                  cdirs.add(self.get_saved_path(i, None, 1, type))
               else:
                  cdirs.add(self.get_web_path(i, None, 1, type))
         for cdir in cdirs:
            self.clean_empty_directory(cdir, self.PGLOG['GPFSHOST'])
   
   # validate given version indices
   def validate_versions(self, ):
      if self.OPTS['VI'][2]&8 == 8: return   # already validated
      self.OPTS['VI'][2] |= 8   # set validated flag
      vcnt = len(self.params['VI']) if 'VI' in self.params else 0
      if not vcnt:
         if self.PGOPT['CACT'] == 'SV':
            if 'NV' not in self.params: self.action_error("Add Mode option -NV for new version control record")
            self.params['VI'] = [0]
         return
      for i in range(vcnt):
         val = self.params['VI'][i]
         if not val:
            if 'NV' not in self.params:
               self.action_error("Mode option -NV must be present to add new version control record")
            continue
         if i and val == self.params['VI'][i-1]: continue
         pgrec = self.pgget("dsvrsn", "dsid", "vindex = {}".format(val), self.PGOPT['extlog'])
         if not pgrec:
            self.action_error("Version Control Index val is not in RDADB")
         elif pgrec['dsid'] != self.params['DS']:
            self.action_error("Version Control Index {} is for {}".format(val, pgrec['dsid']))
   
   # validate local files for getting MD5 checksum
   def validate_md5_checksum(self, allcnt):
      if 'SC' in self.params and self.params['MC']:
         del self.params['SC']
      elif 'MC' not in self.params:
         self.params['MC'] = ['']*allcnt   # create place hold for MD5 cehcksum
         i = 0
         if 'LF' in self.params:
            while i < allcnt:
               if not op.exists(self.params['LF'][i]): break
               i += 1
         else:
            i = 0
         if i >= allcnt:
            self.OPTS['MC'][2] |= 16
            if 'SC' not in self.params: self.params['SC'] = 1
         else:
            if 'SC' in self.params:
               s = 's' if allcnt > 1 else ''
               self.pglog("Cannot locate local files for md5 checksums!", self.PGOPT['emlerr'])
               del self.params['SC']
            if 'MC' in self.params: del self.params['MC']
   
   # append moving info to bfile.note
   def save_move_info(self, bid, ffile, ftype, fftype, fdsid, tfile, ttype, tftype, tdsid):
      bcnd = "bid = {}".format(bid)
      flds = 'note'
      if fftype != tftype: flds += ', scount, wcount'
      pgrec = self.pgget('bfile', flds, bcnd, self.LGEREX)
      if not pgrec: return 0
      sftype = ('Web' if fftype == 'W' else 'Saved')
      note = ">MV {} Type {} {} File {} To".format(fdsid, ftype, sftype, ffile)
      if tdsid != fdsid: note += ' ' + tdsid
      if ttype != ftype: note += ' Type ' + ttype
      if tftype != fftype: note += (' Web' if tftype == 'W' else ' Saved')
      note += ' File'
      if tfile != ffile: note += ' ' + tfile
      note += '\n'
      if pgrec['note']:
         if pgrec['note'][-1] != '\n': pgrec['note'] += "\n"
         pgrec['note'] += note
      else:
         pgrec['note'] = note
      if fftype == 'W' and tftype == 'S':
         pgrec['scount'] += 1
         pgrec['wcount'] -= 1
      elif fftype == 'S' and tftype == 'W':
         pgrec['scount'] -= 1
         pgrec['wcount'] += 1
      return self.pgupdt('bfile', pgrec, bcnd, self.LGEREX)
   
   # set dataset location flag
   def set_dataset_locflag(self, dsid, locflag):
      dscnd = "dsid = '{}'".format(dsid)
      if self.pgget('dataset', '', "{} AND locflag = '{}'".format(dscnd, locflag)): return
      cnd = "type = 'D' AND status = 'P' AND locflag <> '{}'".format(locflag)
      if self.pgget_wfile(dsid, '', cnd): return
      self.pgexec("UPDATE dataset SET locflag = '{}' WHERE {}".format(locflag, dscnd))
   
   # get dataset location flag
   def get_dataset_locflag(self, dsid):
      dscnd = "dsid = '{}'".format(dsid)
      pgrec = self.pgget('dataset', 'locflag', dscnd)
      return pgrec['locflag'] if pgrec else 'G'
