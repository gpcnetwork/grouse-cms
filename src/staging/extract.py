#####################################################################     
# Copyright (c) 2021-2022 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: extract.py                                                 
# The file parse the decrypted .fts files with metadata information
# and generate DDL/DML scripts for staging data into snowflake                                          
#####################################################################
# YOU DON'T NEED TO RUN THIS SCRIPT, IT WILL BE CALLED BY load.py
#####################################################################
from typing import Dict
from re import findall, match
from itertools import compress
from abc import ABC, abstractmethod

class SDAFileNameParser:
    '''
    Parse the data file names to identify key information to suggest data staging target
    Input: str (filename)
    Output: dict (year, tname)
    '''
    def __init__(self,fname,stopwords = ["j","k","file"]):
        self.fname = fname
        self.stopwords = stopwords
        
    @property
    def parse_fname(self,sep="_") -> dict:
        # name follows certain convention with "_" as separator
        fn_lst = self.fname.split(sep)
        tname = []
        for str_pt in fn_lst:
            if "req" in str_pt:
                req_num = str_pt.lstrip("req").lstrip("0")
            elif "res" in str_pt:
                res_num = str_pt.lstrip("res").lstrip("0")
            elif match(r'[1-3][0-9]{3}',str_pt) is not None:
                yr = str_pt
            elif str_pt not in self.stopwords:
                tname.append(str_pt)             
        # create a dictionary as output
        fn_out = {
            "year": yr,
            "tname": sep.join(tname),
        }
        return(fn_out)
    
    @property
    def GetSchemaName(self):
        patterns = ['^(?=.*max.*$)+','^((?!max|xwalk).)*$','^(?=.*bene.*xwalk.*$)+','^(?=.*msis.*xwalk.*$)+'] 
        schema_prefix = ['MEDICAID','MEDICARE','BENE_MAPPING','MSIS_MAPPING']
        reg_match_bool = [bool(match(p,self.parse_fname["tname"])) for p in patterns]
        schema_name = list(compress(schema_prefix,reg_match_bool))[0]
        # data from different years are separated into different schemas
        if (reg_match_bool.index(True) < 2):
            schema_name += '_%s' % self.parse_fname["year"]
        return(schema_name)
    
    @property
    def GetTableName(self):
        table_name = self.parse_fname["tname"]
        # mapping from different years are saved under the same schema but separated by table
        if (match('(?=.*xwalk.*$)+',table_name)):
            table_name += '_%s' % self.parse_fname["year"]
        return(table_name)

class FTSParser:
    """
    Parse out important parameters from the FTS files
    Input: file object
    Output: dict[list[str]]
    """
    def __init__(self,filedat):
        self.filedat = filedat
    
    def parse_body(self) -> dict:
        formt = []
        colsize = []
        rowsize_part = []
        rowsize = []
        filesize = []
        meta = []
        meta_field_width = []
        file_list = [line.rstrip('\n') for line in self.filedat]
        line_cntr = 0
        for line in file_list:
            # parse the header portion
            formt += filter(None,findall('Type|Format: (.*)', line))
            colsize += filter(None,[int(c.replace(",","")) for c in findall(r'[Columns|Variables] in File: (.*)', line)])
            rowsize_part += filter(None,[int(c.replace(",","")) for c in findall(r'\.dat \((.*?)\s+[R|r]ows\)',line)])
            rowsize += filter(None,[int(c.replace(",","")) for c in findall(r'Exact File Quantity \(Rows\): (.*)', line)])
            filesize += filter(None,[int(c.replace(",","")) for c in findall(r'Exact File Size in Bytes with 512 Blocksize: (.*)', line)])
            '''
            parse the main body of metadata
            assumptions after inspecting sample fts files:
            1. the '--- ----- ---' line is used to populate meta_field_width which also marks the start of metadata table
            2. the first empty line after metadata table is used to mark the end of the metadata table
            '''
            if match('^-+\s+', line):
                meta_field_width = [len(c.strip()) for c in line.split(' ') if c.strip()]
            elif meta_field_width != []:
                line_cntr+=1
                '''assumption: colsize shouldn't be empty at this point '''
                if line_cntr <= colsize[0]:
                    cols = []
                    cols_mod = []
                    start = 0
                    for width in meta_field_width:
                        line_sec = line[start:start + width].strip()
                        # expand number precision due to potential data entry error
                        if(match('^[0-9]+[\.]+[0-9]+$',line_sec)):
                            line_sec_split = line_sec.split('.')
                            prec = int(line_sec_split[0].strip())
                            prec_mod = prec + 2 # expand precision by 2 digits
                            scale = line_sec_split[1]
                            line_sec = f'{prec},{scale},{prec_mod}' # incompatiable SAS format for numerics, i.e. 12.2 -> 12,2
                        cols.append(line_sec) 
                        start += width + 1
                    meta.append(cols) 
                elif line.strip() == '':
                    break # end of file
                    
        stdout = {
            "format": formt,
            "colsize": colsize,
            "rowsize_part": rowsize_part,
            "rowsize": rowsize,
            "filesize": filesize,
            "meta": meta
        }
        return stdout


class SqlGenerator(ABC):
    """
    abstract-based class for generating SQL statments
    note that in the gloable variable " _sql_type_map ", %width is an optional variable, which can be replaced by other values
    """
    
    _sql_type_map = {
        'CHAR': ('VARCHAR' + '(%(width)s)','',''),
        'NUM': ('NUMBER' + '(%(width)s)','TRY_TO_DECIMAL(',')'),
        'NUMERIC': ('NUMBER','TRY_TO_DECIMAL(',')'),
        'DATE': ('DATE','TRY_TO_DATE(',')'),
        'TIME': ('VARCHAR(8)','','')
    }
    
    @abstractmethod
    def GenerateDDL(self):
        """generate DDL statment based on metadata stored in a dictionary"""
        pass
    
    @abstractmethod
    def GenerateDML(self):
        """generate DML statment based on metadata stored in a dictionary"""
        pass
    
    @abstractmethod
    def GenerateDDML(self):
        """generate DDL+DML statment based on metadata stored in a dictionary"""
        pass


class SqlGenerator_PcornetURL(SqlGenerator):
    """
    Generate DDL and DML based on metadata file (e.g. in XLSX format) downloaded from an URL
    Input: url link to PCORNET CDM parsable file
    - https://pcornet.org/wp-content/uploads/2021/11/2021_11_29_PCORnet_Common_Data_Model_v6dot0_parseable.xlsx
    Output: derived target schema name and three SQL statements
      1.DDL: create table...; 
      2.DML: insert into...select substr()...from...;
      3.DDML: create table as select from...;
    """
    
    def __init__(self,schema_name:str,table_name:str,meta:list):
        self.schema_name = schema_name
        self.table_name = table_name
        self.meta = meta
        
    def GenerateDDL(self):
        sql = '''CREATE OR REPLACE TABLE %(schema_name)s.%(table_name)s (%(cols)s);''' % dict (
            schema_name = self.schema_name,
            table_name = self.table_name,
            cols = ',\n'.join('"%s" %s' % (item[0],self._sql_type_map[item[1]][0] % dict(width='600')) 
                                           for item in self.meta)
        )
        return (sql)
    
    def GenerateDML(self):
        print('not supported!')
        
    def GenerateDDML(self):
        print('not supported!')

class SqlGenerator_FTS(SqlGenerator):
    """
    Generate DDL and DML based on FTS metadata file output
    Input: FTSParser-class object
      1.dname: name of source data file
      2.ftsout: output of FTSParser
      3.stg_tname: name of snowflake stage where source data is temporarily stored 
    Output: derived target schema name and three SQL statements
      1.DDL: create table...; 
      2.DML: insert into...select substr()...from...;
      3.DDML: create table as select from...;
    """
    
    def __init__(self,dname:str,ftsout:dict,stg_tname:str):
        self.ftsout = ftsout
        self.stg_tname = stg_tname
        self.fnameout = SDAFileNameParser(dname) #output are lists

    def GenerateDDL(self):
        sql = '''CREATE OR REPLACE TABLE %(schema_name)s.%(table_name)s (%(cols)s);''' % dict (
            schema_name = self.fnameout.GetSchemaName,
            table_name = self.fnameout.GetTableName,
            cols = ',\n'.join('%s %s' % (item[-5],self._sql_type_map[item[-4]][0] % dict(width=','.join(item[-2].split(',')[-2:][::-1]))) 
                                         for item in self.ftsout["meta"])
        )
        return (sql)
    
    def GenerateDML(self):
        # need to remove uneven white spaces within PLAIN_TEXT_COL after SUBSTR
        # need to transform field length:"12,2" -> ending position:"12"
        sql = '''INSERT INTO %(schema_name)s.%(table_name)s (%(cols)s)
                 SELECT %(substr_statements)s
                 FROM %(stg_tname)s;''' % dict (
            schema_name = self.fnameout.GetSchemaName,
            table_name = self.fnameout.GetTableName, 
            cols = ','.join('%s' % (item[-5]) 
                              for item in self.ftsout["meta"]),
            substr_statements = ',\n'.join('%sTRIM(SUBSTR(PLAIN_TEXT_COL, %s, %s))%s as %s' % (self._sql_type_map[item[-4]][1],
                                                                                               item[-3],item[-2].split(',')[0],
                                                                                               self._sql_type_map[item[-4]][2],
                                                                                               item[-5])  
                                           for item in self.ftsout["meta"]),
            stg_tname = self.stg_tname
        )
        return(sql)
        
    def GenerateDDML(self):
        # need to remove uneven white spaces within PLAIN_TEXT_COL after SUBSTR
        # need to transform field length:"12,2" -> ending position:"12"
        sql = '''CREATE OR REPLACE TABLE %(schema_name)s.%(table_name)s AS
                 SELECT %(substr_statements)s
                 FROM %(stg_tname)s;''' % dict (
            schema_name = self.fnameout.GetSchemaName,
            table_name = self.fnameout.GetTableName, 
            substr_statements = ',\n'.join('%sTRIM(SUBSTR(PLAIN_TEXT_COL, %s, %s))%s as %s' % (self._sql_type_map[item[-4]][1],
                                                                                               item[-3],item[-2].split(',')[0],
                                                                                               self._sql_type_map[item[-4]][2],
                                                                                               item[-5])  
                                           for item in self.ftsout["meta"]),
            stg_tname = self.stg_tname
        )
        return(sql)