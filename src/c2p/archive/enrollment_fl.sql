/*
transform MBSF denominator files into CDM ENROLLMENT table structure
 ref: https://github.com/kumc-bmi/grouse/blob/master/etl_i2b2/sql_scripts/cms_enr_dstats.sql
*/

/*setup environment*/
use role GROUSE_ROlE_B_ADMIN;
use warehouse GROUSE_WH;
use database GROUSE_DB;

create schema if not exists CMS_PCORNET_CDM;
use schema CMS_PCORNET_CDM;

-- initialize table
create table PRIVATE_ENROLLMENT if not exists (
     PATID varchar(50) NOT NULL
    ,ENR_START_DATE date NOT NULL
	,ENR_END_DATE date NULL
	,CHART varchar(1) NULL
	,ENR_BASIS varchar(1) NOT NULL
--	,RAW_CHART varchar(50) NULL
	,RAW_BASIS varchar(50) NULL
);

create table PRIVATE_ENROLLMENT_AB_STAGE if not exists (
    BENE_ID varchar(50) NOT NULL,
    RFRNC_YR varchar(5) NOT NULL,
    BUYIN01 varchar(1) NULL,
    BUYIN02 varchar(1) NULL,
    BUYIN03 varchar(1) NULL, 
    BUYIN04 varchar(1) NULL,
    BUYIN05 varchar(1) NULL, 
    BUYIN06 varchar(1) NULL,
    BUYIN07 varchar(1) NULL,
    BUYIN08 varchar(1) NULL,
    BUYIN09 varchar(1) NULL,
    BUYIN10 varchar(1) NULL,
    BUYIN11 varchar(1) NULL,
    BUYIN12 varchar(1) NULL,
    HMOIND01 varchar(1) NULL,
    HMOIND02 varchar(1) NULL,
    HMOIND03 varchar(1) NULL,
    HMOIND04 varchar(1) NULL,
    HMOIND05 varchar(1) NULL,
    HMOIND06 varchar(1) NULL,
    HMOIND07 varchar(1) NULL,
    HMOIND08 varchar(1) NULL,
    HMOIND09 varchar(1) NULL,
    HMOIND10 varchar(1) NULL,
    HMOIND11 varchar(1) NULL,
    HMOIND12 varchar(1) NULL
);

create table PRIVATE_ENROLLMENT_D_STAGE if not exists (
    BENE_ID varchar(50) NOT NULL,
    RFRNC_YR varchar(5) NOT NULL,
    PTDCNTRCT01 varchar(5) NULL,
    PTDCNTRCT02 varchar(5) NULL,
    PTDCNTRCT03 varchar(5) NULL,
    PTDCNTRCT04 varchar(5) NULL,
    PTDCNTRCT05 varchar(5) NULL,
    PTDCNTRCT06 varchar(5) NULL,
    PTDCNTRCT07 varchar(5) NULL,
    PTDCNTRCT08 varchar(5) NULL,
    PTDCNTRCT09 varchar(5) NULL,
    PTDCNTRCT10 varchar(5) NULL,
    PTDCNTRCT11 varchar(5) NULL,
    PTDCNTRCT12 varchar(5) NULL,
    PTDPBPID01 varchar(3) NULL,
    PTDPBPID02 varchar(3) NULL,
    PTDPBPID03 varchar(3) NULL,
    PTDPBPID04 varchar(3) NULL,
    PTDPBPID05 varchar(3) NULL,
    PTDPBPID06 varchar(3) NULL,
    PTDPBPID07 varchar(3) NULL,
    PTDPBPID08 varchar(3) NULL,
    PTDPBPID09 varchar(3) NULL,
    PTDPBPID10 varchar(3) NULL,
    PTDPBPID11 varchar(3) NULL,
    PTDPBPID12 varchar(3) NULL,
    RDSIND01 varchar(1) NULL,
    RDSIND02 varchar(1) NULL,
    RDSIND03 varchar(1) NULL,
    RDSIND04 varchar(1) NULL,
    RDSIND05 varchar(1) NULL,
    RDSIND06 varchar(1) NULL,
    RDSIND07 varchar(1) NULL,
    RDSIND08 varchar(1) NULL,
    RDSIND09 varchar(1) NULL,
    RDSIND10 varchar(1) NULL,
    RDSIND11 varchar(1) NULL,
    RDSIND12 varchar(1) NULL
);

/** Part AB Enrollment*/
-- snowsql unpivot doesn't take additional columns beyond what is needed after unpivoting
insert into PRIVATE_ENROLLMENT_AB_STAGE
select bene_id, rfrnc_yr,
       BUYIN01,BUYIN02,BUYIN03,BUYIN04,BUYIN05,BUYIN06,BUYIN07,BUYIN08,BUYIN09,BUYIN10,BUYIN11,BUYIN12,
       HMOIND01,HMOIND02,HMOIND03,HMOIND04,HMOIND05,HMOIND06,HMOIND07,HMOIND08,HMOIND09,HMOIND10,HMOIND11,HMOIND12 
from MEDICARE_2017.mbsf_abcd_summary
    union
select bene_id, rfrnc_yr,
       BUYIN01,BUYIN02,BUYIN03,BUYIN04,BUYIN05,BUYIN06,BUYIN07,BUYIN08,BUYIN09,BUYIN10,BUYIN11,BUYIN12,
       HMOIND01,HMOIND02,HMOIND03,HMOIND04,HMOIND05,HMOIND06,HMOIND07,HMOIND08,HMOIND09,HMOIND10,HMOIND11,HMOIND12 
from MEDICARE_2016.mbsf_abcd_summary
    union
select bene_id, rfrnc_yr,
       BUYIN01,BUYIN02,BUYIN03,BUYIN04,BUYIN05,BUYIN06,BUYIN07,BUYIN08,BUYIN09,BUYIN10,BUYIN11,BUYIN12,
       HMOIND01,HMOIND02,HMOIND03,HMOIND04,HMOIND05,HMOIND06,HMOIND07,HMOIND08,HMOIND09,HMOIND10,HMOIND11,HMOIND12 
from MEDICARE_2015.mbsf_abcd_summary
    union
select bene_id, rfrnc_yr,
       BUYIN01,BUYIN02,BUYIN03,BUYIN04,BUYIN05,BUYIN06,BUYIN07,BUYIN08,BUYIN09,BUYIN10,BUYIN11,BUYIN12,
       HMOIND01,HMOIND02,HMOIND03,HMOIND04,HMOIND05,HMOIND06,HMOIND07,HMOIND08,HMOIND09,HMOIND10,HMOIND11,HMOIND12 
from MEDICARE_2014.mbsf_abcd_summary
    union
select bene_id, rfrnc_yr,
       BUYIN01,BUYIN02,BUYIN03,BUYIN04,BUYIN05,BUYIN06,BUYIN07,BUYIN08,BUYIN09,BUYIN10,BUYIN11,BUYIN12,
       HMOIND01,HMOIND02,HMOIND03,HMOIND04,HMOIND05,HMOIND06,HMOIND07,HMOIND08,HMOIND09,HMOIND10,HMOIND11,HMOIND12 
from MEDICARE_2013.mbsf_ab_summary
    union
select bene_id, rfrnc_yr,
       BUYIN01,BUYIN02,BUYIN03,BUYIN04,BUYIN05,BUYIN06,BUYIN07,BUYIN08,BUYIN09,BUYIN10,BUYIN11,BUYIN12,
       HMOIND01,HMOIND02,HMOIND03,HMOIND04,HMOIND05,HMOIND06,HMOIND07,HMOIND08,HMOIND09,HMOIND10,HMOIND11,HMOIND12 
from MEDICARE_2012.mbsf_ab_summary
    union
select bene_id, rfrnc_yr,
       BUYIN01,BUYIN02,BUYIN03,BUYIN04,BUYIN05,BUYIN06,BUYIN07,BUYIN08,BUYIN09,BUYIN10,BUYIN11,BUYIN12,
       HMOIND01,HMOIND02,HMOIND03,HMOIND04,HMOIND05,HMOIND06,HMOIND07,HMOIND08,HMOIND09,HMOIND10,HMOIND11,HMOIND12
from MEDICARE_2011.mbsf_ab_summary
;
-- multi unpivot
with per_bene_mo as (
select bene_id, rfrnc_yr, buyin, hmo, right(buyin_mo,2) as mo
from PRIVATE_ENROLLMENT_AB_STAGE
unpivot 
 (buyin for buyin_mo in (BUYIN01,
                         BUYIN02,
                         BUYIN03,
                         BUYIN04,
                         BUYIN05,
                         BUYIN06,
                         BUYIN07,
                         BUYIN08,
                         BUYIN09,
                         BUYIN10,
                         BUYIN11,
                         BUYIN12)
  ) buyin_unpvt

unpivot 
 (hmo for hmo_mo in (  HMOIND01,
                       HMOIND02,
                       HMOIND03,
                       HMOIND04,
                       HMOIND05,
                       HMOIND06,
                       HMOIND07,
                       HMOIND08,
                       HMOIND09,
                       HMOIND10,
                       HMOIND11,
                       HMOIND12)
  ) hmo_unpvt

where right(buyin_mo,2) = right(hmo_mo,2) and
      buyin != '0' -- Not entitled
)
   ,ab_cte_cvg as (
select bene_id, rfrnc_yr, mo, buyin, hmo,
       to_date(replace(rfrnc_yr,',','') || mo, 'YYYYMM') as enroll_mo,
       case when hmo = '0' and buyin in ('1', 'A') then 'A'
            when hmo = '0' and buyin in ('2', 'B') then 'B'
            when hmo = '0' and buyin in ('3','C') then 'AB'
            when hmo in ('1', '2', '4', 'A', 'B', 'C') and buyin in ('1', 'A') then 'HMO_A'
            when hmo in ('1', '2', '4', 'A', 'B', 'C') and buyin in ('2', 'B') then 'HMO_B'
            when hmo in ('1', '2', '4', 'A', 'B', 'C') and buyin in ('3', 'C') then 'HMO_AB'
       end as coverage
from per_bene_mo
)
   ,ab_cte_cvg_agg as(
SELECT ab_cte_cvg.*,
       -- ack: https://blog.jooq.org/2015/11/07/how-to-find-the-longest-consecutive-series-of-events-in-sql/
       dateadd(month, - dense_rank() over (partition by bene_id, coverage order by bene_id, enroll_mo, coverage) + 1,enroll_mo) series
FROM ab_cte_cvg
)
select bene_id,
       min(enroll_mo),
       max(enroll_mo),
       'Y',
       'I',
       coverage
from   ab_cte_cvg_agg
group by bene_id, coverage, series
;
commit;


/** Part D Enrollment*/
insert into PRIVATE_ENROLLMENT_D_STAGE
select bene_id, rfrnc_yr,
    PTDCNTRCT01,PTDCNTRCT02,PTDCNTRCT03,PTDCNTRCT04,PTDCNTRCT05,PTDCNTRCT06,PTDCNTRCT07,PTDCNTRCT08,PTDCNTRCT09,PTDCNTRCT10,PTDCNTRCT11,PTDCNTRCT12,
    PTDPBPID01,PTDPBPID02,PTDPBPID03,PTDPBPID04,PTDPBPID05,PTDPBPID06,PTDPBPID07,PTDPBPID08,PTDPBPID09,PTDPBPID10,PTDPBPID11,PTDPBPID12,
    RDSIND01,RDSIND02,RDSIND03,RDSIND04,RDSIND05,RDSIND06,RDSIND07,RDSIND08,RDSIND09,RDSIND10,RDSIND11,RDSIND12
from MEDICARE_2017.mbsf_abcd_summary
    union
select bene_id, rfrnc_yr,
    PTDCNTRCT01,PTDCNTRCT02,PTDCNTRCT03,PTDCNTRCT04,PTDCNTRCT05,PTDCNTRCT06,PTDCNTRCT07,PTDCNTRCT08,PTDCNTRCT09,PTDCNTRCT10,PTDCNTRCT11,PTDCNTRCT12,
    PTDPBPID01,PTDPBPID02,PTDPBPID03,PTDPBPID04,PTDPBPID05,PTDPBPID06,PTDPBPID07,PTDPBPID08,PTDPBPID09,PTDPBPID10,PTDPBPID11,PTDPBPID12,
    RDSIND01,RDSIND02,RDSIND03,RDSIND04,RDSIND05,RDSIND06,RDSIND07,RDSIND08,RDSIND09,RDSIND10,RDSIND11,RDSIND12 
from MEDICARE_2016.mbsf_abcd_summary
    union
select bene_id, rfrnc_yr,
    PTDCNTRCT01,PTDCNTRCT02,PTDCNTRCT03,PTDCNTRCT04,PTDCNTRCT05,PTDCNTRCT06,PTDCNTRCT07,PTDCNTRCT08,PTDCNTRCT09,PTDCNTRCT10,PTDCNTRCT11,PTDCNTRCT12,
    PTDPBPID01,PTDPBPID02,PTDPBPID03,PTDPBPID04,PTDPBPID05,PTDPBPID06,PTDPBPID07,PTDPBPID08,PTDPBPID09,PTDPBPID10,PTDPBPID11,PTDPBPID12,
    RDSIND01,RDSIND02,RDSIND03,RDSIND04,RDSIND05,RDSIND06,RDSIND07,RDSIND08,RDSIND09,RDSIND10,RDSIND11,RDSIND12
from MEDICARE_2015.mbsf_abcd_summary
    union
select bene_id, rfrnc_yr,
    PTDCNTRCT01,PTDCNTRCT02,PTDCNTRCT03,PTDCNTRCT04,PTDCNTRCT05,PTDCNTRCT06,PTDCNTRCT07,PTDCNTRCT08,PTDCNTRCT09,PTDCNTRCT10,PTDCNTRCT11,PTDCNTRCT12,
    PTDPBPID01,PTDPBPID02,PTDPBPID03,PTDPBPID04,PTDPBPID05,PTDPBPID06,PTDPBPID07,PTDPBPID08,PTDPBPID09,PTDPBPID10,PTDPBPID11,PTDPBPID12,
    RDSIND01,RDSIND02,RDSIND03,RDSIND04,RDSIND05,RDSIND06,RDSIND07,RDSIND08,RDSIND09,RDSIND10,RDSIND11,RDSIND12
from MEDICARE_2014.mbsf_abcd_summary
    union
select bene_id, rfrnc_yr,
    CNTRCT01,CNTRCT02,CNTRCT03,CNTRCT04,CNTRCT05,CNTRCT06,CNTRCT07,CNTRCT08,CNTRCT09,CNTRCT10,CNTRCT11,CNTRCT12,
    PBPID01,PBPID02,PBPID03,PBPID04,PBPID05,PBPID06,PBPID07,PBPID08,PBPID09,PBPID10,PBPID11,PBPID12,
    RDSIND01,RDSIND02,RDSIND03,RDSIND04,RDSIND05,RDSIND06,RDSIND07,RDSIND08,RDSIND09,RDSIND10,RDSIND11,RDSIND12
from MEDICARE_2013.mbsf_d_cmpnts
    union
select bene_id, rfrnc_yr,
    CNTRCT01,CNTRCT02,CNTRCT03,CNTRCT04,CNTRCT05,CNTRCT06,CNTRCT07,CNTRCT08,CNTRCT09,CNTRCT10,CNTRCT11,CNTRCT12,
    PBPID01,PBPID02,PBPID03,PBPID04,PBPID05,PBPID06,PBPID07,PBPID08,PBPID09,PBPID10,PBPID11,PBPID12,
    RDSIND01,RDSIND02,RDSIND03,RDSIND04,RDSIND05,RDSIND06,RDSIND07,RDSIND08,RDSIND09,RDSIND10,RDSIND11,RDSIND12
from MEDICARE_2012.mbsf_d_cmpnts
    union
select bene_id, rfrnc_yr,
    CNTRCT01,CNTRCT02,CNTRCT03,CNTRCT04,CNTRCT05,CNTRCT06,CNTRCT07,CNTRCT08,CNTRCT09,CNTRCT10,CNTRCT11,CNTRCT12,
    PBPID01,PBPID02,PBPID03,PBPID04,PBPID05,PBPID06,PBPID07,PBPID08,PBPID09,PBPID10,PBPID11,PBPID12,
    RDSIND01,RDSIND02,RDSIND03,RDSIND04,RDSIND05,RDSIND06,RDSIND07,RDSIND08,RDSIND09,RDSIND10,RDSIND11,RDSIND12
from MEDICARE_2011.mbsf_d_cmpnts
;
-- multi unpivot
with per_bene_mo as (
select bene_id, rfrnc_yr, buyin, pbp, rds, right(buyin_mo,2) as mo
from PRIVATE_ENROLLMENT_D_STAGE
unpivot 
 (buyin for buyin_mo in (PTDCNTRCT01,
                         PTDCNTRCT02,
                         PTDCNTRCT03,
                         PTDCNTRCT04,
                         PTDCNTRCT05,
                         PTDCNTRCT06,
                         PTDCNTRCT07,
                         PTDCNTRCT08,
                         PTDCNTRCT09,
                         PTDCNTRCT10,
                         PTDCNTRCT11,
                         PTDCNTRCT12)
  ) buyin_unpvt

unpivot 
 (pbp for pbp_mo in (PTDPBPID01,
                     PTDPBPID02,
                     PTDPBPID03,
                     PTDPBPID04,
                     PTDPBPID05,
                     PTDPBPID06,
                     PTDPBPID07,
                     PTDPBPID08,
                     PTDPBPID09,
                     PTDPBPID10,
                     PTDPBPID11,
                     PTDPBPID12)
  ) pbp_unpvt

unpivot 
 (rds for rds_mo in (RDSIND01,
                     RDSIND02,
                     RDSIND03,
                     RDSIND04,
                     RDSIND05,
                     RDSIND06,
                     RDSIND07,
                     RDSIND08,
                     RDSIND09,
                     RDSIND10,
                     RDSIND11,
                     RDSIND12)
  ) rds_unpvt

where right(buyin_mo,2) = right(pbp_mo,2) and
      right(buyin_mo,2) = right(rds_mo,2) and
      buyin != '0' -- Not entitled
)
   ,d_cte_cvg as (
select bene_id, rfrnc_yr, mo, buyin, pbp, rds,
       to_date(replace(rfrnc_yr,',','') || mo, 'YYYYMM') as enroll_mo,
       substr(buyin, 1, 1) as coverage
from per_bene_mo
/*Interpretation:
- PTD_CNTRCT_ID_XX are contract IDs. Any that start with values of  ('E', 'H', 'R', 'S', 'X') submit all PDE to CMS (i.e. are observable)
- PTD_PBP_ID_XX are benefit packages. If there is a contract ID, this should also be filled in (note, technically it's supposed to be a 3-digit alphanumeric that can include leading zeros).
- RDS_IND_XX are employer-offered prescription drug plans.  These do not submit all PDE to CMS.  So we only include those without it.
*/
where substr(buyin, 1, 1) in ('E', 'H', 'R', 'S', 'X') and 
      pbp is NOT NULL and rds = 'N'
)
   ,d_cte_cvg_agg as(
SELECT d_cte_cvg.*,
       -- ack: https://blog.jooq.org/2015/11/07/how-to-find-the-longest-consecutive-series-of-events-in-sql/
       dateadd(month, - dense_rank() over (partition by bene_id, coverage order by bene_id, enroll_mo, coverage) + 1,enroll_mo) series
FROM d_cte_cvg
)
select bene_id,
       min(enroll_mo),
       max(enroll_mo),
       'Y',
       'D',
       count(*) || substr(listagg(coverage, '') within group (order by enroll_mo), 1, 45) raw_basis
from   d_cte_cvg_agg
group by bene_id, coverage, series
;
commit;


