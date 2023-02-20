/*set environment*/
use role GROUSE_ROlE_B_ADMIN;
use warehouse GROUSE_WH;
use database GROUSE_DB;
use schema SCRATCH; 

with mbsf_state as (
select BENE_ID, RFRNC_YR, STATE_CD from MEDICARE_2011.MBSF_AB_SUMMARY
  union
select BENE_ID, RFRNC_YR, STATE_CD from MEDICARE_2012.MBSF_AB_SUMMARY
  union
select BENE_ID, RFRNC_YR, STATE_CD from MEDICARE_2013.MBSF_AB_SUMMARY
  union
select BENE_ID, RFRNC_YR, STATE_CD from MEDICARE_2014.MBSF_ABCD_SUMMARY
  union
select BENE_ID, RFRNC_YR, STATE_CD from MEDICARE_2015.MBSF_ABCD_SUMMARY
  union
select BENE_ID, RFRNC_YR, STATE_CD from MEDICARE_2016.MBSF_ABCD_SUMMARY
  union
select BENE_ID, RFRNC_YR, STATE_CD from MEDICARE_2017.MBSF_ABCD_SUMMARY
)
select state_cd, count(distinct bene_id)
from mbsf_state
group by state_cd
order by state_cd
;

/*Use CMS RIF Schema*/
-- get overall CMS beneficiary stack at patient-year level
create or replace table PRIVATE_ENROLLMENT_YEARLY as
with mbsf_ab_cte as (
select BENE_ID, BENE_DOB, DEATH_DT, SEX, RTI_RACE_CD, COVSTART, OREC, CREC, RFRNC_YR, A_MO_CNT, B_MO_CNT, HMO_MO from MEDICARE_2011.MBSF_AB_SUMMARY
  union
select BENE_ID, BENE_DOB, DEATH_DT, SEX, RTI_RACE_CD, COVSTART, OREC, CREC, RFRNC_YR, A_MO_CNT, B_MO_CNT, HMO_MO from MEDICARE_2012.MBSF_AB_SUMMARY
  union
select BENE_ID, BENE_DOB, DEATH_DT, SEX, RTI_RACE_CD, COVSTART, OREC, CREC, RFRNC_YR, A_MO_CNT, B_MO_CNT, HMO_MO from MEDICARE_2013.MBSF_AB_SUMMARY
),
    mbsf_d_cte as (
select BENE_ID, RFRNC_YR,PLNCOVMO, DUAL_MO from MEDICARE_2011.MBSF_D_CMPNTS
   union
select BENE_ID, RFRNC_YR,PLNCOVMO, DUAL_MO from MEDICARE_2012.MBSF_D_CMPNTS
   union
select BENE_ID, RFRNC_YR,PLNCOVMO, DUAL_MO from MEDICARE_2013.MBSF_D_CMPNTS
),
    mbsf_abcd_reconstr as (
select distinct
       ab.BENE_ID, ab.BENE_DOB, ab.DEATH_DT, ab.SEX, ab.RTI_RACE_CD, ab.COVSTART, ab.OREC, ab.CREC, ab.RFRNC_YR, ab.A_MO_CNT, ab.B_MO_CNT, ab.HMO_MO,
       NVL(TRY_TO_NUMERIC(d.PLNCOVMO),0) as PTD_MO, TRY_TO_NUMERIC(d.DUAL_MO) as DUAL_MO
from mbsf_ab_cte ab
left join mbsf_d_cte d
on ab.BENE_ID = d.BENE_ID and ab.RFRNC_YR = d.RFRNC_YR
)
select r.*, 1 as IND from mbsf_abcd_reconstr r 
  union 
select BENE_ID, BENE_DOB, DEATH_DT, SEX, RTI_RACE_CD, COVSTART, OREC, CREC, RFRNC_YR, A_MO_CNT, B_MO_CNT, HMO_MO, PTD_MO, DUAL_MO, 1 as IND from MEDICARE_2014.MBSF_ABCD_SUMMARY
  union
select BENE_ID, BENE_DOB, DEATH_DT, SEX, RTI_RACE_CD, COVSTART, OREC, CREC, RFRNC_YR, A_MO_CNT, B_MO_CNT, HMO_MO, PTD_MO, DUAL_MO, 1 as IND from MEDICARE_2015.MBSF_ABCD_SUMMARY
  union
select BENE_ID, BENE_DOB, DEATH_DT, SEX, RTI_RACE_CD, COVSTART, OREC, CREC, RFRNC_YR, A_MO_CNT, B_MO_CNT, HMO_MO, PTD_MO, DUAL_MO, 1 as IND from MEDICARE_2016.MBSF_ABCD_SUMMARY
  union
select BENE_ID, BENE_DOB, DEATH_DT, SEX, RTI_RACE_CD, COVSTART, OREC, CREC, RFRNC_YR, A_MO_CNT, B_MO_CNT, HMO_MO, PTD_MO, DUAL_MO, 1 as IND from MEDICARE_2017.MBSF_ABCD_SUMMARY
;

-- summarizing coverage at patient level
create or replace table PRIVATE_ENROLLMENT as
with PARTAB_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT > 0 and B_MO_CNT > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    PARTABC_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT > 0 and B_MO_CNT > 0 and HMO_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
        PARTAC_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT > 0 and B_MO_CNT = 0 and HMO_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
        PARTBC_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT = 0 and B_MO_CNT > 0 and HMO_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
        PARTC_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT = 0 and B_MO_CNT = 0 and HMO_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    PARTABD_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT > 0 and B_MO_CNT > 0 and PTD_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    PARTAD_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT > 0 and B_MO_CNT = 0 and PTD_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    PARTBD_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT = 0 and B_MO_CNT > 0 and PTD_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    PARTD_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT = 0 and B_MO_CNT = 0 and PTD_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    ABDUAL_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT > 0 and B_MO_CNT > 0 and DUAL_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    ADUAL_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT > 0 and B_MO_CNT = 0 and DUAL_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    BDUAL_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT = 0 and B_MO_CNT > 0 and DUAL_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    ABDDUAL_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT > 0 and B_MO_CNT > 0 and PTD_MO > 0 and DUAL_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    BDDUAL_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT > 0 and B_MO_CNT = 0 and PTD_MO > 0 and DUAL_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
),
    ADDUAL_ENR_SUMMARY as (
  select BENE_ID, 
        NVL(CY2011,0)+NVL(CY2012,0)+NVL(CY2013,0)+NVL(CY2014,0)+NVL(CY2015,0)+NVL(CY2016,0)+NVL(CY2017,0) as ENR_DUR,
        NVL(CY2011,0) || NVL(CY2012,0) || NVL(CY2013,0) || NVL(CY2014,0) || NVL(CY2015,0) || NVL(CY2016,0) || NVL(CY2017,0) ENR_SEQ 
  from (select BENE_ID, IND, RFRNC_YR from PRIVATE_ENROLLMENT_YEARLY where A_MO_CNT = 0 and B_MO_CNT > 0 and PTD_MO > 0 and DUAL_MO > 0)
       pivot (max(IND)
             for RFRNC_YR in (2011, 2012, 2013, 2014, 2015, 2016, 2017))
       as p(BENE_ID,CY2011,CY2012,CY2013,CY2014,CY2015,CY2016,CY2017)
)
select ab.BENE_ID
      ,ab.ENR_DUR as AB_CVGE
      ,ab.ENR_SEQ as AB_CVGE_DETAIL
      ,abd.ENR_DUR as ABD_CVGE
      ,abd.ENR_SEQ as ABD_CVGE_DETAIL
      ,ad.ENR_DUR as AD_CVGE
      ,ad.ENR_SEQ as AD_CVGE_DETAIL
      ,bd.ENR_DUR as BD_CVGE
      ,bd.ENR_SEQ as BD_CVGE_DETAIL
      ,d.ENR_DUR as D_CVGE
      ,d.ENR_SEQ as D_CVGE_DETAIL
      ,abc.ENR_DUR as ABC_CVGE
      ,abc.ENR_SEQ as ABC_CVGE_DETAIL
      ,ac.ENR_DUR as AC_CVGE
      ,ac.ENR_SEQ as AC_CVGE_DETAIL
      ,bc.ENR_DUR as BC_CVGE
      ,bc.ENR_SEQ as BC_CVGE_DETAIL
      ,c.ENR_DUR as C_CVGE
      ,c.ENR_SEQ as C_CVGE_DETAIL
      ,abdl.ENR_DUR as ABDUAL_CVGE
      ,abdl.ENR_SEQ as ABDUAL_CVGE_DETAIL
      ,adl.ENR_DUR as ADUAL_CVGE
      ,adl.ENR_SEQ as ADUAL_CVGE_DETAIL
      ,bdl.ENR_DUR as BDUAL_CVGE
      ,bdl.ENR_SEQ as BDUAL_CVGE_DETAIL
      ,abddl.ENR_DUR as ABDDUAL_CVGE
      ,abddl.ENR_SEQ as ABDDUAL_CVGE_DETAIL
      ,addl.ENR_DUR as ADDUAL_CVGE
      ,addl.ENR_SEQ as ADDUAL_CVGE_DETAIL
      ,bddl.ENR_DUR as BDDUAL_CVGE
      ,bddl.ENR_SEQ as BDDUAL_CVGE_DETAIL
from PARTAB_ENR_SUMMARY ab
left join PARTABD_ENR_SUMMARY abd on ab.BENE_ID = abd.BENE_ID
left join PARTAD_ENR_SUMMARY ad on ab.BENE_ID = ad.BENE_ID
left join PARTBD_ENR_SUMMARY bd on ab.BENE_ID = bd.BENE_ID
left join PARTD_ENR_SUMMARY d on ab.BENE_ID = d.BENE_ID
left join PARTABC_ENR_SUMMARY abc on ab.BENE_ID = abc.BENE_ID
left join PARTAC_ENR_SUMMARY ac on ab.BENE_ID = ac.BENE_ID
left join PARTBC_ENR_SUMMARY bc on ab.BENE_ID = bc.BENE_ID
left join PARTC_ENR_SUMMARY c on ab.BENE_ID = c.BENE_ID
left join ABDUAL_ENR_SUMMARY abdl on ab.BENE_ID = abdl.BENE_ID
left join ADUAL_ENR_SUMMARY adl on ab.BENE_ID = adl.BENE_ID
left join BDUAL_ENR_SUMMARY bdl on ab.BENE_ID = bdl.BENE_ID
left join ABDDUAL_ENR_SUMMARY abddl on ab.BENE_ID = abddl.BENE_ID
left join ADDUAL_ENR_SUMMARY addl on ab.BENE_ID = addl.BENE_ID
left join BDDUAL_ENR_SUMMARY bddl on ab.BENE_ID = bddl.BENE_ID
;

create or replace table dstat_enr as
with enr_cte as (
  -- AB coverage
  select 'AB' as PART,'>=1 Year' as ENR_DURATION, count(distinct BENE_ID) as BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE >= 1
  union 
  select 'AB','>=2 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE >= 2
  union 
  select 'AB','>=3 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE >= 3
  union
  select 'AB','>=4 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE >= 4
  union
  select 'AB','>=5 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE >= 5
  union
  select 'AB','>=6 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE >= 6
  union
  select 'AB','2-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE_DETAIL like '%11%'
  union 
  select 'AB','3-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE_DETAIL like '%111%'
  union
  select 'AB','4-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE_DETAIL like '%1111%'
  union
  select 'AB','5-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE_DETAIL like '%11111%'
  union
  select 'AB','6-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE_DETAIL like '%111111%'
  union
  select 'AB','7-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AB_CVGE_DETAIL = '1111111'
  union
  -- part ABD coverage
  select 'ABD','>=1 Year', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE >= 1
  union 
  select 'ABD','>=2 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE >= 2
  union 
  select 'ABD','>=3 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE >= 3
  union
  select 'ABD','>=4 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE >= 4
  union
  select 'ABD','>=5 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE >= 5
  union
  select 'ABD','>=6 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE >= 6
  union
  select 'ABD','2-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE_DETAIL like '%11%'
  union 
  select 'ABD','3-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE_DETAIL like '%111%'
  union
  select 'ABD','4-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE_DETAIL like '%1111%'
  union
  select 'ABD','5-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE_DETAIL like '%11111%'
  union
  select 'ABD','6-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE_DETAIL like '%111111%'
  union
  select 'ABD','7-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABD_CVGE_DETAIL = '1111111'
  union
  -- part AD coverage
  select 'AD','>=1 Year', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE >= 1
  union 
  select 'AD','>=2 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE >= 2
  union 
  select 'AD','>=3 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE >= 3
  union
  select 'AD','>=4 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE >= 4
  union
  select 'AD','>=5 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE >= 5
  union
  select 'AD','>=6 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE >= 6
  union
  select 'AD','2-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE_DETAIL like '%11%'
  union 
  select 'AD','3-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE_DETAIL like '%111%'
  union
  select 'AD','4-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE_DETAIL like '%1111%'
  union
  select 'AD','5-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE_DETAIL like '%11111%'
  union
  select 'AD','6-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE_DETAIL like '%111111%'
  union
  select 'AD','7-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where AD_CVGE_DETAIL = '1111111'
  union
  -- part BD coverage
  select 'BD','>=1 Year', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE >= 1
  union 
  select 'BD','>=2 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE >= 2
  union 
  select 'BD','>=3 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE >= 3
  union
  select 'BD','>=4 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE >= 4
  union
  select 'BD','>=5 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE >= 5
  union
  select 'BD','>=6 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE >= 6
  union
  select 'BD','2-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE_DETAIL like '%11%'
  union 
  select 'BD','3-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE_DETAIL like '%111%'
  union
  select 'BD','4-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE_DETAIL like '%1111%'
  union
  select 'BD','5-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE_DETAIL like '%11111%'
  union
  select 'BD','6-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE_DETAIL like '%111111%'
  union
  select 'BD','7-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where BD_CVGE_DETAIL = '1111111'
  union
  -- part ABC coverage
  select 'ABC','>=1 Year', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE >= 1
  union 
  select 'ABC','>=2 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE >= 2
  union 
  select 'ABC','>=3 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE >= 3
  union
  select 'ABC','>=4 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE >= 4
  union
  select 'ABC','>=5 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE >= 5
  union
  select 'ABC','>=6 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE >= 6
  union
  select 'ABC','2-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE_DETAIL like '%11%'
  union 
  select 'ABC','3-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE_DETAIL like '%111%'
  union
  select 'ABC','4-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE_DETAIL like '%1111%'
  union
  select 'ABC','5-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE_DETAIL like '%11111%'
  union
  select 'ABC','6-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE_DETAIL like '%111111%'
  union
  select 'ABC','7-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABC_CVGE_DETAIL = '1111111'
  union
  -- dual eligibility
  select 'ABDual','>=1 Year', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE >= 1
  union 
  select 'ABDual','>=2 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE >= 2
  union 
  select 'ABDual','>=3 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE >= 3
  union
  select 'ABDual','>=4 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE >= 4
  union
  select 'ABDual','>=5 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE >= 5
  union
  select 'ABDual','>=6 Years (not necessarily consecutive)', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE >= 6
  union
  select 'ABDual','2-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE_DETAIL like '%11%'
  union 
  select 'ABDual','3-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE_DETAIL like '%111%'
  union
  select 'ABDual','4-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE_DETAIL like '%1111%'
  union
  select 'ABDual','5-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE_DETAIL like '%11111%'
  union
  select 'ABDual','6-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE_DETAIL like '%111111%'
  union
  select 'ABDual','7-Consecutive Year Coverage', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
  where ABDual_CVGE_DETAIL = '1111111'
  union
  -- crosswalked population
  select 'XWalk_ABD','>=1 Year', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE >= 1 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABD','>=2 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE >= 2 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABD','>=3 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE >= 3 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD','>=4 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE >= 4 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD','>=5 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE >= 5 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD','>=6 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE >= 6 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD','2-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE_DETAIL like '%11%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABD','3-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE_DETAIL like '%111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD','4-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE_DETAIL like '%1111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD','5-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE_DETAIL like '%11111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD','6-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE_DETAIL like '%111111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABD','7-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABD_CVGE_DETAIL = '1111111' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABDual','>=1 Year', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE >= 1 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABDual','>=2 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE >= 2 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABDual','>=3 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE >= 3 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABDual','>=4 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE >= 4 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABDual','>=5 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE >= 5 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABDual','>=6 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE >= 6 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABDual','2-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE_DETAIL like '%11%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABDual','3-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE_DETAIL like '%111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABDual','4-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE_DETAIL like '%1111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABDual','5-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE_DETAIL like '%11111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABDual','6-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE_DETAIL like '%111111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABDual','7-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDual_CVGE_DETAIL = '1111111' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD_Dual','>=1 Year', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE >= 1 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABD_Dual','>=2 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE >= 2 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABD_Dual','>=3 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE >= 3 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD_Dual','>=4 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE >= 4 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD_Dual','>=5 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE >= 5 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD_Dual','>=6 Years (not necessarily consecutive)', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE >= 6 and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD_Dual','2-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE_DETAIL like '%11%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABD_Dual','3-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE_DETAIL like '%111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD_Dual','4-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE_DETAIL like '%1111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD_Dual','5-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE_DETAIL like '%11111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union
  select 'XWalk_ABD_Dual','6-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE_DETAIL like '%111111%' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
  union 
  select 'XWalk_ABD_Dual','7-Consecutive Year Coverage', count(distinct e.BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT e
  where e.ABDDual_CVGE_DETAIL = '1111111' and exists (select 1 from BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw where e.BENE_ID = xw.BENE_ID and xw.pat_match = 1)
)
select * from enr_cte
pivot (sum(BENE_CNT) for PART in ('AB','ABD','AD','BD','ABC','ABDual','XWalk_ABD','XWalk_ABDual','XWalk_ABD_Dual'))
as p(ENR_DURATION, PART_AB, PART_ABD, PART_AD, PART_BD, PART_ABC, PT_DUAL_ELIGIBLE,Xwalk_ABD,XWalk_ABDual,XWalk_ABD_Dual)
order by ENR_DURATION 
;

/*Use the transformed CDM schema*/
create or replace table dstat_enr_cdm as
with ab_or_c_cte as (
    select PATID, ENR_START_DATE, ENR_END_DATE
    from CMS_PCORNET_CDM.ENROLLMENT
    where enr_basis = 'I' and raw_basis like 'AB%'
), ab_or_c_and_d_cte as (
    select a.PATID, 
           greatest(a.ENR_START_DATE,b.ENR_START_DATE) as ENR_START_DATE,
           least(a.ENR_END_DATE,b.ENR_END_DATE) as ENR_END_DATE  
    from ab_or_c_cte a 
    join CMS_PCORNET_CDM.ENROLLMENT b
    on a.patid = b.patid 
    where b.enr_basis = 'D' AND
          b.ENR_START_DATE >= a.ENR_START_DATE AND
          b.ENR_START_DATE <= a.ENR_END_DATE
), enr_cte as (
    select patid, 'ABorC' AS enr_type,
           round((ENR_END_DATE::date - ENR_START_DATE::date)/365.25) as ENR_DURATION
    from ab_or_c_cte
    union
    select patid, 'ABorCandD', 
           round((ENR_END_DATE::date - ENR_START_DATE::date)/365.25) as ENR_DURATION
    from ab_or_c_and_d_cte
), enr_cte_flag as (
    select e.patid, e.enr_type, e.enr_duration, 
           case when xw.pat_match = 1 then 1 else 0 end as xwalk_ind,
           case when als.patid is not null then 1 else 0 end as als_ind
    from enr_cte e
    left join BENE_MAPPING.UNIQUE_BENE_XWALK_2019 xw on e.patid = xw.BENE_ID
    left join als.ALS_ONE_CLAIM als on e.patid = als.patid
)
select '<1' as CoC, 'all' as summary_type, enr_type, count(distinct patid) as pat_cnt from enr_cte_flag where ENR_DURATION < 1 group by enr_type
union
select '>=1' as CoC, 'all', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=1 group by enr_type
union
select '>=2' as CoC, 'all', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=2 group by enr_type
union
select '>=3' as CoC, 'all', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=3 group by enr_type
union 
select '>=4' as CoC, 'all', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=4 group by enr_type
union
select '>=5' as CoC, 'all', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=5 group by enr_type
union
select '>=6' as CoC, 'all', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=6 group by enr_type
union
select '>=7' as CoC, 'all', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=7 group by enr_type
union
select '<1' as CoC, 'xwalk_ind'||xwalk_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION < 1 group by enr_type,xwalk_ind
union
select '>=1' as CoC, 'xwalk_ind'||xwalk_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=1 group by enr_type,xwalk_ind
union
select '>=2' as CoC, 'xwalk_ind'||xwalk_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=2 group by enr_type,xwalk_ind
union
select '>=3' as CoC, 'xwalk_ind'||xwalk_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=3 group by enr_type,xwalk_ind
union 
select '>=4' as CoC, 'xwalk_ind'||xwalk_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=4 group by enr_type,xwalk_ind
union
select '>=5' as CoC, 'xwalk_ind'||xwalk_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=5 group by enr_type,xwalk_ind
union
select '>=6' as CoC, 'xwalk_ind'||xwalk_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=6 group by enr_type,xwalk_ind
union
select '>=7' as CoC, 'xwalk_ind'||xwalk_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=7 group by enr_type,xwalk_ind
union
select '<1' as CoC, 'als_ind'||als_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION < 1 group by enr_type,als_ind
union
select '>=1' as CoC, 'als_ind'||als_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=1 group by enr_type,als_ind
union
select '>=2' as CoC, 'als_ind'||als_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=2 group by enr_type,als_ind
union
select '>=3' as CoC, 'als_ind'||als_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=3 group by enr_type,als_ind
union 
select '>=4' as CoC, 'als_ind'||als_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=4 group by enr_type,als_ind
union
select '>=5' as CoC, 'als_ind'||als_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=5 group by enr_type,als_ind
union
select '>=6' as CoC, 'als_ind'||als_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=6 group by enr_type,als_ind
union
select '>=7' as CoC, 'als_ind'||als_ind, enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=7 group by enr_type,als_ind
union
select '<1' as CoC, 'xwalk_als', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION < 1 and als_ind = 1 and xwalk_ind = 1 group by enr_type
union
select '>=1' as CoC, 'xwalk_als', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=1 and als_ind = 1 and xwalk_ind = 1 group by enr_type
union
select '>=2' as CoC, 'xwalk_als', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=2 and als_ind = 1 and xwalk_ind = 1 group by enr_type
union
select '>=3' as CoC, 'xwalk_als', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=3 and als_ind = 1 and xwalk_ind = 1 group by enr_type
union 
select '>=4' as CoC, 'xwalk_als', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=4 and als_ind = 1 and xwalk_ind = 1 group by enr_type
union
select '>=5' as CoC, 'xwalk_als', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=5 and als_ind = 1 and xwalk_ind = 1 group by enr_type
union
select '>=6' as CoC, 'xwalk_als', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=6 and als_ind = 1 and xwalk_ind = 1 group by enr_type
union
select '>=7' as CoC, 'xwalk_als', enr_type, count(distinct patid) from enr_cte_flag where ENR_DURATION >=7 and als_ind = 1 and xwalk_ind = 1 group by enr_type
;

create or replace table dstat_enr_sex as
select d.SEX, 
       round((e.ENR_END_DATE::date - e.ENR_START_DATE::date)/365.25) as ENR_DURATION,
       count(distinct d.PATID) as PAT_CNT
from CMS_PCORNET_CDM.ENROLLMENT e
join CMS_PCORNET_CDM.DEMOGRAPHIC d
on e.PATID = d.PATID
group by SEX, ENR_DURATION
order by SEX, ENR_DURATION
;

create or replace table dstat_enr_race as
select d.RACE, 
       round((e.ENR_END_DATE::date - e.ENR_START_DATE::date)/365.25) as ENR_DURATION,
       count(distinct d.PATID) as PAT_CNT
from CMS_PCORNET_CDM.ENROLLMENT e
join CMS_PCORNET_CDM.DEMOGRAPHIC d
on e.PATID = d.PATID
group by RACE, ENR_DURATION
order by RACE, ENR_DURATION
;

select * from dstat_enr;
select * from dstat_enr_cdm;
select * from dstat_enr_race;
select * from dstat_enr_sex;
