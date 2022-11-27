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
;


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
)
select * from enr_cte
pivot (sum(BENE_CNT) for PART in ('AB','ABD','AD','BD','ABC','ABDual'))
as p(ENR_DURATION, PART_AB, PART_ABD, PART_AD, PART_BD, PART_ABC, DUAL_ELIGIBLE)
order by ENR_DURATION 
;

-- edge cases
select 'AC','>=1 Year', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
where AC_CVGE >= 1
union 
select 'BC','>=1 Year', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
where BC_CVGE >= 1
union 
select 'C','>=1 Year', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
where C_CVGE >= 1
union 
select 'D','>=1 Year', count(distinct BENE_ID) BENE_CNT from PRIVATE_ENROLLMENT
where D_CVGE >= 1
;