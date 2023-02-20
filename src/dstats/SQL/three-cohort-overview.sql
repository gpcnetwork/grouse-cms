create or replace table GPC_TABLE1 (
    PATID varchar(50) NOT NULL,
    BIRTH_DATE date,
    INDEX_DATE date,      -- date of first encounter/current date
    AGE_AT_INDEX integer, 
    AGEGRP_AT_INDEX varchar(10),
    SEX varchar(3),
    RACE varchar(6),
    HISPANIC varchar(20),
    SITE varchar(10),
    CMS_ENR_IND integer,
    CMS_AB_CONT5YR integer,
    CMS_D_CONT5YR integer
);

create or replace table ALS_TABLE1 (
    PATID varchar(50) NOT NULL,
    BIRTH_DATE date,
    INDEX_DATE date,      -- date of first ALS dx
    AGE_AT_INDEX integer,
    AGEGRP_AT_INDEX varchar(10),
    SEX varchar(3),
    RACE varchar(6),
    HISPANIC varchar(20),
    SITE varchar(10),
    CMS_ENR_IND integer,
    CMS_AB_CONT5YR integer,
    CMS_D_CONT5YR integer
);

create or replace table BC_TABLE1 (
    PATID varchar(50) NOT NULL,
    BIRTH_DATE date,
    INDEX_DATE date,      -- date of first breast cancer dx
    AGE_AT_INDEX integer,
    AGEGRP_AT_INDEX varchar(10),
    SEX varchar(3),
    RACE varchar(6),
    HISPANIC varchar(20),
    SITE varchar(10),
    CMS_ENR_IND integer,
    CMS_AB_CONT5YR integer,
    CMS_D_CONT5YR integer
);

create or replace table WT_TABLE_LONG (
    PATID varchar(50) NOT NULL,
    MEASURE_DATE date,      -- date of first HT/WT/BMI record
    AGE_AT_MEASURE integer,
    MEASURE_TYPE varchar(4),
    MEASURE_NUM double, -- ht:m; wt:kg
    SITE varchar(10),
    SRC_TABLE varchar(10)
);

/*stored procedure to collect overall GPC cohort*/
create or replace procedure get_gpc_table1(SITES array)
returns variant
language javascript
as
$$
/**
 * Stored procedure to collect a Table 1 for overall GPC cohort
 * @param {array} SITES: an array of site acronyms (matching schema name suffix) - not include CMS
*/
var i;
for(i=0; i<SITES.length; i++){
    var site = SITES[i].toString();
    var site_cdm = 'PCORNET_CDM_' + site;
    
    // dynamic query
    var sqlstmt_par = `INSERT INTO GPC_TABLE1
                       WITH cte_enc_age AS (
                         SELECT d.patid,
                                d.birth_date,
                                -- e.admit_date::date as index_date,
                                -- round(datediff(day,d.birth_date::date,e.admit_date::date)/365.25) AS age_at_index,
                                current_date as index_date,
                                round(datediff(day,d.birth_date::date,current_date)/365.25) AS age_at_index,
                                d.sex, 
                                CASE WHEN d.race IN ('05') THEN 'white' 
                                     WHEN d.race IN ('03') THEN 'black'
                                     WHEN d.race IN ('NI','UN',NULL) THEN 'NI'
                                     ELSE 'ot' END AS race, 
                                CASE WHEN d.hispanic = 'Y' THEN 'hispanic' 
                                     WHEN d.hispanic = 'N' THEN 'non-hispanic' 
                                     WHEN d.hispanic IN ('NI','UN',NULL) THEN 'NI'
                                     ELSE 'ot' END AS hispanic,
                                -- row_number() over (partition by e.patid order by e.admit_date::date) rn
                                row_number() over (partition by d.patid order by current_date) rn
                         FROM `+ site_cdm +`.LDS_DEMOGRAPHIC d 
                         -- JOIN `+ site_cdm +`.LDS_ENCOUNTER e ON d.PATID = e.PATID
                         -- WHERE e.ENC_TYPE not in ('NI','UN','OT') and e.ENC_TYPE is not null
                         ),  cte_enr AS (
                            SELECT patid, 1 AS CMS_ENR_IND
                            FROM CMS_PCORNET_CDM.LDS_enrollment
                            GROUP BY patid
                         ), cte_ab_enr AS (
                            SELECT patid, 1 AS CMS_AB_CONT5YR
                            FROM CMS_PCORNET_CDM.LDS_enrollment 
                            WHERE raw_basis like 'AB%' AND DATEDIFF(day,enr_start_date,enr_end_date) >= 365.25*5
                            GROUP BY patid
                         ),  cte_d_enr AS (
                            SELECT patid, 1 as CMS_D_CONT5YR
                            FROM CMS_PCORNET_CDM.LDS_enrollment
                            WHERE enr_basis = 'D' AND DATEDIFF(day,enr_start_date,enr_end_date) >= 365.25*5
                            GROUP BY patid
                         )
                         SELECT DISTINCT
                                cte.patid
                               ,cte.birth_date
                               ,cte.index_date
                               ,cte.age_at_index
                               ,case when cte.age_at_index < 19 then 'agegrp1'
                                     when cte.age_at_index >= 19 and cte.age_at_index < 24 then 'agegrp2'
                                     when cte.age_at_index >= 25 and cte.age_at_index < 85 then 'agegrp' || (floor((cte.age_at_index - 25)/5) + 3)
                                     else 'agegrp15' end as agegrp_at_index
                               ,cte.sex
                               ,cte.race
                               ,cte.hispanic
                               ,'`+ site +`' AS site
                               ,NVL(enr.CMS_ENR_IND,0)
                               ,NVL(ab.CMS_AB_CONT5YR,0)
                               ,NVL(d.CMS_D_CONT5YR,0)
                         FROM cte_enc_age cte
                         LEFT JOIN cte_enr enr on enr.patid = cte.patid
                         LEFT JOIN cte_ab_enr ab on ab.patid = cte.patid
                         LEFT JOIN cte_d_enr d on d.patid = cte.patid
                         WHERE cte.rn = 1;`;
    
    // run query
    var sqlstmt_run = snowflake.createStatement({sqlText:sqlstmt_par});
    sqlstmt_run.execute(); 
}
$$
;

truncate GPC_TABLE1;
call get_gpc_table1(array_construct(
     'ALLINA'
    ,'IHC'
    ,'KUMC'
    ,'MCRI'
    ,'MCW'
    ,'MU'
    ,'UIOWA'
    ,'UNMC'
    ,'UTHOUSTON'
    ,'UTHSCSA'
    ,'UTSW'
    ,'UU'
    ,'WASHU'
));


/*stored procedure to identify ALS cohort*/
create or replace procedure get_als_table1(SITES array)
returns variant
language javascript
as
$$
/**
 * Stored procedure to collect a Table 1 for ALS cohort identifier by:
 *  - ICD9: 335.20 or; 
 *  - ICD10: I12.21
 * @param {array} SITES: an array of site acronyms (matching schema name suffix) - include CMS
*/
var i;
for(i=0; i<SITES.length; i++){
    var site = SITES[i].toString();
    var site_cdm = (site === 'CMS') ? 'CMS_PCORNET_CDM' : 'PCORNET_CDM_' + site;
    
    // dynamic query
    var sqlstmt_par = `INSERT INTO ALS_TABLE1
                        WITH cte AS (
                            SELECT a.patid,
                                   b.birth_date, 
                                   MIN(NVL(a.dx_date,a.admit_date)) AS index_date,
                                   datediff(day,b.birth_date,MIN(NVL(a.dx_date,a.admit_date)))/365.25 AS age_at_index, 
                                   b.sex, 
                                   CASE WHEN b.race IN ('05') THEN 'white' 
                                        WHEN b.race IN ('03') THEN 'black'
                                        WHEN b.race IN ('NI','UN',NULL) THEN 'NI'
                                        ELSE 'ot' END AS race, 
                                   CASE WHEN b.hispanic = 'Y' THEN 'hispanic' 
                                        WHEN b.hispanic = 'N' THEN 'non-hispanic' 
                                        WHEN b.hispanic IN ('NI','UN',NULL) THEN 'NI'
                                        ELSE 'ot' END AS hispanic, 
                                   MAX(NVL(gpc.CMS_ENR_IND,0)) AS CMS_ENR_IND,
                                   MAX(NVL(gpc.CMS_AB_CONT5YR,0)) AS CMS_AB_CONT5YR,
                                   MAX(NVL(gpc.CMS_D_CONT5YR,0)) AS CMS_D_CONT5YR
                            FROM `+ site_cdm +`.LDS_DIAGNOSIS a
                            JOIN `+ site_cdm +`.LDS_DEMOGRAPHIC b ON a.patid = b.patid
                            LEFT JOIN GPC_TABLE1 gpc ON a.patid = gpc.patid
                            WHERE a.dx LIKE '335.2%' OR a.dx LIKE 'I12.2%'
                            GROUP BY a.patid, b.birth_date, b.sex, b.race, b.hispanic
                        )
                        SELECT  DISTINCT
                                cte.patid
                               ,cte.birth_date
                               ,cte.index_date
                               ,cte.age_at_index
                               ,case when cte.age_at_index < 19 then 'agegrp1'
                                     when cte.age_at_index >= 19 and cte.age_at_index < 24 then 'agegrp2'
                                     when cte.age_at_index >= 25 and cte.age_at_index < 85 then 'agegrp' || (floor((cte.age_at_index - 25)/5) + 3)
                                     else 'agegrp15' end as agegrp_at_index
                               ,cte.sex
                               ,cte.race
                               ,cte.hispanic
                               ,'`+ site +`' AS site
                               ,cms_enr_ind
                               ,cms_ab_cont5yr
                               ,cms_d_cont5yr
                        FROM cte;`;

    // run query
    var sqlstmt_run = snowflake.createStatement({sqlText:sqlstmt_par});
    sqlstmt_run.execute();
}
$$
;

truncate ALS_TABLE1;
call get_als_table1(array_construct(
     'CMS'
    ,'ALLINA'
    ,'IHC'
    ,'KUMC'
    ,'MCRI'
    ,'MCW'
    ,'MU'
    ,'UIOWA'
    ,'UNMC'
    ,'UTHOUSTON'
    ,'UTHSCSA'
    ,'UTSW'
    ,'UU'
    ,'WASHU'
));


/*stored procedure to identify Breast Cancer cohort*/
create or replace procedure get_bc_table1(SITES array)
returns variant
language javascript
as
$$
/**
 * Stored procedure to collect a Table 1 for BC cohort identifier by:
 *  - ICD9: 174 or 233.0; 
 *  - ICD10: C50
 * @param {array} SITES: an array of site acronyms (matching schema name suffix) - include CMS
*/
var i;
for(i=0; i<SITES.length; i++){
    var site = SITES[i].toString();
    var site_cdm = (site === 'CMS') ? 'CMS_PCORNET_CDM' : 'PCORNET_CDM_' + site;
    
    // dynamic query
    var sqlstmt_par = `INSERT INTO BC_TABLE1
                        WITH cte AS (
                            SELECT a.patid, 
                                   b.birth_date, 
                                   MIN(NVL(a.dx_date,a.admit_date)) AS index_date,
                                   datediff(day,b.birth_date,MIN(NVL(a.dx_date,a.admit_date)))/365.25 AS age_at_index, 
                                   b.sex, 
                                   CASE WHEN b.race IN ('05') THEN 'white' 
                                        WHEN b.race IN ('03') THEN 'black'
                                        WHEN b.race IN ('NI','UN',NULL) THEN 'NI'
                                        ELSE 'ot' END AS race, 
                                   CASE WHEN b.hispanic = 'Y' THEN 'hispanic' 
                                        WHEN b.hispanic = 'N' THEN 'non-hispanic' 
                                        WHEN b.hispanic IN ('NI','UN',NULL) THEN 'NI'
                                        ELSE 'ot' END AS hispanic,  
                                   MAX(NVL(gpc.CMS_ENR_IND,0)) AS CMS_ENR_IND,
                                   MAX(NVL(gpc.CMS_AB_CONT5YR,0)) AS CMS_AB_CONT5YR,
                                   MAX(NVL(gpc.CMS_D_CONT5YR,0)) AS CMS_D_CONT5YR
                            FROM `+ site_cdm +`.LDS_DIAGNOSIS a
                            JOIN `+ site_cdm +`.LDS_DEMOGRAPHIC b ON a.patid = b.patid
                            LEFT JOIN GPC_TABLE1 gpc ON b.patid = gpc.patid
                            WHERE a.dx LIKE '174%' OR a.dx LIKE '233.0%' OR a.dx LIKE 'C50%'
                            GROUP BY a.patid, b.birth_date, b.sex, b.race, b.hispanic
                        )
                        SELECT  DISTINCT
                                cte.patid
                               ,cte.birth_date
                               ,cte.index_date
                               ,cte.age_at_index
                               ,case when cte.age_at_index < 19 then 'agegrp1'
                                     when cte.age_at_index >= 19 and cte.age_at_index < 24 then 'agegrp2'
                                     when cte.age_at_index >= 25 and cte.age_at_index < 85 then 'agegrp' || (floor((cte.age_at_index - 25)/5) + 3)
                                     else 'agegrp15' end as agegrp_at_index
                               ,cte.sex
                               ,cte.race
                               ,cte.hispanic
                               ,'`+ site +`' AS site
                               ,cms_enr_ind
                               ,cms_ab_cont5yr
                               ,cms_d_cont5yr
                        FROM cte;`;

    // run query
    var sqlstmt_run = snowflake.createStatement({sqlText:sqlstmt_par});
    sqlstmt_run.execute();  
}
$$
;

truncate BC_TABLE1;
call get_bc_table1(array_construct(
     'CMS'
    ,'ALLINA'
    ,'IHC'
    ,'KUMC'
    ,'MCRI'
    ,'MCW'
    ,'MU'
    ,'UIOWA'
    ,'UNMC'
    ,'UTHOUSTON'
    ,'UTHSCSA'
    ,'UTSW'
    ,'UU'
    ,'WASHU'
));

/*stored procedure to identify WeighT cohort*/
create or replace procedure get_wt_table_long(SITES array)
returns variant
language javascript
as
$$
/**
 * Stored procedure to collect a Table 1 for weight cohort identifier by:
 *  - height and weight pair OR an original_bmi record
 * @param {array} SITES: an array of site acronyms (matching schema name suffix) - not include CMS
*/
var i;
for(i=0; i<SITES.length; i++){
    // parameter
    var site = SITES[i].toString();
    var site_cdm = 'PCORNET_CDM_' + site;
    
    // dynamic query
    var sqlstmt_par = `INSERT INTO WT_TABLE_LONG 
                       -- height --
                       SELECT a.patid,b.measure_date::date,
                              round(datediff(day,a.birth_date,b.measure_date::date)/365.25),
                              'HT',b.ht/39.37,'`+ site +`','VITAL' -- default at 'in'
                       FROM GPC_TABLE1 a
                       JOIN `+ site_cdm +`.lds_vital b ON a.patid = b.patid
                       WHERE b.ht is not null
                       UNION
                       select a.PATID,oc.OBSCLIN_START_DATE::date,
                              round(datediff(day,a.birth_date,oc.OBSCLIN_START_DATE::date)/365.25),'HT',
                              case when lower(oc.OBSCLIN_RESULT_UNIT) like '%cm%' then oc.OBSCLIN_RESULT_NUM/1000
                                   else oc.OBSCLIN_RESULT_NUM/39.37 end,
                              '`+ site +`','OBSCLIN'
                       FROM GPC_TABLE1 a
                       JOIN `+ site_cdm +`.deid_obs_clin oc ON a.patid = oc.patid AND
                            oc.OBSCLIN_TYPE = 'LC' and oc.OBSCLIN_CODE = '8302-2'
                       UNION
                       -- weight --
                       SELECT a.patid,b.measure_date::date,
                              round(datediff(day,a.birth_date,b.measure_date::date)/365.25),
                              'WT',b.wt/2.205,'`+ site +`','VITAL' -- default at 'lb'
                       FROM GPC_TABLE1 a
                       JOIN `+ site_cdm +`.deid_vital b ON a.patid = b.patid
                       WHERE b.wt is not null
                       UNION
                       select a.PATID,oc.OBSCLIN_START_DATE::date,
                              round(datediff(day,a.birth_date,oc.OBSCLIN_START_DATE::date)/365.25),'WT',
                              case when lower(oc.OBSCLIN_RESULT_UNIT) like 'g%' then oc.OBSCLIN_RESULT_NUM/1000
                                   when lower(oc.OBSCLIN_RESULT_UNIT) like '%kg%' then oc.OBSCLIN_RESULT_NUM
                                   else oc.OBSCLIN_RESULT_NUM/2.205 end,
                              '`+ site +`','OBSCLIN'
                       FROM GPC_TABLE1 a
                       JOIN `+ site_cdm +`.deid_obs_clin oc ON a.patid = oc.patid AND
                            oc.OBSCLIN_TYPE = 'LC' and oc.OBSCLIN_CODE = '29463-7'
                       UNION
                       -- bmi --
                       SELECT a.patid,b.measure_date::date,
                              round(datediff(day,a.birth_date,b.measure_date::date)/365.25),
                              'BMI',b.ORIGINAL_BMI,'`+ site +`','VITAL'
                       FROM GPC_TABLE1 a
                       JOIN `+ site_cdm +`.deid_vital b ON a.patid = b.patid
                       WHERE b.ORIGINAL_BMI is not null AND a.site = '`+ site +`'
                       UNION
                       select a.PATID,oc.OBSCLIN_START_DATE::date,
                              round(datediff(day,a.birth_date,oc.OBSCLIN_START_DATE::date)/365.25),
                              'BMI',oc.OBSCLIN_RESULT_NUM,'`+ site +`','OBSCLIN'   
                       FROM GPC_TABLE1 a
                       JOIN `+ site_cdm +`.deid_obs_clin oc ON a.patid = oc.patid AND
                            oc.OBSCLIN_TYPE = 'LC' and oc.OBSCLIN_CODE = '39156-5'
                       ;`;

    // run query
    var sqlstmt_run = snowflake.createStatement({sqlText:sqlstmt_par});
    sqlstmt_run.execute();
}
$$
;

truncate WT_TABLE_LONG;
call get_wt_table_long(array_construct(
     'ALLINA'
    ,'IHC'
    ,'KUMC'
    ,'MCRI'
    ,'MCW'
    ,'MU'
    ,'UIOWA'
    ,'UNMC'
    ,'UTHOUSTON'
    ,'UTHSCSA'
    ,'UTSW'
    ,'UU'
    ,'WASHU'
));

create or replace table WT_TABLE1 as
with daily_agg as(
    select patid,measure_date,age_at_measure,HT,WT,
           case when BMI>100 then NULL else BMI end as BMI,
           case when round(WT/(HT*HT))>100 then NULL else round(WT/(HT*HT)) end as BMI_CALCULATED
    from (
        select patid, 
               measure_type, 
               measure_date, 
               age_at_measure, 
               median(measure_num) as measure_num
    from WT_TABLE_LONG
    group by patid, measure_type, measure_date,age_at_measure
    ) 
    pivot(
        median(measure_num) 
        for measure_type in ('HT','WT','BMI')
    ) as p(patid,measure_date,age_at_measure,HT,WT,BMI)
    where WT is not null and HT is not null and WT>0 and HT>0
), pat_ord as(
    select patid,measure_date,age_at_measure,
           ht,wt,NVL(bmi_calculated,bmi) as bmi,
           row_number() over (partition by patid order by measure_date) as rn
    from daily_agg
    where NVL(BMI,BMI_CALCULATED) is not null and NVL(BMI,BMI_CALCULATED)>0
)
select a.patid,
       b.birth_date,
       a.measure_date as index_date,
       a.age_at_measure as age_at_index,
       a.ht,
       a.wt,
       a.bmi,
       b.sex, 
       CASE WHEN b.race IN ('05') THEN 'white' 
            WHEN b.race IN ('03') THEN 'black'
            WHEN b.race IN ('NI','UN',NULL) THEN 'NI'
            ELSE 'ot' END AS race, 
       CASE WHEN b.hispanic = 'Y' THEN 'hispanic' 
            WHEN b.hispanic = 'N' THEN 'non-hispanic' 
            WHEN b.hispanic IN ('NI','UN',NULL) THEN 'NI'
            ELSE 'ot' END AS hispanic,
       case when a.age_at_measure < 19 then 'agegrp1'
            when a.age_at_measure >= 19 and a.age_at_measure < 24 then 'agegrp2'
            when a.age_at_measure >= 25 and a.age_at_measure < 85 then 'agegrp' || (floor((a.age_at_measure - 25)/5) + 3)
            else 'agegrp15' end as agegrp_at_index,
       MAX(NVL(b.CMS_ENR_IND,0)) AS CMS_ENR_IND,
       MAX(NVL(b.CMS_AB_CONT5YR,0)) AS CMS_AB_CONT5YR,
       MAX(NVL(b.CMS_D_CONT5YR,0)) AS CMS_D_CONT5YR
from pat_ord a
join GPC_TABLE1 b on a.patid = b.patid
where a.rn = 1
group by a.patid,b.birth_date,a.measure_date,a.age_at_measure,a.ht,a.wt,a.bmi,b.sex,b.race,b.hispanic
;


/*create summary container*/
create or replace table DSTAT_COHORT(
    SITE varchar(10),
    COHORT varchar(4),
    DATA_COVERAGE varchar(20),
    SUMM_VAR varchar(20),
    SUMM_CAT varchar(20),
    SUMM_CNT integer
);

/*get quick summaries*/
create or replace procedure get_xwalk_summ(COHORT string)
returns variant
language javascript
as
$$
/**
 * Stored procedure to collect summarization tally integrating all sites 
 * @param {array} COHORT: one of the 4 pre-defined cohorts (GPC, ALS, WT, BC)
 * Dependency: Table1 for each cohort has been created
*/
// dynamic query
var insert_summ = `INSERT INTO DSTAT_COHORT            
                    -------------------overall-xwalk---------------------------------------
                    SELECT 'GPC','`+ COHORT +`','XWALK','N','N',COUNT(DISTINCT gpc.PATID)
                    FROM `+ COHORT +`_TABLE1 gpc
                    WHERE gpc.CMS_ENR_IND = 1
                    UNION
                    SELECT 'GPC','`+ COHORT +`','XWALK_AB5YR','N','N',COUNT(DISTINCT gpc.PATID)
                    FROM `+ COHORT +`_TABLE1 gpc
                    WHERE gpc.CMS_AB_CONT5YR = 1
                    UNION
                    SELECT 'GPC','`+ COHORT +`','XWALK_D5YR','N','N',COUNT(DISTINCT gpc.PATID)
                    FROM `+ COHORT +`_TABLE1 gpc
                    WHERE gpc.CMS_D_CONT5YR = 1
                    UNION                  
                    -------------------by site-----------------------------------------
                    SELECT gpc.site,'`+ COHORT +`','XWALK','N','N',COUNT(DISTINCT gpc.PATID)
                    FROM GPC_TABLE1 gpc
                    WHERE gpc.CMS_ENR_IND = 1 AND
                          EXISTS (SELECT 1 from `+ COHORT +`_TABLE1 cohort WHERE gpc.patid = cohort.patid)
                    GROUP BY gpc.site
                    UNION
                    SELECT gpc.site,'`+ COHORT +`','XWALK_AB5YR','N','N',COUNT(DISTINCT gpc.PATID)
                    FROM GPC_TABLE1 gpc
                    WHERE gpc.CMS_AB_CONT5YR = 1 AND
                          EXISTS (SELECT 1 from `+ COHORT +`_TABLE1 cohort WHERE gpc.patid = cohort.patid)
                    GROUP BY gpc.site
                    UNION
                    SELECT gpc.site,'`+ COHORT +`','XWALK_D5YR','N','N',COUNT(DISTINCT gpc.PATID)
                    FROM GPC_TABLE1 gpc
                    WHERE gpc.CMS_D_CONT5YR = 1 AND
                          EXISTS (SELECT 1 from `+ COHORT +`_TABLE1 cohort WHERE gpc.patid = cohort.patid)
                    GROUP BY gpc.site
                    UNION
                    -------------------overall-xwalk-demo-----------------------------------------
                    SELECT 'GPC','`+ COHORT +`','XWALK','agegrp_at_index',gpc.agegrp_at_index,COUNT(DISTINCT gpc.PATID)
                    FROM `+ COHORT +`_TABLE1 gpc
                    WHERE gpc.CMS_ENR_IND = 1
                    GROUP BY gpc.agegrp_at_index
                    UNION
                    SELECT 'GPC','`+ COHORT +`','XWALK','sex',gpc.sex,COUNT(DISTINCT gpc.PATID)
                    FROM `+ COHORT +`_TABLE1 gpc
                    WHERE gpc.CMS_ENR_IND = 1
                    GROUP by gpc.sex
                    UNION
                    SELECT 'GPC','`+ COHORT +`','XWALK','race',gpc.race,COUNT(DISTINCT gpc.PATID)
                    FROM `+ COHORT +`_TABLE1 gpc
                    WHERE gpc.CMS_ENR_IND = 1
                    GROUP BY gpc.race
                    UNION
                    SELECT 'GPC','`+ COHORT +`','XWALK','hispanic',gpc.hispanic,COUNT(DISTINCT gpc.PATID) 
                    FROM `+ COHORT +`_TABLE1 gpc
                    WHERE gpc.CMS_ENR_IND = 1
                    GROUP by gpc.hispanic
                    UNION
                    -------------------by site-xwalk-demo-----------------------------------------
                    SELECT gpc.site,'`+ COHORT +`','XWALK','agegrp_at_index',gpc.agegrp_at_index,COUNT(DISTINCT gpc.PATID)
                    FROM GPC_TABLE1 gpc 
                    WHERE gpc.CMS_ENR_IND = 1 AND
                          EXISTS (SELECT 1 from `+ COHORT +`_TABLE1 cohort WHERE gpc.patid = cohort.patid)
                    GROUP BY gpc.site,gpc.agegrp_at_index
                    UNION
                    SELECT gpc.site,'`+ COHORT +`','XWALK','sex',gpc.sex,COUNT(DISTINCT gpc.PATID)
                    FROM GPC_TABLE1 gpc 
                    WHERE gpc.CMS_ENR_IND = 1 AND
                          EXISTS (SELECT 1 from `+ COHORT +`_TABLE1 cohort WHERE gpc.patid = cohort.patid)
                    GROUP BY gpc.site,gpc.sex
                    UNION
                    SELECT gpc.site,'`+ COHORT +`','XWALK','race',gpc.race,COUNT(DISTINCT gpc.PATID) 
                    FROM GPC_TABLE1 gpc 
                    WHERE gpc.CMS_ENR_IND = 1 AND
                          EXISTS (SELECT 1 from `+ COHORT +`_TABLE1 cohort WHERE gpc.patid = cohort.patid)
                    GROUP BY gpc.site,gpc.race
                    UNION
                    SELECT gpc.site,'`+ COHORT +`','XWALK','hispanic',gpc.hispanic,COUNT(DISTINCT gpc.PATID)
                    FROM GPC_TABLE1 gpc 
                    WHERE gpc.CMS_ENR_IND = 1 AND
                          EXISTS (SELECT 1 from `+ COHORT +`_TABLE1 cohort WHERE gpc.patid = cohort.patid)
                    GROUP BY gpc.site,gpc.hispanic
                    ;`; 
// run query
var insert_summ_run = snowflake.createStatement({sqlText:insert_summ});
insert_summ_run.execute(); 
$$
;

truncate DSTAT_COHORT;
call get_xwalk_summ('GPC');
call get_xwalk_summ('ALS');
call get_xwalk_summ('BC');
call get_xwalk_summ('WT');


-- add medicare population info
INSERT INTO DSTAT_COHORT
with rec_addr as (
    SElECT patid, address_state, 
           min(address_period_start) over (partition by patid order by address_period_end) as enroll_start_date,
           row_number() over (partition by patid order by address_period_end desc) as rn
    FROM CMS_PCORNET_CDM.LDS_LDS_ADDRESS_HISTORY
),  rec_addr_demo as (
    SELECT a.patid, a.address_state,
           b.sex, 
           CASE WHEN b.race IN ('05') THEN 'white' 
                WHEN b.race IN ('03') THEN 'black'
                WHEN b.race IN ('NI','UN',NULL) THEN 'NI'
                ELSE 'ot' END AS race, 
           CASE WHEN b.hispanic = 'Y' THEN 'hispanic' 
                WHEN b.hispanic = 'N' THEN 'non-hispanic' 
                WHEN b.hispanic IN ('NI','UN',NULL) THEN 'NI'
                ELSE 'ot' END AS hispanic,
           round(datediff(day,b.birth_date,a.enroll_start_date)/365.25) as age_at_enroll
    FROM rec_addr a
    JOIN CMS_PCORNET_CDM.LDS_DEMOGRAPHIC b 
    ON a.patid = b.patid
    WHERE a.rn = 1
),  regrp_age as (
    SELECT a.*, 
    case when a.age_at_enroll < 19 then 'agegrp1'
         when a.age_at_enroll >= 19 and a.age_at_enroll < 24 then 'agegrp2'
         when a.age_at_enroll >= 25 and a.age_at_enroll < 85 then 'agegrp' || (floor((a.age_at_enroll - 25)/5) + 3)
         else 'agegrp15' end as agegrp_at_enroll
    from rec_addr_demo a
)
select 'CMS', 'CMS', 'CMS', 'N','N',count(distinct patid)
from rec_addr
union
select  'CMS', 'CMS', address_state, 'N','N',count(distinct patid)
from rec_addr
group by address_state
union
SELECT 'CMS','CMS','CMS','sex',sex,COUNT(DISTINCT PATID) 
FROM regrp_age 
GROUP BY sex
union
SELECT 'CMS','CMS','CMS','race',race,COUNT(DISTINCT PATID) 
FROM regrp_age 
GROUP BY race
union
SELECT 'CMS','CMS','CMS','hispanic',hispanic,COUNT(DISTINCT PATID) 
FROM regrp_age 
GROUP BY hispanic
union
SELECT 'CMS','CMS','CMS','agegrp_at_enroll',agegrp_at_enroll,COUNT(DISTINCT PATID) 
FROM regrp_age 
GROUP BY agegrp_at_enroll
union
select  'CMS', 'CMS', address_state,'sex',sex,count(distinct patid)
from regrp_age
group by address_state,sex
union
select  'CMS', 'CMS', address_state,'race',race,count(distinct patid)
from regrp_age
group by address_state,race
union
select  'CMS', 'CMS', address_state,'hispanic',hispanic,count(distinct patid)
from regrp_age
group by address_state,hispanic
union
select  'CMS', 'CMS', address_state,'agegrp_at_enroll',agegrp_at_enroll,count(distinct patid)
from regrp_age
group by address_state,agegrp_at_enroll
;
