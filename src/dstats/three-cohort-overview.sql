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
    CMS_D_CONT5YR integer,
    constraint pk primary key(PATID)
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
    CMS_D_CONT5YR integer,
    constraint pk primary key(PATID)
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
    CMS_D_CONT5YR integer,
    constraint pk primary key(PATID)
);

create or replace table WT_TABLE1 (
    PATID varchar(50) NOT NULL,
    BIRTH_DATE date,
    INDEX_DATE date,      -- date of first BMI record
    AGE_AT_INDEX integer,
    AGEGRP_AT_INDEX varchar(10),
    OBESE_IND integer,             -- BMI ever >= 30
    BMI_CNT integer,
    BMI_DATE_RANGE integer,
    SEX varchar(3),
    RACE varchar(6),
    HISPANIC varchar(20),
    SITE varchar(10),
    CMS_ENR_IND integer,
    CMS_AB_CONT5YR integer,
    CMS_D_CONT5YR integer,
    constraint pk primary key(PATID)
);

/*create summary container*/
create or replace table DSTAT_DENOM(
    SITE varchar(10),
    COHORT varchar(4),
    SOURCE varchar(20),
    SUMM_VAR varchar(10),
    SUMM_CAT varchar(10),
    SUMM_CNT integer,
    SUMM_PROP double
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
                                     when cte.age_at_index >= 25 and cte.age_at_index < 85 then 'agegrp' || floor((cte.age_at_index - 25)/5) + 3
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
                            WHERE (a.dx LIKE '335.20%' OR a.dx LIKE 'I12.21%') AND
                                   a.dx_date >= b.birth_date
                            GROUP BY a.patid, b.birth_date, b.sex, b.race, b.hispanic
                        )
                        SELECT  DISTINCT
                                cte.patid
                               ,cte.birth_date
                               ,cte.index_date
                               ,cte.age_at_index
                               ,case when cte.age_at_index < 19 then 'agegrp1'
                                     when cte.age_at_index >= 19 and cte.age_at_index < 24 then 'agegrp2'
                                     when cte.age_at_index >= 25 and cte.age_at_index < 85 then 'agegrp' || floor((cte.age_at_index - 25)/5) + 3
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
                            WHERE (a.dx LIKE '174%' OR a.dx LIKE '233.0%' OR a.dx LIKE 'C50%' )AND
                                   a.dx_date >= b.birth_date
                            GROUP BY a.patid, b.birth_date, b.sex, b.race, b.hispanic
                        )
                        SELECT  DISTINCT
                                cte.patid
                               ,cte.birth_date
                               ,cte.index_date
                               ,cte.age_at_index
                               ,case when cte.age_at_index < 19 then 'agegrp1'
                                     when cte.age_at_index >= 19 and cte.age_at_index < 24 then 'agegrp2'
                                     when cte.age_at_index >= 25 and cte.age_at_index < 85 then 'agegrp' || floor((cte.age_at_index - 25)/5) + 3
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
create or replace procedure get_wt_table1(SITES array)
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
    var sqlstmt_par = `INSERT INTO WT_TABLE1 
                       WITH cte_bmi_age AS (
                         SELECT v.PATID,
                                v.HT,
                                v.WT,
                                v.ORIGINAL_BMI,
                                CASE WHEN HT = 0 THEN ORIGINAL_BMI
                                     ELSE NVL(v.ORIGINAL_BMI,round(WT/(v.HT*v.HT)*703)) 
                                     END AS BMI,
                                d.BIRTH_DATE,
                                v.MEASURE_DATE::date as INDEX_DATE,
                                round(datediff(day,d.BIRTH_DATE::date,v.MEASURE_DATE::date)/365.25) AS age_at_index,
                                count(distinct v.MEASURE_DATE::date) over (partition by d.PATID) AS bmi_cnt,
                                min(v.MEASURE_DATE::date) over (partition by d.PATID) AS bmi_date_min,
                                max(v.MEASURE_DATE::date) over (partition by d.PATID) AS bmi_date_max,
                                d.sex, 
                                CASE WHEN d.race IN ('05') THEN 'white' 
                                     WHEN d.race IN ('03') THEN 'black'
                                     WHEN d.race IN ('NI','UN',NULL) THEN 'NI'
                                     ELSE 'ot' END AS race, 
                                CASE WHEN d.hispanic = 'Y' THEN 'hispanic' 
                                     WHEN d.hispanic = 'N' THEN 'non-hispanic' 
                                     WHEN d.hispanic IN ('NI','UN',NULL) THEN 'NI'
                                     ELSE 'ot' END AS hispanic
                         FROM `+ site_cdm +`.LDS_VITAL v
                         JOIN `+ site_cdm +`.LDS_DEMOGRAPHIC d ON d.PATID = v.PATID
                         WHERE NVL(v.HT*v.WT,v.ORIGINAL_BMI) is not null
                         )
                         SELECT bmi.patid
                               ,bmi.birth_date
                               ,bmi.index_date
                               ,bmi.age_at_index
                               ,case when bmi.age_at_index < 19 then 'agegrp1'
                                     when bmi.age_at_index >= 19 and bmi.age_at_index < 24 then 'agegrp2'
                                     when bmi.age_at_index >= 25 and bmi.age_at_index < 85 then 'agegrp' || floor((bmi.age_at_index - 25)/5) + 3
                                     else 'agegrp15' end as agegrp_at_index
                               ,CASE WHEN max(bmi.BMI)>=30 THEN 1 ELSE 0 END AS obese_ind
                               ,bmi.bmi_cnt
                               ,datediff(day,bmi.bmi_date_min,bmi.bmi_date_max) AS bmi_date_range
                               ,bmi.sex
                               ,bmi.race
                               ,bmi.hispanic
                               ,'`+ site +`' AS site,
                               MAX(NVL(gpc.CMS_ENR_IND,0)) AS CMS_ENR_IND,
                               MAX(NVL(gpc.CMS_AB_CONT5YR,0)) AS CMS_AB_CONT5YR,
                               MAX(NVL(gpc.CMS_D_CONT5YR,0)) AS CMS_D_CONT5YR
                         FROM cte_bmi_age bmi 
                         LEFT JOIN GPC_TABLE1 gpc ON bmi.patid = gpc.patid
                         GROUP BY bmi.patid, bmi.birth_date, bmi.index_date, bmi.age_at_index, bmi.bmi_cnt, bmi.bmi_date_min, bmi.bmi_date_max, bmi.sex, bmi.race, bmi.hispanic;`;

    // run query
    var sqlstmt_run = snowflake.createStatement({sqlText:sqlstmt_par});
    sqlstmt_run.execute();
}
$$
;

truncate WT_TABLE1;
call get_wt_table1(array_construct(
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

/*get quick summaries*/
create or replace procedure get_summ(COHORT string)
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
var insert_summ = `INSERT INTO DSTAT_DENOM
                    WITH cte_N AS (
                        SELECT COUNT(DISTINCT patid) AS N 
                        FROM `+ COHORT +`_TABLE1
                    ),  cte_site_N AS (
                        SELECT site, COUNT(DISTINCT patid) AS N 
                        FROM `+ COHORT +`_TABLE1
                        GROUP BY site
                    )
                    -------------------overall---------------------------------------
                    SELECT 'GPC','`+ COHORT +`','EHR','N','N',N,1 
                    FROM cte_N 
                    UNION
                    SELECT 'GPC','`+ COHORT +`','XWALK','n','n',COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                    FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                    WHERE gpc.CMS_ENR_IND = 1 GROUP BY n.N
                    UNION
                    SELECT 'GPC','`+ COHORT +`','XWALK_AB5YR','n','n',COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                    FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                    WHERE gpc.CMS_AB_CONT5YR = 1 GROUP BY n.N
                    UNION
                    SELECT 'GPC','`+ COHORT +`','XWALK_D5YR','n','n',COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                    FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                    WHERE gpc.CMS_D_CONT5YR = 1 GROUP BY n.N /*
                    UNION
                    -------------------by site-----------------------------------------
                    SELECT site,'`+ COHORT +`','EHR','N','N',N,1 
                    FROM cte_site_N
                    UNION
                    SELECT gpc.site,'`+ COHORT +`','XWALK','n','n',COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                    FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                    WHERE gpc.site = n.site AND gpc.CMS_ENR_IND = 1 GROUP BY gpc.site,n.N
                    UNION
                    SELECT gpc.site,'`+ COHORT +`','XWALK_AB5YR','n','n',COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                    FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                    WHERE gpc.site = n.site AND gpc.CMS_AB_CONT5YR = 1 GROUP BY gpc.site,n.N
                    UNION
                    SELECT gpc.site,'`+ COHORT +`','XWALK_D5YR','n','n',COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                    FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                    WHERE gpc.site = n.site AND gpc.CMS_D_CONT5YR = 1 GROUP BY gpc.site,n.N
                    ;`; 
// run query
var insert_summ_run = snowflake.createStatement({sqlText:insert_summ});
insert_summ_run.execute(); 

var insert_summ_demo = `INSERT INTO DSTAT_DENOM
                        WITH cte_N AS (
                            SELECT COUNT(DISTINCT patid) AS N 
                            FROM `+ COHORT +`_TABLE1
                        ),  cte_site_N AS (
                            SELECT site, COUNT(DISTINCT patid) AS N 
                            FROM `+ COHORT +`_TABLE1
                            GROUP BY site
                        )
                        -------------------overall-demo-----------------------------------------
                        SELECT 'GPC','`+ COHORT +`','EHR','agegrp_at_index',gpc.agegrp_at_index,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                        GROUP BY gpc.agegrp_at_index, n.N
                        UNION
                        SELECT 'GPC','`+ COHORT +`','EHR','sex',gpc.sex,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                        GROUP BY gpc.sex,n.N
                        UNION
                        SELECT 'GPC','`+ COHORT +`','EHR','race',gpc.race,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                        GROUP BY gpc.race,n.N
                        UNION
                        SELECT 'GPC','`+ COHORT +`','EHR','hispanic',gpc.hispanic,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                        GROUP BY gpc.hispanic,n.N */
                        UNION 
                        -------------------overall-xwalk-demo-----------------------------------------
                        SELECT 'GPC','`+ COHORT +`','EHR','agegrp_at_index',gpc.agegrp_at_index,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                        WHERE gpc.CMS_ENR_IND = 1 GROUP BY gpc.agegrp_at_index,n.N
                        UNION
                        SELECT 'GPC','`+ COHORT +`','EHR','sex',gpc.sex,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                        WHERE gpc.CMS_ENR_IND = 1 GROUP BY gpc.sex,n.N
                        UNION
                        SELECT 'GPC','`+ COHORT +`','EHR','race',gpc.race,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                        WHERE gpc.CMS_ENR_IND = 1 GROUP BY gpc.race,n.N
                        UNION
                        SELECT 'GPC','`+ COHORT +`','EHR','hispanic',gpc.hispanic,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc CROSS JOIN cte_N n
                        WHERE gpc.CMS_ENR_IND = 1 GROUP BY gpc.hispanic,n.N
                        UNION
                        -------------------by site-demo-----------------------------------------
                        SELECT gpc.site,'`+ COHORT +`','EHR','agegrp_at_index',gpc.agegrp_at_index,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                        WHERE gpc.site = n.site AND gpc.CMS_ENR_IND = 1 GROUP BY gpc.site,gpc.agegrp_at_index,n.N
                        UNION
                        SELECT gpc.site,'`+ COHORT +`','EHR','sex',gpc.sex,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                        WHERE gpc.site = n.site AND gpc.CMS_ENR_IND = 1 GROUP BY gpc.site,gpc.sex,n.N
                        UNION
                        SELECT gpc.site,'`+ COHORT +`','EHR','race',gpc.race,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                        WHERE gpc.site = n.site AND gpc.CMS_ENR_IND = 1 GROUP BY gpc.site,gpc.race,n.N
                        UNION
                        SELECT gpc.site,'`+ COHORT +`','EHR','hispanic',gpc.hispanic,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                        WHERE gpc.site = n.site AND gpc.CMS_ENR_IND = 1 GROUP BY gpc.site,gpc.hispanic,n.N
                        UNION
                        -------------------by site-xwalk-demo-----------------------------------------
                        SELECT gpc.site,'`+ COHORT +`','XWALK','agegrp_at_index',gpc.agegrp_at_index,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                        WHERE gpc.site = n.site AND gpc.CMS_ENR_IND = 1 GROUP BY gpc.site,gpc.agegrp_at_index,n.N
                        UNION
                        SELECT gpc.site,'`+ COHORT +`','XWALK','sex',gpc.sex,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                        WHERE gpc.site = n.site AND gpc.CMS_ENR_IND = 1 GROUP BY gpc.site,gpc.sex,n.N
                        UNION
                        SELECT gpc.site,'`+ COHORT +`','XWALK','race',gpc.race,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                        WHERE gpc.site = n.site AND gpc.CMS_ENR_IND = 1 GROUP BY gpc.site,gpc.race,n.N
                        UNION
                        SELECT gpc.site,'`+ COHORT +`','XWALK','hispanic',gpc.hispanic,COUNT(DISTINCT gpc.PATID),ROUND(COUNT(DISTINCT gpc.PATID)/n.N,4) 
                        FROM `+ COHORT +`_TABLE1 gpc, cte_site_N n 
                        WHERE gpc.site = n.site AND gpc.CMS_ENR_IND = 1 GROUP BY gpc.site,gpc.hispanic,n.N
                        ;`; 
// run query
var insert_summ_demo_run = snowflake.createStatement({sqlText:insert_summ_demo});
insert_summ_demo_run.execute(); 
$$
;

truncate DSTAT_DENOM;
call get_summ('GPC');
call get_summ('ALS');
call get_summ('BC');
call get_summ('WT');