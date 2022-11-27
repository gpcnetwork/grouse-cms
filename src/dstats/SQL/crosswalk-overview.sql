create or replace table xwalk_summary_orig as
with N as (
    select 'GPC' as site_id, count(distinct bene_id) as bene_cnt
    from bene_mapping.unique_bene_xwalk_2022
    where unique_match = 1
    union all
    select site_id, count(bene_id)
    from bene_mapping.unique_bene_xwalk_2022
    where unique_match = 1
    group by site_id
), N_dob_m as (
    select 'GPC' as site_id, count(distinct bene_id) as bene_cnt
    from bene_mapping.unique_bene_xwalk_2022
    where unique_match = 1 and dob_match = 1
    union all
    select site_id, count(bene_id)
    from bene_mapping.unique_bene_xwalk_2022
    where unique_match = 1 and dob_match = 1
    group by site_id
), N_sex_m as (
    select 'GPC' as site_id, count(distinct bene_id) as bene_cnt
    from bene_mapping.unique_bene_xwalk_2022
    where unique_match = 1 and sex_match = 1
    union all
    select site_id, count(bene_id)
    from bene_mapping.unique_bene_xwalk_2022
    where unique_match = 1  and sex_match = 1
    group by site_id
)
select CASE WHEN N.site_id = 'UMO' THEN 'MU'
            WHEN N.site_id = 'UN' THEN 'UNMC'
            WHEN N.site_id = 'MCRF' THEN 'MCRI'
            WHEN N.site_id = 'UK' THEN 'KUMC'
            WHEN N.site_id = 'AH' THEN 'ALLINA'
            WHEN N.site_id = 'WU' THEN 'WASHU'
            ELSE N.site_id END AS siteid,
       N.bene_cnt, 
       ndob.bene_cnt as bene_dob_match, round(ndob.bene_cnt/N.bene_cnt,3) dob_match_rate,
       nsex.bene_cnt as bene_sex_match, round(nsex.bene_cnt/N.bene_cnt,3) sex_match_rate
from N
join N_dob_m ndob on N.site_id = ndob.site_id
join N_sex_m nsex on N.site_id = nsex.site_id
order by bene_cnt desc
;

-- select * from xwalk_summary_orig
-- order by bene_cnt desc;

create or replace table xwalk_summary_demo as
with n as (
    select 'GPC' as siteid, count(distinct bene_id) as bene_cnt
    from bene_mapping.bene_xwalk_cms
    union
    select siteid, count(distinct bene_id) as bene_cnt
    from bene_mapping.bene_xwalk_cms
    group by siteid
)
select a.siteid,
       a.bene_cnt,
       n.bene_cnt as bene_cnt_incld,
       round(n.bene_cnt/a.bene_cnt,3) retention_rate
from xwalk_summary_orig a
join n on a.siteid = n.siteid
order by bene_cnt desc
;

-- create or replace xwalk_summary_cross_pat as
-- with pat_cross as (
--     select bene_id, count(distinct siteid)
--     from bene_mapping.bene_xwalk_cms
--     group by bene_id
--     having count(distinct siteid)> 1
-- )
-- select count(distinct bene_id) as pat_cross_n
-- from pat_cross
-- ; -- 231,509