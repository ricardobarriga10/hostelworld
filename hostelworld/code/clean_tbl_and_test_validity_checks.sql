/*******************************************************/
/*        FINAL CLEAN TABLE TO USE FOR ANALYSIS        */
/*******************************************************/


create or replace table `elegant-shelter-407900.hostelworld.ab_test_base_2` as 
select 
  to_hex(md5(concat(user_id, countif(date_diff_seconds > 1800 or date_diff_seconds is null)  over (partition by user_id order by event_datetime)))) as sssion_id
  , * except(cohorts_p_user, RN)
from (
     select 
          USER_ID
          , COHORT
          , DATE(DATETIME(event_datetime, coalesce(trim(timezone), 'UTC'))) as event_date
          , EVENT_NAME
          , ACTION
          , PAGE_TYPE
          , DATETIME(event_datetime, coalesce(trim(timezone), 'UTC')) as event_datetime
          , coalesce(timezone, 'UTC') as timezone
          , IP_COUNTRY
          , PLATFORM
          , LOGIN_STATUS
          , APP_LANGUAGE
          , RELEASE_VERSION
          , date_diff(DATETIME(event_datetime, coalesce(trim(timezone), 'UTC')),  lag(DATETIME(event_datetime, coalesce(trim(timezone), 'UTC'))) over (partition by concat(user_id) order by event_datetime), second) as date_diff_seconds
          , count(distinct cohort) over (partition by user_id) as cohorts_p_user 
          , row_number() over (partition by EVENT_DATE, EVENT_NAME, ACTION, EVENT_DATETIME, PLATFORM, USER_ID, LOGIN_STATUS, APP_LANGUAGE, RELEASE_VERSION, PAGE_TYPE, IP_COUNTRY, COHORT) as RN
              
      FROM `elegant-shelter-407900.hostelworld.ab_test_dataset` a
      left join `elegant-shelter-407900.hostelworld.country_timezone` b
        on ip_country = country
      where RELEASE_VERSION not like '%staging%'
      qualify cohorts_p_user < 2
          and RN = 1
    )
order by 1, 2, 4
;


ALTER TABLE `elegant-shelter-407900.hostelworld.ab_test_base_2` 
RENAME COLUMN sssion_id TO session_id
;


select
  'Base Clean' as table_name
  , count(distinct user_id) as total_users
  , count(*) as rows_
from `elegant-shelter-407900.hostelworld.ab_test_base`

union all

select
  'Base Clean 2' as table_name
  , count(distinct user_id) as total_users
  , count(*) as rows_
from `elegant-shelter-407900.hostelworld.ab_test_base_2`

union all

select
  'Base Original' as table_name
  , count(distinct user_id) as total_users
  , count(*) as rows_
from `elegant-shelter-407900.hostelworld.ab_test_dataset`
;



/*********************************/
/*     VIABILITY OF THE TEST     */
/*********************************/


-- Validate cohort size overall:

Select cohort
  , count(distinct user_id) as user_count
from `elegant-shelter-407900.hostelworld.ab_test_base`
group by 1
order by 2 desc
;


-- Validate cohort COUNTRY distribution:
-- socio economic behavior
select IP_COUNTRY
  , count(distinct case when cohort = 'Control' then user_id end) as control_users
  , 42858 as total_control_users
  , round(100*count(distinct case when cohort = 'Control' then user_id end) / 42858, 2) as control_users_perc
  , count(distinct case when cohort != 'Control' then user_id end) as variation_users
  , 113284 as total_variation_users
  , round(100*count(distinct case when cohort != 'Control' then user_id end) / 113284, 2) as variation_users_perc
  , round(100*count(distinct case when cohort = 'Control' then user_id end) / 42858, 2) - round(100*count(distinct case when cohort != 'Control' then user_id end) / 113284, 2) as perc_diff
from `elegant-shelter-407900.hostelworld.ab_test_base`
group by all
order by abs(perc_diff) desc
;


-- Validate cohort PLATFORM distribution:
-- different user experience overall is other apps 
select distinct platform from `elegant-shelter-407900.hostelworld.ab_test_base`;
-- only android users :ok:


-- Validate cohort LOGIN habits distribution:
-- different user experience overall 
select distinct LOGIN_STATUS from `elegant-shelter-407900.hostelworld.ab_test_base`;

select LOGIN_STATUS
  , count(distinct case when cohort = 'Control' then user_id end) as control_users
  , 42858 as total_control_users
  , round(100*count(distinct case when cohort = 'Control' then user_id end) / 42858, 2) as control_users_perc
  , count(distinct case when cohort != 'Control' then user_id end) as variation_users
  , 113284 as total_variation_users
  , round(100*count(distinct case when cohort != 'Control' then user_id end) / 113284, 2) as variation_users_perc
  , round(100*count(distinct case when cohort = 'Control' then user_id end) / 42858, 2) - round(100*count(distinct case when cohort != 'Control' then user_id end) / 113284, 2) as perc_diff
from `elegant-shelter-407900.hostelworld.ab_test_base`
group by all
order by abs(perc_diff) desc
;


-- Validate cohort APP LANGUAGE distribution:
-- different user experience overall, and could potentially have some influence in the auto complete translator 
select APP_LANGUAGE
  , count(distinct case when cohort = 'Control' then user_id end) as control_users
  , 42858 as total_control_users
  , round(100*count(distinct case when cohort = 'Control' then user_id end) / 42858, 2) as control_users_perc
  , count(distinct case when cohort != 'Control' then user_id end) as variation_users
  , 113284 as total_variation_users
  , round(100*count(distinct case when cohort != 'Control' then user_id end) / 113284, 2) as variation_users_perc
  , round(100*count(distinct case when cohort = 'Control' then user_id end) / 42858, 2) - round(100*count(distinct case when cohort != 'Control' then user_id end) / 113284, 2) as perc_diff
from `elegant-shelter-407900.hostelworld.ab_test_base`
group by all
order by abs(perc_diff) desc
;


-- Validate cohort VERSION distribution:
-- different user experience overall in other apps 
select RELEASE_VERSION
  , count(distinct case when cohort = 'Control' then user_id end) as control_users
  , 42858 as total_control_users
  , round(100*count(distinct case when cohort = 'Control' then user_id end) / 42858, 2) as control_users_perc
  , count(distinct case when cohort != 'Control' then user_id end) as variation_users
  , 113284 as total_variation_users
  , round(100*count(distinct case when cohort != 'Control' then user_id end) / 113284, 2) as variation_users_perc
  , round(100*count(distinct case when cohort = 'Control' then user_id end) / 42858, 2) - round(100*count(distinct case when cohort != 'Control' then user_id end) / 113284, 2) as perc_diff
from `elegant-shelter-407900.hostelworld.ab_test_base`
group by all
order by abs(perc_diff) desc
;

-- Validate cohort common EVENTS distribution:
-- different user habits overall
 -- previous data would really benefit in this part
 -- no point in checking now because this is only test data

 -- repeating customers
 -- purchasing custmers
 -- distribution per time to book cohort (same day purchases vs medium planners vs long term planners) 

-- we'd need past data to evaluate this, as typically these groups are generated using data from before the test.
