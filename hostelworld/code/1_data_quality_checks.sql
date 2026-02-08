
/*********************************/
/*      DATA QUALITY CHECKS      */
/*********************************/


-- check available data in the file / confirm we have only AB Test data: 
select min(event_date), max(event_date)
FROM `elegant-shelter-407900.hostelworld.ab_test_dataset`
;
-- min: 2025-05-19	
-- max: 2025-07-14


-- any null dates?
select 
  count( case when event_date is null then 1 end) as event_date_null_Records
  , count( case when event_datetime is null then 1 end) as event_datetime_null_Records
FROM `elegant-shelter-407900.hostelworld.ab_test_dataset`
;
-- 0


-- checking cohort ditribution per release version
SELECT RELEASE_VERSION  
  , COHORT
  , count(distinct user_id) as users
FROM `elegant-shelter-407900.hostelworld.ab_test_dataset`
group by 1, 2
--order by 3 desc
order by 1, 2
;
/*
 WHY?
 In my experience, some older versions may noy get the Variant they are expectd to have and it sometimes influences results.
 Also, w can see some dev data (maybe from test devices) identified with "staging" in front of the version identifiers, most likely used to try the variants allocation and functionality of the feature - should be removed from the analysis
 - Versions 9.79.3 and 9.81.0 only contain Variation type users, however they don't seem to be super significant in the analysis if we include them anyway, as each have only 2 users in each and in the remaining versions we have a fair distribution
 - Per version we seem to have a fair representation of the user distribution between Variation and Control, as to have a Control group relatively representative, in terms of general size of the sample it can be a smaller sample as long as it remains statistically representative of the target group - or variation (if we follow a normal approximation calculation of the sample size). Further checks on Control group's representativeness of the target will be made in another code.
May be worth mentioning this release version diferences across the entire population, just in case the results are not as expected. This can then be taken into consideration for a deeper dive, or even to report problems with previous versions and outdated devices.
*/

-- checking all events combinations (to know the data we have)
select EVENT_NAME 
  , ACTION  
  , PAGE_TYPE
  , count(*)
FROM `elegant-shelter-407900.hostelworld.ab_test_dataset`
group by 1, 2, 3
order by 1, 2, 3
;


-- checking users per test variant
select user_id
  , count(distinct cohort) as cohort_count
FROM `elegant-shelter-407900.hostelworld.ab_test_dataset`
group by 1
having cohort_count > 1
;
-- 17679 users out of all 173872 users (10.2% of all users)
-- these will need to be removed as the results will likely be affected, since to try to isolate the effects of the feature changed, the participants can't be exposed to both variants, as then the behaviors are biased by previous experiences and expectations created and therefore unreliable for the comparison.


-- checking if there are any technical issues with data collection when the tool batches events together (GA used to do this quite often in Mobile and Kiosk applications in my previous experiences) sending the same event timestamp for multiple events that genberally speaking have a natural time to trigger inbetween (ex: seeing the item list and clicking in an item from that list)
-- all data:
select a.*
from `elegant-shelter-407900.hostelworld.ab_test_dataset` a
join (
    -- to identify which users had differences betwen events of 0 seconds and then search their whole sessions and see the data myself to see what kind of events have this "issue"
    select distinct user_id
    from (
      select 
        distinct user_id
          , EVENT_DATE
          , event_datetime
          , lag(event_datetime) over (partition by concat(user_id) order by event_datetime) as last_event_time
          , lag(event_name) over (partition by concat(user_id) order by event_datetime) as last_event
          , date_diff(event_datetime,  lag(event_datetime) over (partition by concat(user_id) order by event_datetime), second) as date_diff_seconds
      FROM `elegant-shelter-407900.hostelworld.ab_test_dataset`
      order by 1, 2, 3
      )
      where date_diff_seconds = 0
    )
  using(user_id)
order by a.user_id, event_Date, event_datetime
;
-- yes we have and those happen just within two events in a session, not for a bulk load of the entire session events.

-- checking what event combination results in these 0 seconds apart events:
-- trying to understand if this could potentially affect the conversion rates calculation ahead
  select 
    event_name
    , last_event
    , count(*) as frequency
  from (
      select 
        distinct user_id
          , EVENT_DATE
          , event_name
          , event_datetime
          , lag(event_datetime) over (partition by concat(user_id) order by event_datetime) as last_event_time
          , lag(event_name) over (partition by concat(user_id) order by event_datetime) as last_event
          , date_diff(event_datetime,  lag(event_datetime) over (partition by concat(user_id) order by event_datetime), second) as date_diff_seconds
      FROM `elegant-shelter-407900.hostelworld.ab_test_dataset`
      order by 1, 2, 3
      )
  where date_diff_seconds = 0
  group by 1,2
  order by 3 desc
;

/* given the explanations in the document of what each event means, and not knowing the particulat UI and layout of the mobile app at the time of this exercise's data, I'll make some assumptions below to help explain the need (or not) to remove such events or sessions from the analysis: 


      Event                            Previous Event                     Frequency
 1 -> Search_Event	                   Homepage_Viewed	                  2535
 2 -> Homepage_Viewed	                 Destination_Search_Page_Viewed	    411
 3 -> Homepage_Viewed	                 Homepage_Viewed	                  111
 4 -> view_item	                       Homepage_Viewed	                  67
 5 -> Destination_Search_Page_Viewed	 Search_Event	                      26
 6 -> Search_Event	                   Destination_Search_Page_Viewed	    24
 7 -> Search_Event	                   Search_Event	                      8
 8 -> Search_Event	                   view_item	                        2
 9 -> view_item	                       Destination_Search_Page_Viewed	    2
10 -> Destination_Search_Page_Viewed	 Destination_Search_Page_Viewed	    2
11 -> view_item	                       view_item	                        1


What behaviors does this sequence of events potentially mean?
What technical issues are found and are those relevant enough to remove? 

1. Home page + search together are not a problem. Seems like a pageview event triggered at the same time when the user interacts with the search bar, which would be in the homepage. For my calculations of time to convert this will not affect as I believe I will move past the homepage event, most likely, so adding 0 seconds is not a problem since the total time will be reflected in the last event.
WILL NOT REMOVE

2. Most likely means the user went back to the homepage. This event matters as movement (from a searchresult list to homepage), indicating potential dissatisfaction with the search results. Time here is not super relevant. 
WILL NOT REMOVE

3. Must be a duplication issue due to a duplicate trigger, not a problem for me right no. 
WILL NOT REMOVE

4. An item available in the home page that was clicked on. Matters for movement in the app, not time necessarily. 
WILL NOT REMOVE

5. Matters a lot for movement and also time. This seems to show the time the person spent on a search bar interaction event, so seeing the actions would be beneficial. However, since it's only 26 occurrences, I'll let it stay as it doesn't seem to be too problematic and removing it to ssave on time makes us lose conversion rate value.
WILL NOT REMOVE

WILL NOT REMOVE any of the remaining ones due to the same reasons mentioned in point 5, low amount for effort to remove + potential loss in other metrics.
*/



-- check for nulls where they shouldn't exist
SELECT 
  count(case when user_id is null then 1 end) as user_null
  , count(case when event_name is null then 1 end) as event_null
  , count(case when login_status is null then 1 end) as login_null
  , count(case when platform is null then 1 end) as platform_null
  , count(case when cohort is null then 1 end) as cohort_null
FROM `elegant-shelter-407900.hostelworld.ab_test_dataset`
-- 0
;
-- Check for duplicates

with aa as (
  SELECT 
    EVENT_DATE, EVENT_NAME, ACTION, EVENT_DATETIME, PLATFORM, USER_ID, LOGIN_STATUS, APP_LANGUAGE, RELEASE_VERSION, PAGE_TYPE, IP_COUNTRY, COHORT, count(*) as dups_flg
  FROM `elegant-shelter-407900.hostelworld.ab_test_dataset`
  group by all
  having dups_flg >1
  )

select dups_flg, count(*) --sum(dups_flg)
from aa
group by 1
;


SELECT 1 as RELEASE_VERSION 
  , count(distinct user_id) as users
  , count(*) as rows_
FROM `elegant-shelter-407900.hostelworld.ab_test_dataset`
where RELEASE_VERSION like '%staging%'
group by 1
--order by 3 desc
order by 1, 2 desc
;



-- check if we have any session lasting more than one day and also with long duration if I aggregate by user_id (no session_id or some other identifier)
-- my gioal is to find what defines a session and go with that
-- general information states acceptable time before timeout is 30 minutes in low risk apps - trying this first and then changing to other threasholds if I fund the data too strange (Google search) 
select *
    , countif(date_diff_seconds > 1800 or date_diff_seconds is null)  over (partition by user_id order by event_datetime) AS session_id

from (
     select 
        distinct user_id
          , DATE(DATETIME(event_datetime, coalesce(trim(timezone), 'UTC'))) as EVENT_DATE
          , case when action is null then event_name else concat(action, '-', PAGE_TYPE) end as event
          , DATETIME(event_datetime, coalesce(trim(timezone), 'UTC')) as event_datetime
          , lag(DATETIME(event_datetime, coalesce(trim(timezone), 'UTC'))) over (partition by concat(user_id) order by event_datetime) as last_event_time
          , lag(case when action is null then event_name else concat(action, '-', PAGE_TYPE) end ) over (partition by concat(user_id) order by event_datetime) as last_event
          , date_diff(DATETIME(event_datetime, coalesce(trim(timezone), 'UTC')),  lag(DATETIME(event_datetime, coalesce(trim(timezone), 'UTC'))) over (partition by concat(user_id) order by event_datetime), second) as date_diff_seconds
              
      from `elegant-shelter-407900.hostelworld.ab_test_base` a
      left join `elegant-shelter-407900.hostelworld.country_timezone` b
        on ip_country = country
    )
order by 1, 2, 4
;


-- check landing page quality:
      select event, count(*) as occur
      from (
        select
          case when action is null then event_name else concat(action, '-', PAGE_TYPE) end as event
          , Row_number() over (partition by user_id order by event_datetime) as RN
        FROM `elegant-shelter-407900.hostelworld.ab_test_base`
        qualify RN  = 1
      )
      group by 1
      order by 2 desc
;


-- how many countries would be without timezone
select count(distinct ip_country)
  , count(distinct case when country is not null then ip_country end)
  , string_agg(distinct case when country is null then ip_country end, ', ')

from `elegant-shelter-407900.hostelworld.ab_test_base` a
left join `elegant-shelter-407900.hostelworld.country_timezone` b
  on ip_country = country
;


