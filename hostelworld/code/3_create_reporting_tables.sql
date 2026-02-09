
/* Sessions table */

create or replace table  `elegant-shelter-407900.hostelworld.ab_test_base_sessions` as 
select 
  COHORT
  , user_id
  , session_id -- I defined a session ID when creating the base_2 table because I found multiple sessions spanning across multiple days and I though it could be beneficial to remove such erratic behavior from the equation
  -- this will be seen especially when I'm calcualting the CR % later on that I am keeping sessions with Step 2 as the main denominator for all calculations that should otherwise be held over TOTAL Sessions
  , APP_LANGUAGE
  , IP_COUNTRY
  , release_version
  , timezone
  , min(DATE(event_datetime)) as session_start_date
  , max(DATE(event_datetime)) as session_end_date
  , min(event_datetime) as first_datetime
  , max(event_datetime) as last_datetime
  , date_diff(max(event_datetime), min(event_datetime), second) as session_duration_seconds
  , string_agg(case when action is null then event_name else concat(action, '-', PAGE_TYPE) end, '|*|') as session_seq
from `elegant-shelter-407900.hostelworld.ab_test_base_2` a
group by COHORT
  , user_id
  , session_id
  , APP_LANGUAGE
  , IP_COUNTRY
  , release_version
  , timezone
;


/*
Homepage_Viewed                     User views the homepage
Search_Event                        User interacts with search functionality 
Destination_Search_Page_Viewed      User views search results page
view_item                           User views property details page
purchase                            User completes a booking

Search Activated - User begins typing or focuses on search input (key metric) 
Search Submitted - Users submits search (key metric)


Primary Success Metric:
» Searches per Searcher: Average number of search activations per user who performs at least one search (expected to decrease with better autocomplete). Where a search activation is: event_name = 'Search_Event' and action = 'Search Activated' and page_type = 'Destination Search'.

Health Metrics:
» Search Activation to Search Results Conversion: Percentage of users who view search results after activating search 
» Search Activation to Property Page Conversion: Percentage of users who view property details after activating search 
» Booking Conversion Rate: Percentage of users who complete a purchase (booking) 
*/

/*
select EVENT_NAME 
  , ACTION  
  , PAGE_TYPE
  , count(*)
FROM `elegant-shelter-407900.hostelworld.ab_test_base_2`
group by 1, 2, 3
order by 1, 2, 3
;
*/


create or replace table `elegant-shelter-407900.hostelworld.ab_test_base_sessions_pre_agregg` as
  
with step_1 as (
-- All sessions
select 
  COHORT
  , user_id
  , session_id
  , IP_COUNTRY
  , timezone
  , count(*) as event_count
  , min(DATE(event_datetime)) as session_start_date
  , max(DATE(event_datetime)) as session_end_date
  , min(event_datetime) as first_datetime
  , max(event_datetime) as last_datetime
  , date_diff(max(event_datetime), min(event_datetime), second) as session_duration_seconds
from `elegant-shelter-407900.hostelworld.ab_test_base_2` a
where session_id is not null
group by all
)


, step_2 as (
-- Search Activation
select 
  COHORT
  , user_id
  , session_id
  , IP_COUNTRY
  , timezone
  , count(*) as event_count_Search_Act
  , min(DATE(event_datetime)) as session_start_date
  , max(DATE(event_datetime)) as session_end_date
  , min(event_datetime) as first_datetime
  , max(event_datetime) as last_datetime
  , date_diff(max(event_datetime), min(event_datetime), second) as session_duration_seconds
from `elegant-shelter-407900.hostelworld.ab_test_base_2` a
where event_name = 'Search_Event' 
  and action = 'Search Activated' 
  and page_type = 'Destination Search'
group by all
)


, step_3 as (
-- Search Submitted
select 
  COHORT
  , user_id
  , session_id
  , IP_COUNTRY
  , timezone
  , count(*) as event_count_Search_Sub
  , min(DATE(event_datetime)) as session_start_date
  , max(DATE(event_datetime)) as session_end_date
  , min(event_datetime) as first_datetime
  , max(event_datetime) as last_datetime
  , date_diff(max(event_datetime), min(event_datetime), second) as session_duration_seconds
from `elegant-shelter-407900.hostelworld.ab_test_base_2` a
where event_name = 'Search_Event' 
  and action = 'Search Submitted' 
  and page_type = 'Destination Search'
group by all
)


,  step_4 as (
-- Search Results Viewed
select 
  COHORT
  , user_id
  , session_id
  , IP_COUNTRY
  , timezone
  , count(*) as event_count_Destination_Viewed
  , min(DATE(event_datetime)) as session_start_date
  , max(DATE(event_datetime)) as session_end_date
  , min(event_datetime) as first_datetime
  , max(event_datetime) as last_datetime
  , date_diff(max(event_datetime), min(event_datetime), second) as session_duration_seconds
from `elegant-shelter-407900.hostelworld.ab_test_base_2` a
where event_name = 'Destination_Search_Page_Viewed' 
group by all
)


, step_5 as (
-- view_item
select 
  COHORT
  , user_id
  , session_id
  , IP_COUNTRY
  , timezone
  , count(*) as event_count_view_item
  , min(DATE(event_datetime)) as session_start_date
  , max(DATE(event_datetime)) as session_end_date
  , min(event_datetime) as first_datetime
  , max(event_datetime) as last_datetime
  , date_diff(max(event_datetime), min(event_datetime), second) as session_duration_seconds
from `elegant-shelter-407900.hostelworld.ab_test_base_2` a
where event_name = 'view_item' 
group by all
)



, step_6 as ( 
-- Purchase
select 
  COHORT
  , user_id
  , session_id
  , IP_COUNTRY
  , timezone
  , count(*) as event_count_purchase
  , min(DATE(event_datetime)) as session_start_date
  , max(DATE(event_datetime)) as session_end_date
  , min(event_datetime) as first_datetime
  , max(event_datetime) as last_datetime
  , date_diff(max(event_datetime), min(event_datetime), second) as session_duration_seconds
from `elegant-shelter-407900.hostelworld.ab_test_base_2` a
where event_name = 'purchase' 
group by all
)


Select 
  s1.COHORT
  , s1.user_id
  , s1.session_id
  , s1.IP_COUNTRY
  , s1.timezone
  , s1.session_start_date
  , s1.session_end_date
  , s1.session_duration_seconds
  , event_count_Search_Act
  , event_count_Search_Sub
  , event_count_Destination_Viewed
  , event_count_view_item
  , event_count_purchase
  -- I'll go with my Ghost Steps approach because I've seen issues already with doing simple division
  , case when s2.session_id is not null then 1
         when s2.session_id is null and (s3.session_id is not null or s4.session_id is not null or s5.session_id is not null or s6.session_id is not null) then 2
         else 0 
        end as step_2_flg
  , case when s3.session_id is not null then 1
         when s3.session_id is null and (s4.session_id is not null or s5.session_id is not null or s6.session_id is not null) then 2
         else 0 
        end as step_3_flg
  , case when s4.session_id is not null then 1
         when s4.session_id is null and (s5.session_id is not null or s6.session_id is not null) then 2
         else 0 
        end as step_4_flg
  , case when s5.session_id is not null then 1
         when s5.session_id is null and (s6.session_id is not null) then 2
         else 0 
        end as step_5_flg
  , case when s6.session_id is not null then 1
         else 0 
        end as step_6_flg
  /*
  , case when s2.session_id is not null then 1 else 0 end as step_2_flg
  , case when s3.session_id is not null then 1 else 0 end as step_3_flg
  , case when s4.session_id is not null then 1 else 0 end as step_4_flg
  , case when s5.session_id is not null then 1 else 0 end as step_5_flg
  , case when s6.session_id is not null then 1 else 0 end as step_6_flg
  */
from step_1 s1
left join step_2 s2
  on s1.session_id = s2.session_id
left join step_3 s3
  on s1.session_id = s3.session_id
left join step_4 s4
  on s1.session_id = s4.session_id
left join step_5 s5
  on s1.session_id = s5.session_id
left join step_6 s6
  on s1.session_id = s6.session_id
;



-- Table I'll use in the Looker Studio connection to build the charts and % for stakeholders analysis
create or replace table `elegant-shelter-407900.hostelworld.ab_test_base_reporting` as 

select 
  1 as step_id
  , 'Session Start' as step
  , session_start_date
  , 'Enhanced Autocomplete Search with personalized suggestions' as Experiment
  , COHORT
  , IP_COUNTRY
  , timezone
  , 0 as event_count
  , count(distinct user_id) as users
  , count(distinct session_id) as sessions
  , sum(session_duration_seconds) as total_session_duration
  , 
from `elegant-shelter-407900.hostelworld.ab_test_base_sessions_pre_agregg`
group by all

union all 

select 
  2 as step_id
  , 'Search Activated' as step
  , session_start_date
  , 'Enhanced Autocomplete Search with personalized suggestions' as Experiment
  , COHORT
  , IP_COUNTRY
  , timezone
  , sum(event_count_Search_Act) as event_count 
  , count(distinct user_id) as users
  , count(distinct session_id) as sessions
  , sum(session_duration_seconds) as total_session_duration
  , 
from `elegant-shelter-407900.hostelworld.ab_test_base_sessions_pre_agregg`
where step_2_flg > 0
group by all

union all

select 
  99 as step_id
  , 'Search Activated - step 2 specific so I can test and validate results' as step
  , session_start_date
  , 'Enhanced Autocomplete Search with personalized suggestions' as Experiment
  , COHORT
  , IP_COUNTRY
  , timezone
  , sum(event_count_Search_Act) as event_count 
  , count(distinct user_id) as users
  , count(distinct session_id) as sessions
  , sum(session_duration_seconds) as total_session_duration
  , 
from `elegant-shelter-407900.hostelworld.ab_test_base_sessions_pre_agregg`
where step_2_flg = 1
group by all

union all 

select 
  3 as step_id
  , 'Search Submitted' as step
  , session_start_date
  , 'Enhanced Autocomplete Search with personalized suggestions' as Experiment
  , COHORT
  , IP_COUNTRY
  , timezone
  , sum(event_count_Search_Sub) as event_count
  , count(distinct user_id) as users
  , count(distinct session_id) as sessions
  , sum(session_duration_seconds) as total_session_duration
  , 
from `elegant-shelter-407900.hostelworld.ab_test_base_sessions_pre_agregg`
where step_3_flg > 0
group by all

union all 

select 
  4 as step_id
  , 'Destination Search Results Page View' as step
  , session_start_date
  , 'Enhanced Autocomplete Search with personalized suggestions' as Experiment
  , COHORT
  , IP_COUNTRY
  , timezone
  , sum(event_count_Destination_Viewed) as event_count
  , count(distinct user_id) as users
  , count(distinct session_id) as sessions
  , sum(session_duration_seconds) as total_session_duration
  , 
from `elegant-shelter-407900.hostelworld.ab_test_base_sessions_pre_agregg`
where step_4_flg > 0
group by all

union all 

select 
  5 as step_id
  , 'Viewed Destination Details' as step
  , session_start_date
  , 'Enhanced Autocomplete Search with personalized suggestions' as Experiment
  , COHORT
  , IP_COUNTRY
  , timezone
  , sum(event_count_view_item) as event_count
  , count(distinct user_id) as users
  , count(distinct session_id) as sessions
  , sum(session_duration_seconds) as total_session_duration
  , 
from `elegant-shelter-407900.hostelworld.ab_test_base_sessions_pre_agregg`
where step_5_flg > 0
group by all

union all 

select 
  6 as step_id
  , 'Booked a Stay' as step
  , session_start_date
  , 'Enhanced Autocomplete Search with personalized suggestions' as Experiment
  , COHORT
  , IP_COUNTRY
  , timezone
  , sum(event_count_purchase) as event_count
  , count(distinct user_id) as users
  , count(distinct session_id) as sessions
  , sum(session_duration_seconds) as total_session_duration
  , 
from `elegant-shelter-407900.hostelworld.ab_test_base_sessions_pre_agregg`
where step_6_flg > 0
group by all

