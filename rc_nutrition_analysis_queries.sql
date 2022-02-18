-- getting the total revenue and percent of total revenue, revenue per patient and revenue per practice

--create or replace table df_RC_NUTRITION_REVENUE as(
with df_transactions as (-- getting the revenue
      select   datefromparts(year(t.transaction_datetime), month(t.transaction_datetime), 1) as transaction_date
              , t.patient_odu_id
              , t.TOP_REVENUE_CATEGORY_ID
              , pats.species
              , t.reporting_amount
              , zipcode_infos.region
              , practice.STATE
     from "ANALYTICS_PRODUCTION"."CORE"."TRANSACTIONS" as t
     left join "ANALYTICS_PRODUCTION"."CORE"."PATIENTS" pats
        on t.PATIENT_ODU_ID = pats.PATIENT_ODU_ID
     left join "ANALYTICS_PRODUCTION"."CORE"."PRACTICES" practice
        on t.practice_odu_id = practice.practice_odu_id
     left join "ANALYTICS_PRODUCTION"."CORE"."SERVERS" as servers 
        on t.server_odu_id = servers.server_odu_id
     left join "ANALYTICS_PRODUCTION"."SOURCES"."CITUS__ZIPCODE_INFOS" as zipcode_infos 
        on servers.zip = zipcode_infos.zipcode
     where t.TRANSACTION_DATETIME > DATEADD(MONTH, -37, GETDATE())--'2019-02-01'
     and t.TRANSACTION_DATETIME <= GETDATE()
     and t.is_revenue = 'true'
), 
  nutrition_revenue as ( -- getting total revenue for nutrition
    select  transaction_date
           , species
           , region
           , STATE
           , count (distinct(patient_odu_id)) as purchasing_patients
           , sum (reporting_amount) as nutrition_revenue 
    from df_transactions
    where TOP_REVENUE_CATEGORY_ID = 18 -- getting diet revenue
    group by 1,2,3,4
), total_revenue as ( -- getting total_revenue
      select transaction_date
           , species
           , region
           , STATE
           , count (distinct(patient_odu_id)) as total_patients
           , count (distinct(patient_odu_id)) as total_practices
           , sum (reporting_amount) as total_revenue 
      from df_transactions
      group by 1,2,3,4
)
    select a.* , b.total_patients, b.total_practices, b.total_revenue
    from nutrition_revenue a
    join total_revenue b
    on a.transaction_date = b.transaction_date
    and a.species = b.species
    and a.state = b.state
    and a.region = b.region
--)


-- revenue per category (codetag)
--create or replace table df_RC_NUTRITION_CATEGORY as(
with df_nutrition_rev as (-- getting the nutrition revenue
      select  datefromparts(year(t.transaction_datetime), month(t.transaction_datetime), 1) as transaction_date
              , t.revenue_category_odu_id
              , pats.species
              , t.reporting_amount as nutrition_revenue
              , zipcode_infos.region
              , practice.STATE
              , t.LEVEL_2_REVENUE_CATEGORY_ID as manufacturer
              , t.LEVEL_3_REVENUE_CATEGORY_ID as diet_category
              , codes.CATALOG_TAG_ODU_ID as codes
     from "ANALYTICS_PRODUCTION"."CORE"."TRANSACTIONS" as t
     left join "ANALYTICS_PRODUCTION"."SOURCES"."CITUS__CATALOG_TAG_MAPPINGS" as codes
        on t.CATALOG_ODU_ID = codes.CATALOG_ODU_ID
     left join "ANALYTICS_PRODUCTION"."CORE"."PATIENTS" pats
        on t.PATIENT_ODU_ID = pats.PATIENT_ODU_ID
     left join "ANALYTICS_PRODUCTION"."CORE"."PRACTICES" practice
        on t.practice_odu_id = practice.practice_odu_id
     left join "ANALYTICS_PRODUCTION"."CORE"."SERVERS" as servers 
        on t.server_odu_id = servers.server_odu_id
     left join "ANALYTICS_PRODUCTION"."SOURCES"."CITUS__ZIPCODE_INFOS" as zipcode_infos 
        on servers.zip = zipcode_infos.zipcode
     where t.TRANSACTION_DATETIME > DATEADD(MONTH, -37, GETDATE())--'2019-02-01'
     and t.TRANSACTION_DATETIME <= GETDATE()
     and t.is_revenue = 'true'
     and t.TOP_REVENUE_CATEGORY_ID = 18
)--, revenue_by_codes as (
    select transaction_date
          , species
          , region
          , state
          , codes
          , tags.name
          , sum(nutrition_revenue) as nutrition_revenue_by_codetag
  FROM df_nutrition_rev n
  left join "ANALYTICS_PRODUCTION"."CORE"."CATALOG_TAGS" tags
  on n.codes = tags.CATALOG_TAG_ODU_ID
  group by 1,2,3,4,5,6
  order by 1
--)

-- revenue stratified by practice size 

--create or replace table df_RC_NUTRITION_PRACTICE_SIZE as(
  with df_revenue as (-- getting the totalrevenue
        select   
                datefromparts(year(t.transaction_datetime), month(t.transaction_datetime), 1) as transaction_date
                , t.PRACTICE_ODU_ID
                , t.PATIENT_ODU_ID
                , pats.species
                , t.reporting_amount as total_revenue
                , zipcode_infos.region
                , practice.STATE
                , TOP_REVENUE_CATEGORY_ID
       from "ANALYTICS_PRODUCTION"."CORE"."TRANSACTIONS" as t
       left join "ANALYTICS_PRODUCTION"."CORE"."PATIENTS" pats
       on t.PATIENT_ODU_ID = pats.PATIENT_ODU_ID
       left join "ANALYTICS_PRODUCTION"."CORE"."PRACTICES" practice
       on t.practice_odu_id = practice.practice_odu_id
       left join "ANALYTICS_PRODUCTION"."CORE"."SERVERS" as servers 
       on t.server_odu_id = servers.server_odu_id
       left join "ANALYTICS_PRODUCTION"."SOURCES"."CITUS__ZIPCODE_INFOS" as zipcode_infos 
       on servers.zip = zipcode_infos.zipcode
       where t.TRANSACTION_DATETIME > DATEADD(MONTH, -37, GETDATE())--'2019-02-01'
       and t.TRANSACTION_DATETIME <= GETDATE()
       and t.is_revenue = 'true'
 ), df_practice_size as (
    -- to get the number of patients for each practice to determine practice size
   select PRACTICE_ODU_ID
   , transaction_date
   , case 
        when count(distinct PATIENT_ODU_ID) <= 200 then 'small practice'
        when ( count(distinct PATIENT_ODU_ID) > 200 and count(distinct PATIENT_ODU_ID) <= 800) then 'medium practice'
        when count(distinct PATIENT_ODU_ID) > 800 then 'large practice'
     end as practice_size
   from df_revenue
   group by 1,2
   order by 1,2
 ), df_revenue_per_practice as (
    -- getting revenue for every practice
   select  transaction_date
           , PRACTICE_ODU_ID
           , species
           , region
           , STATE
           , sum (total_revenue) as nutrition_revenue 
   from df_revenue
   where TOP_REVENUE_CATEGORY_ID = 18
   group by 1,2,3,4,5
   order by 2,1
 )
   select rev.transaction_date, rev.species, rev.region, rev.STATE
          , b.practice_size
          , round(sum(nutrition_revenue), 2) as nutrition_rev_practice_size
   from df_revenue_per_practice rev
   left join df_practice_size b
   on rev.transaction_date = b.transaction_date
   and rev.PRACTICE_ODU_ID = b.PRACTICE_ODU_ID
   group by 1,2,3,4,5
--)
       
-- nutrition revenue by manufacturer

--create or replace table df_RC_NUTRITION_manufacturer as(
with df_nutrition_rev as (-- getting the nutrition revenue
      select  datefromparts(year(t.transaction_datetime), month(t.transaction_datetime), 1) as transaction_date
              , t.revenue_category_odu_id
              , pats.species
              , t.reporting_amount as nutrition_revenue
              , zipcode_infos.region
              , practice.STATE
              , t.LEVEL_2_REVENUE_CATEGORY_ID as manufacturer
     from "ANALYTICS_PRODUCTION"."CORE"."TRANSACTIONS" as t
     left join "ANALYTICS_PRODUCTION"."CORE"."PATIENTS" pats
        on t.PATIENT_ODU_ID = pats.PATIENT_ODU_ID
     left join "ANALYTICS_PRODUCTION"."CORE"."PRACTICES" practice
        on t.practice_odu_id = practice.practice_odu_id
     left join "ANALYTICS_PRODUCTION"."CORE"."SERVERS" as servers 
        on t.server_odu_id = servers.server_odu_id
     left join "ANALYTICS_PRODUCTION"."SOURCES"."CITUS__ZIPCODE_INFOS" as zipcode_infos 
        on servers.zip = zipcode_infos.zipcode
     where t.TRANSACTION_DATETIME > DATEADD(MONTH, -37, GETDATE())--'2019-02-01'
     and t.TRANSACTION_DATETIME <= GETDATE()
     and t.is_revenue = 'true'
     and t.TOP_REVENUE_CATEGORY_ID = 18
)
    select transaction_date
          , species
          , region
          , state
          , manufacturer
          , sum(nutrition_revenue) as nutrition_revenue_by_manufacturer
  FROM df_nutrition_rev n
  group by 1,2,3,4,5
  order by 1
--)