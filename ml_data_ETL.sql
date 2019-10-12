-----------
-----------
-----------
----------- TRAINING INTL Separated
-----------
-----------
-----------
-----------

CREATE OR REPLACE VIEW periscope.sales_fcst_train_intl_separated AS
(

WITH a AS 
--- Opportunities touched in last 120
(SELECT a.dt,
       c.fy_quarter,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN 1 ELSE 0 END) AS stage_1_count,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount_usd ELSE 0 END) AS stage_1_amount,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN 1 ELSE 0 END) AS stage_2_count,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount_usd ELSE 0 END) AS stage_2_amount,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN 1 ELSE 0 END) AS stage_3_count,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount_usd ELSE 0 END) AS stage_3_amount,
       SUM(CASE WHEN a.stagename = '4. POC' THEN 1 ELSE 0 END) AS stage_4_count,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount_usd ELSE 0 END) AS stage_4_amount,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN 1 ELSE 0 END) AS stage_5_count,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount_usd ELSE 0 END) AS stage_5_amount,
       SUM(CASE WHEN a.stagename IN ('3. Solution','4. POC','5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage345_large_deals,
       SUM(CASE WHEN a.stagename IN ('4. POC','5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage45_large_deals,
       SUM(CASE WHEN a.stagename IN ('5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage5_large_deals
FROM  (SELECT a.*, b.rate, a.amount AS amount_usd ---- amount in USD
        FROM src_sfdc.opportunity_history a
        LEFT JOIN src_zuora.currency b ON (case when a.currencyisocode is null then 'USD' else a.currencyisocode end) = b.alphabeticcode
        where  a.dt - a.lastactivitydate::date < 120 OR a.dt - a.lastmodifieddate::date --- only opps that were touched within 120 days
        ) a  
  LEFT JOIN src_sfdc.account b ON a.accountid = b.id 
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
  -----lab.dw_20190311_sales_owner_division_clean_up
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount_usd > 0
AND   a.closedate between c.qtr_start AND c.qtr_end ---- only opps closing from the given quarter
AND a.id <> '0060W00000y3J2PQAU' --- excluding HSBC
GROUP BY 1,
         2,
         3),
         
b AS
---- QTD bookings
(SELECT booking_date__c as dt,
       fy_quarter,
       case when sales_div_clean is null then 'UnDefined' else sales_div_clean end as sales_div,
       SUM(bookings) OVER (PARTITION BY fy_quarter, sales_div_clean ORDER BY booking_date__c ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bookings_qtd
FROM (SELECT a.booking_date__c,
             b.fy_quarter,
             c.sales_div_clean,
             SUM(Amount__c) AS bookings
      FROM src_sfdc.bookings a
        LEFT JOIN src_config.zoom_quarter_mapping b
               ON a.booking_date__c BETWEEN b.qtr_start
              AND b.qtr_end
        LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.owner_division__c = c.sales_div_dirty
      WHERE 1 = 1
      AND   isdeleted = FALSE
      AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 18) 
            OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
            OR (Order_Type__c IN ('New','New Order') AND Amount__c < 18 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
            OR (bookingexception__c = 'Y'))
      AND   LOWER(owner_name) NOT LIKE '%integration%'
      AND   account__c <> ''
      AND   account__c IS NOT NULL
      AND a.id <> 'a1j0W000008kbLlQAI' --- excluding HSBC
      GROUP BY 1,
               2,
               3)
ORDER BY 3,1),

c AS
---- Bookings total for the quarter
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(Amount__c) AS direct_bookings
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.owner_division__c = c.sales_div_dirty
WHERE 1 = 1
AND   isdeleted = FALSE
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 18) 
      OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
      OR (Order_Type__c IN ('New','New Order') AND Amount__c < 18 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
      OR (bookingexception__c = 'Y'))
AND   lower(owner_name) NOT LIKE '%integration%'
AND   account__c <> ''
AND   account__c IS NOT NULL
AND a.id <> 'a1j0W000008kbLlQAI' --- excluding HSBC
GROUP BY 1,2
ORDER BY 1,2),

d AS
--- quota
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(gross_quota__c) AS quota
FROM src_sfdc.quota a
  LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
WHERE quota_owner_type__c = 'Segment'
and gross_quota__c > 0
GROUP BY 1,2

-- UNION
-- SELECT b.fy_quarter,
--        case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
--        SUM(gross_quota__c) AS quota
-- FROM lab.temp_quota a
--   LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
--   LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
-- WHERE quota_owner_type__c = 'Segment'
-- and gross_quota__c > 0
-- GROUP BY 1,2
),

e AS
(SELECT a.fy_quarter,
       b.dt,
       b.dt - a.qtr_start + 1 AS day_of_qtr
FROM src_config.zoom_quarter_mapping a
  LEFT JOIN src_config.zoom_calendar b
         ON b.dt BETWEEN qtr_start
        AND qtr_end),

f AS
(SELECT a.dt,
       c.fy_quarter,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount_usd ELSE 0 END) AS stage_1_amount_all,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount_usd ELSE 0 END) AS stage_2_amount_all,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount_usd ELSE 0 END) AS stage_3_amount_all,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount_usd ELSE 0 END) AS stage_4_amount_all,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount_usd ELSE 0 END) AS stage_5_amount_all
FROM  (select *, amount as amount_usd from src_sfdc.opportunity_history) a
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount_usd > 0
AND   a.closedate between c.qtr_start AND c.qtr_end ---- only opps closing from the given quarter
AND a.id <> '0060W00000y3J2PQAU' --- excluding HSBC
GROUP BY 1,
         2,
         3)

SELECT e.dt,
       a.fy_quarter,
       e.day_of_qtr,
       a.sales_div,
       d.quota,
       a.stage_1_count,
       a.stage_1_amount,
       a.stage_2_count,
       a.stage_2_amount,
       a.stage_3_count,
       a.stage_3_amount,
       a.stage_4_count,
       a.stage_4_amount,
       a.stage_5_count,
       a.stage_5_amount,
       CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
            WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
            ELSE b.bookings_qtd END as bookings_qtd,
       c.direct_bookings,
       case when a.fy_quarter like '%Q1' then e.day_of_qtr::float / 89 else e.day_of_qtr::float / 92 end as qtr_pct,
       a.stage_1_amount / d.quota AS s1_quota_ratio,
       a.stage_2_amount / d.quota AS s2_quota_ratio,
       a.stage_3_amount / d.quota AS s3_quota_ratio,
       a.stage_4_amount / d.quota AS s4_quota_ratio,
       a.stage_5_amount / d.quota AS s5_quota_ratio,
       (a.stage_4_amount + a.stage_5_amount) / d.quota AS s45_quota_ratio,
       (a.stage_1_amount + a.stage_2_amount + a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s12345_bk_qtd_quota_ratio,
       (a.stage_2_amount + a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s2345_bk_qtd_quota_ratio,
       (a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s345_bk_qtd_quota_ratio,
       (a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s45_bk_qtd_quota_ratio,
       (a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s5_bk_qtd_quota_ratio,

       (a.stage345_large_deals / d.quota) AS s345_lg_deal_quota_ratio,
       (a.stage45_large_deals / d.quota) AS s45_lg_deal_quota_ratio,
       (a.stage5_large_deals / d.quota) AS s5_lg_deal_quota_ratio,

       (f.stage_1_amount_all + f.stage_2_amount_all + f.stage_3_amount_all + f.stage_4_amount_all + f.stage_5_amount_all) / 
               (d.quota - CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) AS pipe_coverage,
               
       (d.quota - CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) /
               (f.stage_1_amount_all + f.stage_2_amount_all + f.stage_3_amount_all + f.stage_4_amount_all + f.stage_5_amount_all) AS pct_pipe_remain_to_close,

       (CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) / d.quota as bookings_pct_qtd,
       c.direct_bookings / d.quota as bookings_pct_finish
FROM d
  LEFT JOIN e ON d.fy_quarter = e.fy_quarter
  LEFT JOIN b
         ON b.dt = e.dt
        AND b.sales_div = d.sales_div
        AND b.fy_quarter = d.fy_quarter
  LEFT JOIN c
         ON c.sales_div = d.sales_div
        AND c.fy_quarter = d.fy_quarter
  LEFT JOIN a
         ON a.dt+1 = e.dt
        AND a.sales_div = d.sales_div
        AND a.fy_quarter = d.fy_quarter
  LEFT JOIN f
         ON f.dt+1 = e.dt
        AND f.sales_div = d.sales_div
        AND f.fy_quarter = d.fy_quarter  
WHERE 1=1
AND e.dt >= '2018-08-01'
AND e.dt < (SELECT qtr_start FROM src_config.zoom_quarter_mapping WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
AND a.sales_div NOT IN ('Channel', 'ISV', 'Online Team', 'UnDefined', 'API', 'Network Alliance')
AND d.quota <> 0
ORDER BY 4,1,2,3)
WITH NO SCHEMA BINDING;

COMMIT;


-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
----------- TESTING INTL Separated
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------

CREATE OR REPLACE VIEW periscope.sales_fcst_test_intl_separated AS
(

WITH a AS 
--- Opportunities
(SELECT a.dt,
       c.fy_quarter,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN 1 ELSE 0 END) AS stage_1_count,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount_usd ELSE 0 END) AS stage_1_amount,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN 1 ELSE 0 END) AS stage_2_count,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount_usd ELSE 0 END) AS stage_2_amount,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN 1 ELSE 0 END) AS stage_3_count,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount_usd ELSE 0 END) AS stage_3_amount,
       SUM(CASE WHEN a.stagename = '4. POC' THEN 1 ELSE 0 END) AS stage_4_count,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount_usd ELSE 0 END) AS stage_4_amount,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN 1 ELSE 0 END) AS stage_5_count,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount_usd ELSE 0 END) AS stage_5_amount,
       SUM(CASE WHEN a.stagename IN ('3. Solution','4. POC','5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage345_large_deals,
       SUM(CASE WHEN a.stagename IN ('4. POC','5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage45_large_deals,
       SUM(CASE WHEN a.stagename IN ('5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage5_large_deals
FROM  (SELECT a.*, b.rate, a.amount AS amount_usd ---- amount in USD
        FROM src_sfdc.opportunity_history a
        LEFT JOIN src_zuora.currency b ON (case when a.currencyisocode is null then 'USD' else a.currencyisocode end) = b.alphabeticcode
        where  a.dt - a.lastactivitydate::date < 120 OR a.dt - a.lastmodifieddate::date --- only opps that were touched within 120 days
        ) a  
  LEFT JOIN src_sfdc.account b ON a.accountid = b.id 
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
  -----lab.dw_20190311_sales_owner_division_clean_up
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount_usd > 0
AND   a.closedate between c.qtr_start AND c.qtr_end ---- only opps closing from the given quarter
AND a.id <> '0060W00000y3J2PQAU' --- excluding HSBC
GROUP BY 1,
         2,
         3),
         
b AS
---- QTD bookings
(SELECT booking_date__c as dt,
       fy_quarter,
       case when sales_div_clean is null then 'UnDefined' else sales_div_clean end as sales_div,
       SUM(bookings) OVER (PARTITION BY fy_quarter, sales_div_clean ORDER BY booking_date__c ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bookings_qtd
FROM (SELECT a.booking_date__c,
             b.fy_quarter,
             c.sales_div_clean,
             SUM(Amount__c) AS bookings
      FROM src_sfdc.bookings a
        LEFT JOIN src_config.zoom_quarter_mapping b
               ON a.booking_date__c BETWEEN b.qtr_start
              AND b.qtr_end
        LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.owner_division__c = c.sales_div_dirty
      WHERE 1 = 1
      AND   isdeleted = FALSE
      AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 18) 
            OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
            OR (Order_Type__c IN ('New','New Order') AND Amount__c < 18 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
            OR (bookingexception__c = 'Y'))
      AND   LOWER(owner_name) NOT LIKE '%integration%'
      AND   account__c <> ''
      AND   account__c IS NOT NULL
      AND a.id <> 'a1j0W000008kbLlQAI' --- excluding HSBC
      GROUP BY 1,
               2,
               3)
ORDER BY 3,1),

c AS
---- Bookings total for the quarter
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(Amount__c) AS direct_bookings
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.owner_division__c = c.sales_div_dirty
WHERE 1 = 1
AND   isdeleted = FALSE
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 18) 
      OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
      OR (Order_Type__c IN ('New','New Order') AND Amount__c < 18 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
      OR (bookingexception__c = 'Y'))
AND   lower(owner_name) NOT LIKE '%integration%'
AND   account__c <> ''
AND   account__c IS NOT NULL
AND a.id <> 'a1j0W000008kbLlQAI' --- excluding HSBC
GROUP BY 1,2
ORDER BY 1,2),

d AS
--- quota
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(gross_quota__c) AS quota
FROM src_sfdc.quota a
  LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
WHERE quota_owner_type__c = 'Segment'
and gross_quota__c > 0
GROUP BY 1,2

-- UNION
-- SELECT b.fy_quarter,
--        case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
--        SUM(gross_quota__c) AS quota
-- FROM lab.temp_quota a
--   LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
--   LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
-- WHERE quota_owner_type__c = 'Segment'
-- and gross_quota__c > 0
-- GROUP BY 1,2
),

e AS
(SELECT a.fy_quarter,
       b.dt,
       b.dt - a.qtr_start + 1 AS day_of_qtr
FROM src_config.zoom_quarter_mapping a
  LEFT JOIN src_config.zoom_calendar b
         ON b.dt BETWEEN qtr_start
        AND qtr_end),

f AS
(SELECT a.dt,
       c.fy_quarter,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount_usd ELSE 0 END) AS stage_1_amount_all,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount_usd ELSE 0 END) AS stage_2_amount_all,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount_usd ELSE 0 END) AS stage_3_amount_all,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount_usd ELSE 0 END) AS stage_4_amount_all,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount_usd ELSE 0 END) AS stage_5_amount_all
FROM  (select *, amount as amount_usd from src_sfdc.opportunity_history) a
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount_usd > 0
AND   a.closedate between c.qtr_start AND c.qtr_end ---- only opps closing from the given quarter
AND a.id <> '0060W00000y3J2PQAU' --- excluding HSBC
GROUP BY 1,
         2,
         3)

SELECT e.dt,
       a.fy_quarter,
       e.day_of_qtr,
       a.sales_div,
       d.quota,
       a.stage_1_count,
       a.stage_1_amount,
       a.stage_2_count,
       a.stage_2_amount,
       a.stage_3_count,
       a.stage_3_amount,
       a.stage_4_count,
       a.stage_4_amount,
       a.stage_5_count,
       a.stage_5_amount,
       CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
            WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
            ELSE b.bookings_qtd END as bookings_qtd,
       c.direct_bookings,
       case when a.fy_quarter like '%Q1' then e.day_of_qtr::float / 89 else e.day_of_qtr::float / 92 end as qtr_pct,
       a.stage_1_amount / d.quota AS s1_quota_ratio,
       a.stage_2_amount / d.quota AS s2_quota_ratio,
       a.stage_3_amount / d.quota AS s3_quota_ratio,
       a.stage_4_amount / d.quota AS s4_quota_ratio,
       a.stage_5_amount / d.quota AS s5_quota_ratio,
       (a.stage_4_amount + a.stage_5_amount) / d.quota AS s45_quota_ratio,
       (a.stage_1_amount + a.stage_2_amount + a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s12345_bk_qtd_quota_ratio,
       (a.stage_2_amount + a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s2345_bk_qtd_quota_ratio,
       (a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s345_bk_qtd_quota_ratio,
       (a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s45_bk_qtd_quota_ratio,
       (a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s5_bk_qtd_quota_ratio,

       (a.stage345_large_deals / d.quota) AS s345_lg_deal_quota_ratio,
       (a.stage45_large_deals / d.quota) AS s45_lg_deal_quota_ratio,
       (a.stage5_large_deals / d.quota) AS s5_lg_deal_quota_ratio,

       (f.stage_1_amount_all + f.stage_2_amount_all + f.stage_3_amount_all + f.stage_4_amount_all + f.stage_5_amount_all) / 
               (d.quota - CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) AS pipe_coverage,
               
       (d.quota - CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) /
               (f.stage_1_amount_all + f.stage_2_amount_all + f.stage_3_amount_all + f.stage_4_amount_all + f.stage_5_amount_all) AS pct_pipe_remain_to_close,

       (CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) / d.quota as bookings_pct_qtd,
       c.direct_bookings / d.quota as bookings_pct_finish
FROM d
  LEFT JOIN e ON d.fy_quarter = e.fy_quarter
  LEFT JOIN b
         ON b.dt = e.dt
        AND b.sales_div = d.sales_div
        AND b.fy_quarter = d.fy_quarter
  LEFT JOIN c
         ON c.sales_div = d.sales_div
        AND c.fy_quarter = d.fy_quarter
  LEFT JOIN a
         ON a.dt+1 = e.dt
        AND a.sales_div = d.sales_div
        AND a.fy_quarter = d.fy_quarter
  LEFT JOIN f
         ON f.dt+1 = e.dt
        AND f.sales_div = d.sales_div
        AND f.fy_quarter = d.fy_quarter  
WHERE 1=1
AND e.dt <= (SELECT MAX(booking_date__c) FROM src_sfdc.bookings)
AND e.dt >= '2018-08-01'
AND e.dt >= (SELECT qtr_start FROM src_config.zoom_quarter_mapping WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
AND a.sales_div NOT IN ('Channel', 'ISV', 'Online Team', 'UnDefined', 'API', 'Network Alliance')
ORDER BY 4,1,2,3)
WITH NO SCHEMA BINDING;

COMMIT;




-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
----------- TESTING CURRENT DAY INTL Separated
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------

CREATE OR REPLACE VIEW periscope.sales_fcst_test_curr_day_intl_separated AS
(

WITH TA AS ---- TA is same as testing curr quarter

(WITH a AS 
--- Opportunities
(SELECT a.dt,
       c.fy_quarter,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN 1 ELSE 0 END) AS stage_1_count,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount_usd ELSE 0 END) AS stage_1_amount,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN 1 ELSE 0 END) AS stage_2_count,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount_usd ELSE 0 END) AS stage_2_amount,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN 1 ELSE 0 END) AS stage_3_count,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount_usd ELSE 0 END) AS stage_3_amount,
       SUM(CASE WHEN a.stagename = '4. POC' THEN 1 ELSE 0 END) AS stage_4_count,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount_usd ELSE 0 END) AS stage_4_amount,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN 1 ELSE 0 END) AS stage_5_count,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount_usd ELSE 0 END) AS stage_5_amount,
       SUM(CASE WHEN a.stagename IN ('3. Solution','4. POC','5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage345_large_deals,
       SUM(CASE WHEN a.stagename IN ('4. POC','5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage45_large_deals,
       SUM(CASE WHEN a.stagename IN ('5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage5_large_deals
FROM  (SELECT a.*, b.rate, a.amount AS amount_usd ---- amount in USD
        FROM src_sfdc.opportunity_history a
        LEFT JOIN src_zuora.currency b ON (case when a.currencyisocode is null then 'USD' else a.currencyisocode end) = b.alphabeticcode
        where  a.dt - a.lastactivitydate::date < 120 OR a.dt - a.lastmodifieddate::date --- only opps that were touched within 120 days
        ) a  
  LEFT JOIN src_sfdc.account b ON a.accountid = b.id 
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
  -----lab.dw_20190311_sales_owner_division_clean_up
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount_usd > 0
AND   a.closedate between c.qtr_start AND c.qtr_end ---- only opps closing from the given quarter
AND a.id <> '0060W00000y3J2PQAU' --- excluding HSBC
GROUP BY 1,
         2,
         3),
         
b AS
---- QTD bookings
(SELECT booking_date__c as dt,
       fy_quarter,
       case when sales_div_clean is null then 'UnDefined' else sales_div_clean end as sales_div,
       SUM(bookings) OVER (PARTITION BY fy_quarter, sales_div_clean ORDER BY booking_date__c ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bookings_qtd
FROM (SELECT a.booking_date__c,
             b.fy_quarter,
             c.sales_div_clean,
             SUM(Amount__c) AS bookings
      FROM src_sfdc.bookings a
        LEFT JOIN src_config.zoom_quarter_mapping b
               ON a.booking_date__c BETWEEN b.qtr_start
              AND b.qtr_end
        LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.owner_division__c = c.sales_div_dirty
      WHERE 1 = 1
      AND   isdeleted = FALSE
      AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 18) 
            OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
            OR (Order_Type__c IN ('New','New Order') AND Amount__c < 18 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
            OR (bookingexception__c = 'Y'))
      AND   LOWER(owner_name) NOT LIKE '%integration%'
      AND   account__c <> ''
      AND   account__c IS NOT NULL
      AND a.id <> 'a1j0W000008kbLlQAI' --- excluding HSBC
      GROUP BY 1,
               2,
               3)
ORDER BY 3,1),

c AS
---- Bookings total for the quarter
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(Amount__c) AS direct_bookings
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.owner_division__c = c.sales_div_dirty
WHERE 1 = 1
AND   isdeleted = FALSE
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 18) 
      OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
      OR (Order_Type__c IN ('New','New Order') AND Amount__c < 18 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
      OR (bookingexception__c = 'Y'))
AND   lower(owner_name) NOT LIKE '%integration%'
AND   account__c <> ''
AND   account__c IS NOT NULL
AND a.id <> 'a1j0W000008kbLlQAI' --- excluding HSBC
GROUP BY 1,2
ORDER BY 1,2),

d AS
--- quota
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(gross_quota__c) AS quota
FROM src_sfdc.quota a
  LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
WHERE quota_owner_type__c = 'Segment'
and gross_quota__c > 0
GROUP BY 1,2

-- UNION
-- SELECT b.fy_quarter,
--        case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
--        SUM(gross_quota__c) AS quota
-- FROM lab.temp_quota a
--   LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
--   LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
-- WHERE quota_owner_type__c = 'Segment'
-- and gross_quota__c > 0
-- GROUP BY 1,2
),

e AS
(SELECT a.fy_quarter,
       b.dt,
       b.dt - a.qtr_start + 1 AS day_of_qtr
FROM src_config.zoom_quarter_mapping a
  LEFT JOIN src_config.zoom_calendar b
         ON b.dt BETWEEN qtr_start
        AND qtr_end),

f AS
(SELECT a.dt,
       c.fy_quarter,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount_usd ELSE 0 END) AS stage_1_amount_all,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount_usd ELSE 0 END) AS stage_2_amount_all,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount_usd ELSE 0 END) AS stage_3_amount_all,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount_usd ELSE 0 END) AS stage_4_amount_all,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount_usd ELSE 0 END) AS stage_5_amount_all
FROM  (select *, amount as amount_usd from src_sfdc.opportunity_history) a
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount_usd > 0
AND   a.closedate between c.qtr_start AND c.qtr_end ---- only opps closing from the given quarter
AND a.id <> '0060W00000y3J2PQAU' --- excluding HSBC
GROUP BY 1,
         2,
         3)

SELECT e.dt,
       a.fy_quarter,
       e.day_of_qtr,
       a.sales_div,
       d.quota,
       a.stage_1_count,
       a.stage_1_amount,
       a.stage_2_count,
       a.stage_2_amount,
       a.stage_3_count,
       a.stage_3_amount,
       a.stage_4_count,
       a.stage_4_amount,
       a.stage_5_count,
       a.stage_5_amount,
       CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
            WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
            ELSE b.bookings_qtd END as bookings_qtd,
       c.direct_bookings,
       case when a.fy_quarter like '%Q1' then e.day_of_qtr::float / 89 else e.day_of_qtr::float / 92 end as qtr_pct,
       a.stage_1_amount / d.quota AS s1_quota_ratio,
       a.stage_2_amount / d.quota AS s2_quota_ratio,
       a.stage_3_amount / d.quota AS s3_quota_ratio,
       a.stage_4_amount / d.quota AS s4_quota_ratio,
       a.stage_5_amount / d.quota AS s5_quota_ratio,
       (a.stage_4_amount + a.stage_5_amount) / d.quota AS s45_quota_ratio,
       (a.stage_1_amount + a.stage_2_amount + a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s12345_bk_qtd_quota_ratio,
       (a.stage_2_amount + a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s2345_bk_qtd_quota_ratio,
       (a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s345_bk_qtd_quota_ratio,
       (a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s45_bk_qtd_quota_ratio,
       (a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s5_bk_qtd_quota_ratio,

       (a.stage345_large_deals / d.quota) AS s345_lg_deal_quota_ratio,
       (a.stage45_large_deals / d.quota) AS s45_lg_deal_quota_ratio,
       (a.stage5_large_deals / d.quota) AS s5_lg_deal_quota_ratio,

       (f.stage_1_amount_all + f.stage_2_amount_all + f.stage_3_amount_all + f.stage_4_amount_all + f.stage_5_amount_all) / 
               (d.quota - CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) AS pipe_coverage,
               
       (d.quota - CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) /
               (f.stage_1_amount_all + f.stage_2_amount_all + f.stage_3_amount_all + f.stage_4_amount_all + f.stage_5_amount_all) AS pct_pipe_remain_to_close,

       (CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) / d.quota as bookings_pct_qtd,
       c.direct_bookings / d.quota as bookings_pct_finish
FROM d
  LEFT JOIN e ON d.fy_quarter = e.fy_quarter
  LEFT JOIN b
         ON b.dt = e.dt
        AND b.sales_div = d.sales_div
        AND b.fy_quarter = d.fy_quarter
  LEFT JOIN c
         ON c.sales_div = d.sales_div
        AND c.fy_quarter = d.fy_quarter
  LEFT JOIN a
         ON a.dt+1 = e.dt
        AND a.sales_div = d.sales_div
        AND a.fy_quarter = d.fy_quarter
  LEFT JOIN f
         ON f.dt+1 = e.dt
        AND f.sales_div = d.sales_div
        AND f.fy_quarter = d.fy_quarter  
WHERE 1=1
AND e.dt >= '2018-08-01'
AND e.dt >= (SELECT qtr_start FROM src_config.zoom_quarter_mapping WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
AND a.sales_div NOT IN ('Channel', 'ISV', 'Online Team', 'UnDefined', 'API', 'Network Alliance')
ORDER BY 4,1,2,3)

SELECT * FROM TA
WHERE dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))
ORDER BY 4,1,2,3
WITH NO SCHEMA BINDING;

COMMIT;


-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
----------- SAME TIME LAST QUARTER INTL Separated
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------

CREATE OR REPLACE VIEW periscope.sales_fcst_stlq_intl_separated AS
(

WITH TA AS ---- TA same as training data
(
WITH a AS 
--- Opportunities
(SELECT a.dt,
       c.fy_quarter,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN 1 ELSE 0 END) AS stage_1_count,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount_usd ELSE 0 END) AS stage_1_amount,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN 1 ELSE 0 END) AS stage_2_count,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount_usd ELSE 0 END) AS stage_2_amount,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN 1 ELSE 0 END) AS stage_3_count,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount_usd ELSE 0 END) AS stage_3_amount,
       SUM(CASE WHEN a.stagename = '4. POC' THEN 1 ELSE 0 END) AS stage_4_count,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount_usd ELSE 0 END) AS stage_4_amount,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN 1 ELSE 0 END) AS stage_5_count,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount_usd ELSE 0 END) AS stage_5_amount,
       SUM(CASE WHEN a.stagename IN ('3. Solution','4. POC','5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage345_large_deals,
       SUM(CASE WHEN a.stagename IN ('4. POC','5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage45_large_deals,
       SUM(CASE WHEN a.stagename IN ('5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage5_large_deals
FROM  (SELECT a.*, b.rate, a.amount AS amount_usd ---- amount in USD
        FROM src_sfdc.opportunity_history a
        LEFT JOIN src_zuora.currency b ON (case when a.currencyisocode is null then 'USD' else a.currencyisocode end) = b.alphabeticcode
        where  a.dt - a.lastactivitydate::date < 120 OR a.dt - a.lastmodifieddate::date --- only opps that were touched within 120 days
        ) a  
  LEFT JOIN src_sfdc.account b ON a.accountid = b.id 
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
  -----lab.dw_20190311_sales_owner_division_clean_up
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount_usd > 0
AND   a.closedate between c.qtr_start AND c.qtr_end ---- only opps closing from the given quarter
AND a.id <> '0060W00000y3J2PQAU' --- excluding HSBC
GROUP BY 1,
         2,
         3),
         
b AS
---- QTD bookings
(SELECT booking_date__c as dt,
       fy_quarter,
       case when sales_div_clean is null then 'UnDefined' else sales_div_clean end as sales_div,
       SUM(bookings) OVER (PARTITION BY fy_quarter, sales_div_clean ORDER BY booking_date__c ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bookings_qtd
FROM (SELECT a.booking_date__c,
             b.fy_quarter,
             c.sales_div_clean,
             SUM(Amount__c) AS bookings
      FROM src_sfdc.bookings a
        LEFT JOIN src_config.zoom_quarter_mapping b
               ON a.booking_date__c BETWEEN b.qtr_start
              AND b.qtr_end
        LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.owner_division__c = c.sales_div_dirty
      WHERE 1 = 1
      AND   isdeleted = FALSE
      AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 18) 
            OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
            OR (Order_Type__c IN ('New','New Order') AND Amount__c < 18 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
            OR (bookingexception__c = 'Y'))
      AND   LOWER(owner_name) NOT LIKE '%integration%'
      AND   account__c <> ''
      AND   account__c IS NOT NULL
      AND a.id <> 'a1j0W000008kbLlQAI' --- excluding HSBC
      GROUP BY 1,
               2,
               3)
ORDER BY 3,1),

c AS
---- Bookings total for the quarter
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(Amount__c) AS direct_bookings
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.owner_division__c = c.sales_div_dirty
WHERE 1 = 1
AND   isdeleted = FALSE
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 18) 
      OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
      OR (Order_Type__c IN ('New','New Order') AND Amount__c < 18 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
      OR (bookingexception__c = 'Y'))
AND   lower(owner_name) NOT LIKE '%integration%'
AND   account__c <> ''
AND   account__c IS NOT NULL
AND a.id <> 'a1j0W000008kbLlQAI' --- excluding HSBC
GROUP BY 1,2
ORDER BY 1,2),

d AS
--- quota
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(gross_quota__c) AS quota
FROM src_sfdc.quota a
  LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
WHERE quota_owner_type__c = 'Segment'
and gross_quota__c > 0
GROUP BY 1,2

-- UNION
-- SELECT b.fy_quarter,
--        case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
--        SUM(gross_quota__c) AS quota
-- FROM lab.temp_quota a
--   LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
--   LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
-- WHERE quota_owner_type__c = 'Segment'
-- and gross_quota__c > 0
-- GROUP BY 1,2
),

e AS
(SELECT a.fy_quarter,
       b.dt,
       b.dt - a.qtr_start + 1 AS day_of_qtr
FROM src_config.zoom_quarter_mapping a
  LEFT JOIN src_config.zoom_calendar b
         ON b.dt BETWEEN qtr_start
        AND qtr_end),

f AS
(SELECT a.dt,
       c.fy_quarter,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount_usd ELSE 0 END) AS stage_1_amount_all,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount_usd ELSE 0 END) AS stage_2_amount_all,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount_usd ELSE 0 END) AS stage_3_amount_all,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount_usd ELSE 0 END) AS stage_4_amount_all,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount_usd ELSE 0 END) AS stage_5_amount_all
FROM  (select *, amount as amount_usd from src_sfdc.opportunity_history) a
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount_usd > 0
AND   a.closedate between c.qtr_start AND c.qtr_end ---- only opps closing from the given quarter
AND a.id <> '0060W00000y3J2PQAU' --- excluding HSBC
GROUP BY 1,
         2,
         3)

SELECT e.dt,
       a.fy_quarter,
       e.day_of_qtr,
       a.sales_div,
       d.quota,
       a.stage_1_count,
       a.stage_1_amount,
       a.stage_2_count,
       a.stage_2_amount,
       a.stage_3_count,
       a.stage_3_amount,
       a.stage_4_count,
       a.stage_4_amount,
       a.stage_5_count,
       a.stage_5_amount,
       CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
            WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
            ELSE b.bookings_qtd END as bookings_qtd,
       c.direct_bookings,
       case when a.fy_quarter like '%Q1' then e.day_of_qtr::float / 89 else e.day_of_qtr::float / 92 end as qtr_pct,
       a.stage_1_amount / d.quota AS s1_quota_ratio,
       a.stage_2_amount / d.quota AS s2_quota_ratio,
       a.stage_3_amount / d.quota AS s3_quota_ratio,
       a.stage_4_amount / d.quota AS s4_quota_ratio,
       a.stage_5_amount / d.quota AS s5_quota_ratio,
       (a.stage_4_amount + a.stage_5_amount) / d.quota AS s45_quota_ratio,
       (a.stage_1_amount + a.stage_2_amount + a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s12345_bk_qtd_quota_ratio,
       (a.stage_2_amount + a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s2345_bk_qtd_quota_ratio,
       (a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s345_bk_qtd_quota_ratio,
       (a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s45_bk_qtd_quota_ratio,
       (a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s5_bk_qtd_quota_ratio,

       (a.stage345_large_deals / d.quota) AS s345_lg_deal_quota_ratio,
       (a.stage45_large_deals / d.quota) AS s45_lg_deal_quota_ratio,
       (a.stage5_large_deals / d.quota) AS s5_lg_deal_quota_ratio,

       (f.stage_1_amount_all + f.stage_2_amount_all + f.stage_3_amount_all + f.stage_4_amount_all + f.stage_5_amount_all) / 
               (d.quota - CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) AS pipe_coverage,
               
       (d.quota - CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) /
               (f.stage_1_amount_all + f.stage_2_amount_all + f.stage_3_amount_all + f.stage_4_amount_all + f.stage_5_amount_all) AS pct_pipe_remain_to_close,

       (CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) / d.quota as bookings_pct_qtd,
       c.direct_bookings / d.quota as bookings_pct_finish
FROM d
  LEFT JOIN e ON d.fy_quarter = e.fy_quarter
  LEFT JOIN b
         ON b.dt = e.dt
        AND b.sales_div = d.sales_div
        AND b.fy_quarter = d.fy_quarter
  LEFT JOIN c
         ON c.sales_div = d.sales_div
        AND c.fy_quarter = d.fy_quarter
  LEFT JOIN a
         ON a.dt+1 = e.dt
        AND a.sales_div = d.sales_div
        AND a.fy_quarter = d.fy_quarter
  LEFT JOIN f
         ON f.dt+1 = e.dt
        AND f.sales_div = d.sales_div
        AND f.fy_quarter = d.fy_quarter  
WHERE 1=1
AND a.sales_div NOT IN ('Channel', 'ISV', 'Online Team', 'UnDefined', 'API', 'Network Alliance')
AND d.quota <> 0
AND e.dt >= '2018-08-01'
AND e.dt < (SELECT qtr_start FROM src_config.zoom_quarter_mapping WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
ORDER BY 4,1,2,3)

SELECT * FROM TA
WHERE dt = (WITH T1 AS
                (
                  SELECT dt,
                         day_of_fiscal_quarter,
                         LAG(dt,1) OVER (ORDER BY dt) AS dt_stlq
                  FROM src_config.zoom_calendar
                  WHERE day_of_fiscal_quarter = (SELECT day_of_fiscal_quarter
                                                 FROM src_config.zoom_calendar
                                                 WHERE dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))
                  OR    (day_of_fiscal_quarter = 89 AND TO_CHAR(dt,'mm-dd') IN ('07-28','07-29','07-30','07-31') AND dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings)) --- if previous quarter has feb
                  OR    (day_of_fiscal_quarter = 90 AND TO_CHAR(dt,'mm-dd') IN ('07-28','07-29','07-30','07-31') AND dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))--- if previous quarter has feb
                  OR    (day_of_fiscal_quarter = 91 AND TO_CHAR(dt,'mm-dd') IN ('07-28','07-29','07-30','07-31') AND dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))--- if previous quarter has feb
                  OR    (day_of_fiscal_quarter = 92 AND TO_CHAR(dt,'mm-dd') IN ('07-28','07-29','07-30','07-31') AND dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))--- if previous quarter has feb 
                  ORDER BY dt ASC)
                SELECT 
                --        dt,
                --        day_of_fiscal_quarter,
                       dt_stlq
                FROM T1
                WHERE dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings)))
ORDER BY 4,1,2,3
WITH NO SCHEMA BINDING;

COMMIT;



-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
----------- SAME TIME LAST 4 QUARTERS AVG INTL Separated
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------
-----------


CREATE OR REPLACE VIEW periscope.sales_fcst_stl4q_avg_intl_separated AS
(

WITH TA AS ---- TA same as training data
(
WITH a AS 
--- Opportunities
(SELECT a.dt,
       c.fy_quarter,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN 1 ELSE 0 END) AS stage_1_count,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount_usd ELSE 0 END) AS stage_1_amount,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN 1 ELSE 0 END) AS stage_2_count,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount_usd ELSE 0 END) AS stage_2_amount,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN 1 ELSE 0 END) AS stage_3_count,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount_usd ELSE 0 END) AS stage_3_amount,
       SUM(CASE WHEN a.stagename = '4. POC' THEN 1 ELSE 0 END) AS stage_4_count,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount_usd ELSE 0 END) AS stage_4_amount,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN 1 ELSE 0 END) AS stage_5_count,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount_usd ELSE 0 END) AS stage_5_amount,
       SUM(CASE WHEN a.stagename IN ('3. Solution','4. POC','5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage345_large_deals,
       SUM(CASE WHEN a.stagename IN ('4. POC','5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage45_large_deals,
       SUM(CASE WHEN a.stagename IN ('5. Contract') and a.amount_usd > 10000 THEN a.amount_usd ELSE 0 END) stage5_large_deals
FROM  (SELECT a.*, b.rate, a.amount AS amount_usd ---- amount in USD
        FROM src_sfdc.opportunity_history a
        LEFT JOIN src_zuora.currency b ON (case when a.currencyisocode is null then 'USD' else a.currencyisocode end) = b.alphabeticcode
        where  a.dt - a.lastactivitydate::date < 120 OR a.dt - a.lastmodifieddate::date --- only opps that were touched within 120 days
        ) a  
  LEFT JOIN src_sfdc.account b ON a.accountid = b.id 
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount_usd > 0
AND   a.closedate between c.qtr_start AND c.qtr_end ---- only opps closing from the given quarter
AND a.id <> '0060W00000y3J2PQAU' --- excluding HSBC
GROUP BY 1,
         2,
         3),
         
b AS
---- QTD bookings
(SELECT booking_date__c as dt,
       fy_quarter,
       case when sales_div_clean is null then 'UnDefined' else sales_div_clean end as sales_div,
       SUM(bookings) OVER (PARTITION BY fy_quarter, sales_div_clean ORDER BY booking_date__c ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bookings_qtd
FROM (SELECT a.booking_date__c,
             b.fy_quarter,
             c.sales_div_clean,
             SUM(Amount__c) AS bookings
      FROM src_sfdc.bookings a
        LEFT JOIN src_config.zoom_quarter_mapping b
               ON a.booking_date__c BETWEEN b.qtr_start
              AND b.qtr_end
        LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.owner_division__c = c.sales_div_dirty
      WHERE 1 = 1
      AND   isdeleted = FALSE
      AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 18) 
            OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
            OR (Order_Type__c IN ('New','New Order') AND Amount__c < 18 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
            OR (bookingexception__c = 'Y'))
      AND   LOWER(owner_name) NOT LIKE '%integration%'
      AND   account__c <> ''
      AND   account__c IS NOT NULL
      AND a.id <> 'a1j0W000008kbLlQAI' --- excluding HSBC
      GROUP BY 1,
               2,
               3)
ORDER BY 3,1),

c AS
---- Bookings total for the quarter
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(Amount__c) AS direct_bookings
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.owner_division__c = c.sales_div_dirty
WHERE 1 = 1
AND   isdeleted = FALSE
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 18) 
      OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
      OR (Order_Type__c IN ('New','New Order') AND Amount__c < 18 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
      OR (bookingexception__c = 'Y'))
AND   lower(owner_name) NOT LIKE '%integration%'
AND   account__c <> ''
AND   account__c IS NOT NULL
AND a.id <> 'a1j0W000008kbLlQAI' --- excluding HSBC
GROUP BY 1,2
ORDER BY 1,2),

d AS
--- quota
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(gross_quota__c) AS quota
FROM src_sfdc.quota a
  LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
WHERE quota_owner_type__c = 'Segment'
and gross_quota__c > 0
GROUP BY 1,2

-- UNION
-- SELECT b.fy_quarter,
--        case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
--        SUM(gross_quota__c) AS quota
-- FROM lab.temp_quota a
--   LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
--   LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
-- WHERE quota_owner_type__c = 'Segment'
-- and gross_quota__c > 0
-- GROUP BY 1,2
),

e AS
(SELECT a.fy_quarter,
       b.dt,
       b.dt - a.qtr_start + 1 AS day_of_qtr
FROM src_config.zoom_quarter_mapping a
  LEFT JOIN src_config.zoom_calendar b
         ON b.dt BETWEEN qtr_start
        AND qtr_end),

f AS
(SELECT a.dt,
       c.fy_quarter,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount_usd ELSE 0 END) AS stage_1_amount_all,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount_usd ELSE 0 END) AS stage_2_amount_all,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount_usd ELSE 0 END) AS stage_3_amount_all,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount_usd ELSE 0 END) AS stage_4_amount_all,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount_usd ELSE 0 END) AS stage_5_amount_all
FROM  (select *, amount as amount_usd from src_sfdc.opportunity_history) a
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount_usd > 0
AND   a.closedate between c.qtr_start AND c.qtr_end ---- only opps closing from the given quarter
AND a.id <> '0060W00000y3J2PQAU' --- excluding HSBC
GROUP BY 1,
         2,
         3)

SELECT e.dt,
       a.fy_quarter,
       e.day_of_qtr,
       a.sales_div,
       d.quota,
       a.stage_1_count,
       a.stage_1_amount,
       a.stage_2_count,
       a.stage_2_amount,
       a.stage_3_count,
       a.stage_3_amount,
       a.stage_4_count,
       a.stage_4_amount,
       a.stage_5_count,
       a.stage_5_amount,
       CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
            WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
            ELSE b.bookings_qtd END as bookings_qtd,
       c.direct_bookings,
       case when a.fy_quarter like '%Q1' then e.day_of_qtr::float / 89 else e.day_of_qtr::float / 92 end as qtr_pct,
       a.stage_1_amount / d.quota AS s1_quota_ratio,
       a.stage_2_amount / d.quota AS s2_quota_ratio,
       a.stage_3_amount / d.quota AS s3_quota_ratio,
       a.stage_4_amount / d.quota AS s4_quota_ratio,
       a.stage_5_amount / d.quota AS s5_quota_ratio,
       (a.stage_4_amount + a.stage_5_amount) / d.quota AS s45_quota_ratio,
       (a.stage_1_amount + a.stage_2_amount + a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s12345_bk_qtd_quota_ratio,
       (a.stage_2_amount + a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s2345_bk_qtd_quota_ratio,
       (a.stage_3_amount + a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s345_bk_qtd_quota_ratio,
       (a.stage_4_amount + a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s45_bk_qtd_quota_ratio,
       (a.stage_5_amount +      
            CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END )/ d.quota AS s5_bk_qtd_quota_ratio,

       (a.stage345_large_deals / d.quota) AS s345_lg_deal_quota_ratio,
       (a.stage45_large_deals / d.quota) AS s45_lg_deal_quota_ratio,
       (a.stage5_large_deals / d.quota) AS s5_lg_deal_quota_ratio,

       (f.stage_1_amount_all + f.stage_2_amount_all + f.stage_3_amount_all + f.stage_4_amount_all + f.stage_5_amount_all) / 
               (d.quota - CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) AS pipe_coverage,
               
       (d.quota - CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) /
               (f.stage_1_amount_all + f.stage_2_amount_all + f.stage_3_amount_all + f.stage_4_amount_all + f.stage_5_amount_all) AS pct_pipe_remain_to_close,

       (CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) / d.quota as bookings_pct_qtd,
       c.direct_bookings / d.quota as bookings_pct_finish
FROM d
  LEFT JOIN e ON d.fy_quarter = e.fy_quarter
  LEFT JOIN b
         ON b.dt = e.dt
        AND b.sales_div = d.sales_div
        AND b.fy_quarter = d.fy_quarter
  LEFT JOIN c
         ON c.sales_div = d.sales_div
        AND c.fy_quarter = d.fy_quarter
  LEFT JOIN a
         ON a.dt+1 = e.dt
        AND a.sales_div = d.sales_div
        AND a.fy_quarter = d.fy_quarter
  LEFT JOIN f
         ON f.dt+1 = e.dt
        AND f.sales_div = d.sales_div
        AND f.fy_quarter = d.fy_quarter  
WHERE 1=1
AND a.sales_div NOT IN ('Channel', 'ISV', 'Online Team', 'UnDefined', 'API', 'Network Alliance')
AND d.quota <> 0
AND e.dt >= '2018-08-01'
AND e.dt < (SELECT qtr_start FROM src_config.zoom_quarter_mapping WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
ORDER BY 4,1,2,3)

SELECT MIN(dt) AS dt,
       'STL4Q' AS fy_quarter,
       AVG(day_of_qtr) AS day_of_qtr,
       sales_div,
       AVG(quota) AS quota,
       AVG(stage_1_count) AS stage_1_count,
       AVG(stage_1_amount) AS stage_1_amount,
       AVG(stage_2_count) AS stage_2_count,
       AVG(stage_2_amount) AS stage_2_amount,
       AVG(stage_3_count) AS stage_3_count,
       AVG(stage_3_amount) AS stage_3_amount,
       AVG(stage_4_count) AS stage_4_count,
       AVG(stage_4_amount) AS stage_4_amount,
       AVG(stage_5_count) AS stage_5_count,
       AVG(stage_5_amount) AS stage_5_amount,
       AVG(bookings_qtd) AS bookings_qtd,
       AVG(direct_bookings) AS direct_bookings,
       AVG(qtr_pct) AS qtr_pct,
       AVG(s1_quota_ratio) AS s1_quota_ratio,
       AVG(s2_quota_ratio) AS s2_quota_ratio,
       AVG(s3_quota_ratio) AS s3_quota_ratio,
       AVG(s4_quota_ratio) AS s4_quota_ratio,
       AVG(s5_quota_ratio) AS s5_quota_ratio,
       AVG(s45_quota_ratio) AS s45_quota_ratio,
       AVG(s12345_bk_qtd_quota_ratio) AS s12345_bk_qtd_quota_ratio,
       AVG(s2345_bk_qtd_quota_ratio) AS s2345_bk_qtd_quota_ratio,
       AVG(s345_bk_qtd_quota_ratio) AS s345_bk_qtd_quota_ratio,
       AVG(s45_bk_qtd_quota_ratio) AS s45_bk_qtd_quota_ratio,
       AVG(s5_bk_qtd_quota_ratio) AS s5_bk_qtd_quota_ratio,
       AVG(s345_lg_deal_quota_ratio) AS s345_lg_deal_quota_ratio,
       AVG(s45_lg_deal_quota_ratio) AS s45_lg_deal_quota_ratio,
       AVG(s5_lg_deal_quota_ratio) AS s5_lg_deal_quota_ratio,
       AVG(pipe_coverage) AS pipe_coverage,
       AVG(pct_pipe_remain_to_close) AS pct_pipe_remain_to_close,
       AVG(bookings_pct_qtd) AS bookings_pct_qtd,
       AVG(bookings_pct_finish) AS bookings_pct_finish
FROM TA
WHERE dt IN (WITH T1 AS
             (
               SELECT dt,
                      day_of_fiscal_quarter,
                      LAG(dt,1) OVER (ORDER BY dt) AS dt_stl1q,
                      LAG(dt,2) OVER (ORDER BY dt) AS dt_stl2q,
                      LAG(dt,3) OVER (ORDER BY dt) AS dt_stl3q,
                      LAG(dt,4) OVER (ORDER BY dt) AS dt_stl4q
               FROM src_config.zoom_calendar
               WHERE day_of_fiscal_quarter = (SELECT day_of_fiscal_quarter
                                              FROM src_config.zoom_calendar
                                              WHERE dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))
               OR    (day_of_fiscal_quarter = 89 AND TO_CHAR(dt,'mm-dd') IN ('07-28','07-29','07-30','07-31') AND dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))--- if previous quarter has feb
               OR    (day_of_fiscal_quarter = 90 AND TO_CHAR(dt,'mm-dd') IN ('07-28','07-29','07-30','07-31') AND dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))--- if previous quarter has feb
               OR    (day_of_fiscal_quarter = 91 AND TO_CHAR(dt,'mm-dd') IN ('07-28','07-29','07-30','07-31') AND dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))--- if previous quarter has feb
               OR    (day_of_fiscal_quarter = 92 AND TO_CHAR(dt,'mm-dd') IN ('07-28','07-29','07-30','07-31') AND dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))--- if previous quarter has feb 
               ORDER BY dt ASC
             )
             SELECT dt_stl1q
             FROM T1
             WHERE dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings)
             UNION
             SELECT dt_stl2q
             FROM T1
             WHERE dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings)
             UNION
             SELECT dt_stl3q
             FROM T1
             WHERE dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings)
             UNION
             SELECT dt_stl4q
             FROM T1
             WHERE dt = (SELECT MAX(booking_date__c) FROM src_sfdc.bookings))
GROUP BY sales_div
ORDER BY 4,1,2,3)
WITH NO SCHEMA BINDING;

COMMIT;



SELECT *
FROM periscope.sales_fcst_train_intl_separated;

SELECT *
FROM periscope.sales_fcst_test_intl_separated;

SELECT *
FROM periscope.sales_fcst_test_curr_day_intl_separated;

SELECT *
FROM periscope.sales_fcst_stlq_intl_separated;

SELECT *
FROM periscope.sales_fcst_stl4q_avg_intl_separated;
