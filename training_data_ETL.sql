
-----------
-----------
-----------
----------- TRAINING
-----------
-----------
-----------

WITH a AS 
--- Opportunities
(SELECT a.dt,
       c.fy_quarter,
       a.dt - c.qtr_start + 1 AS day_of_qtr,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN 1 ELSE 0 END) AS stage_1_count,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount ELSE 0 END) AS stage_1_amount,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN 1 ELSE 0 END) AS stage_2_count,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount ELSE 0 END) AS stage_2_amount,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN 1 ELSE 0 END) AS stage_3_count,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount ELSE 0 END) AS stage_3_amount,
       SUM(CASE WHEN a.stagename = '4. POC' THEN 1 ELSE 0 END) AS stage_4_count,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount ELSE 0 END) AS stage_4_amount,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN 1 ELSE 0 END) AS stage_5_count,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount ELSE 0 END) AS stage_5_amount
FROM src_sfdc.opportunity_history a
  LEFT JOIN src_sfdc.account b ON a.accountid = b.id
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND   a.amount > 0
AND   a.dt < (SELECT qtr_start
             FROM src_config.zoom_quarter_mapping
             WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
GROUP BY 1,
         2,
         3,
         4),
         
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
      AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 17) 
            OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
            OR (Order_Type__c IN ('New','New Order') AND Amount__c < 17 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
            OR (bookingexception__c = 'Y'))
      AND   LOWER(owner_name) NOT LIKE '%integration%'
      AND   account__c <> ''
      AND   account__c IS NOT NULL
      AND   a.booking_date__c >= '2018-02-01'
      AND   a.booking_date__c < (SELECT qtr_start
                                FROM src_config.zoom_quarter_mapping
                                WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
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
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 17) 
      OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
      OR (Order_Type__c IN ('New','New Order') AND Amount__c < 17 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
      OR (bookingexception__c = 'Y'))
AND   lower(owner_name) NOT LIKE '%integration%'
AND   account__c <> ''
AND   account__c IS NOT NULL
AND   a.booking_date__c >= '2018-02-01'
AND   a.booking_date__c < (SELECT qtr_start
                          FROM src_config.zoom_quarter_mapping
                          WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
GROUP BY 1,2
ORDER BY 1,2),

d AS
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(quota__c) AS quota
FROM src_sfdc.quota a
  LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
WHERE quota_owner_type__c = 'Segment'
GROUP BY 1,2)

SELECT a.dt,
       a.fy_quarter,
       a.day_of_qtr,
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
       case when a.fy_quarter like '%Q1' then a.day_of_qtr::float / 89 else a.day_of_qtr::float / 92 end as qtr_pct,
       a.stage_1_amount / d.quota AS s1_quota_ratio,
       a.stage_2_amount / d.quota AS s2_quota_ratio,
       a.stage_3_amount / d.quota AS s3_quota_ratio,
       a.stage_4_amount / d.quota AS s4_quota_ratio,
       a.stage_5_amount / d.quota AS s5_quota_ratio,
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
       (CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) / d.quota as bookings_pct_qtd,
       c.direct_bookings / d.quota as bookings_pct_finish
FROM a
  LEFT JOIN b
         ON a.dt = b.dt
        AND a.sales_div = b.sales_div
        AND a.fy_quarter = b.fy_quarter
  LEFT JOIN c
         ON a.sales_div = c.sales_div
        AND a.fy_quarter = c.fy_quarter
  LEFT JOIN d
         ON a.sales_div = d.sales_div
        AND a.fy_quarter = d.fy_quarter
WHERE 1=1
AND a.dt >= '2018-02-01'
AND a.dt < (SELECT qtr_start FROM src_config.zoom_quarter_mapping WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
AND a.sales_div NOT IN ('Channel', 'ISV', 'Online Team', 'UnDefined')
AND d.quota <> 0
ORDER BY 4,1,2,3;




-----------
-----------
-----------
----------- TESTING
-----------
-----------
-----------


WITH a AS 
--- Opportunities
(SELECT a.dt,
       c.fy_quarter,
       a.dt - c.qtr_start + 1 AS day_of_qtr,
       case when d.sales_div_clean is null then 'UnDefined' else d.sales_div_clean end AS sales_div,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN 1 ELSE 0 END) AS stage_1_count,
       SUM(CASE WHEN a.stagename = '1. Qualification' THEN a.amount ELSE 0 END) AS stage_1_amount,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN 1 ELSE 0 END) AS stage_2_count,
       SUM(CASE WHEN a.stagename = '2. Discovery' THEN a.amount ELSE 0 END) AS stage_2_amount,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN 1 ELSE 0 END) AS stage_3_count,
       SUM(CASE WHEN a.stagename = '3. Solution' THEN a.amount ELSE 0 END) AS stage_3_amount,
       SUM(CASE WHEN a.stagename = '4. POC' THEN 1 ELSE 0 END) AS stage_4_count,
       SUM(CASE WHEN a.stagename = '4. POC' THEN a.amount ELSE 0 END) AS stage_4_amount,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN 1 ELSE 0 END) AS stage_5_count,
       SUM(CASE WHEN a.stagename = '5. Contract' THEN a.amount ELSE 0 END) AS stage_5_amount
FROM src_sfdc.opportunity_history a
  LEFT JOIN src_sfdc.account b ON a.accountid = b.id
  LEFT JOIN src_config.zoom_quarter_mapping c
         ON a.dt BETWEEN c.qtr_start
        AND c.qtr_end
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up d ON a.owner_division__c = d.sales_div_dirty
WHERE 1 = 1
AND   a.isdeleted = FALSE
AND   a.isclosed = FALSE
AND a.amount > 0
AND   a.dt >= (SELECT qtr_start
             FROM src_config.zoom_quarter_mapping
             WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
GROUP BY 1,
         2,
         3,
         4),
         
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
      AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 17) 
            OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
            OR (Order_Type__c IN ('New','New Order') AND Amount__c < 17 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
            OR (bookingexception__c = 'Y'))
      AND   LOWER(owner_name) NOT LIKE '%integration%'
      AND   account__c <> ''
      AND   account__c IS NOT NULL
      AND   a.booking_date__c >= '2018-02-01'
      AND   a.booking_date__c >= (SELECT qtr_start
                                FROM src_config.zoom_quarter_mapping
                                WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
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
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 17) 
      OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) 
      OR (Order_Type__c IN ('New','New Order') AND Amount__c < 17 AND Coupon__c <> '' AND coupon__c IS NOT NULL) 
      OR (bookingexception__c = 'Y'))
AND   lower(owner_name) NOT LIKE '%integration%'
AND   account__c <> ''
AND   account__c IS NOT NULL
AND   a.booking_date__c >= '2018-02-01'
AND   a.booking_date__c >= (SELECT qtr_start
                          FROM src_config.zoom_quarter_mapping
                          WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
GROUP BY 1,2
ORDER BY 1,2),

d AS
(SELECT b.fy_quarter,
       case when c.sales_div_clean is null then 'UnDefined' else c.sales_div_clean end as sales_div,
       SUM(quota__c) AS quota
FROM src_sfdc.quota a
  LEFT JOIN src_config.zoom_quarter_mapping b ON a.start_date__c::date = b.qtr_start
  LEFT JOIN lab.dw_20190311_sales_owner_division_clean_up c ON a.email__c = c.sales_div_dirty
WHERE quota_owner_type__c = 'Segment'
GROUP BY 1,2)

SELECT a.dt,
       a.fy_quarter,
       a.day_of_qtr,
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
       case when a.fy_quarter like '%Q1' then a.day_of_qtr::float / 89 else a.day_of_qtr::float / 92 end as qtr_pct,
       a.stage_1_amount / d.quota AS s1_quota_ratio,
       a.stage_2_amount / d.quota AS s2_quota_ratio,
       a.stage_3_amount / d.quota AS s3_quota_ratio,
       a.stage_4_amount / d.quota AS s4_quota_ratio,
       a.stage_5_amount / d.quota AS s5_quota_ratio,
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
       (CASE WHEN b.bookings_qtd IS NULL AND MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) IS NULL THEN 0
                 WHEN b.bookings_qtd IS NULL THEN MAX(b.bookings_qtd) OVER (PARTITION BY a.fy_quarter, a.sales_div ORDER BY a.sales_div, a.dt ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
                 ELSE b.bookings_qtd END ) / d.quota as bookings_pct_qtd,
       c.direct_bookings / d.quota as bookings_pct_finish
FROM a
  LEFT JOIN b
         ON a.dt = b.dt
        AND a.sales_div = b.sales_div
        AND a.fy_quarter = b.fy_quarter
  LEFT JOIN c
         ON a.sales_div = c.sales_div
        AND a.fy_quarter = c.fy_quarter
  LEFT JOIN d
         ON a.sales_div = d.sales_div
        AND a.fy_quarter = d.fy_quarter
WHERE a.dt >= (SELECT qtr_start
                          FROM src_config.zoom_quarter_mapping
                          WHERE CURRENT_DATE BETWEEN qtr_start AND qtr_end)
AND a.sales_div NOT IN ('Channel', 'ISV', 'Online Team', 'UnDefined')
ORDER BY 4,1,2,3;