USE datawarehouse;

-- QUERY 1


WITH RankedProducts AS (

    SELECT 
        p.product_name,
        MONTH(t.date) AS month,
        YEAR(t.date) AS year,
        SUM(sf.total_sale) AS total_revenue,
        CASE
            WHEN DAYOFWEEK(t.date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        
        ROW_NUMBER() OVER (
        
            PARTITION BY MONTH(t.date), YEAR(t.date), 
            
            CASE WHEN DAYOFWEEK(t.date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END
            
            ORDER BY SUM(sf.total_sale) DESC
            
        ) AS product_rank
        
    FROM sales_fact sf
    
    JOIN time_dim t ON sf.time_id = t.time_id
    
    JOIN products_dim p ON sf.product_id = p.product_id
    
    WHERE YEAR(t.date) = 2019
    
    GROUP BY p.product_name, MONTH(t.date), YEAR(t.date), day_type
)

SELECT product_name, month, year, total_revenue, day_type

FROM RankedProducts

WHERE product_rank <= 5

ORDER BY month, year, day_type, total_revenue DESC;





-- QUERY 2


WITH QuarterlyRevenue AS (

    SELECT 
    
        p.store_name,
        QUARTER(t.date) AS quarter,
        SUM(sf.total_sale) AS total_revenue
        
    FROM sales_fact sf
    
    JOIN time_dim t ON sf.time_id = t.time_id
    
    JOIN products_dim p ON sf.product_id = p.product_id
    
    WHERE YEAR(t.date) = 2019
    GROUP BY p.store_name, QUARTER(t.date)
    
),

GrowthRate AS (

    SELECT 
    
        store_name,
        
        quarter,
        
        total_revenue,
        
        LAG(total_revenue) OVER (PARTITION BY store_name ORDER BY quarter) AS previous_quarter_revenue
        
    FROM QuarterlyRevenue
)

SELECT 
    store_name,
    quarter,
    total_revenue,
    previous_quarter_revenue,
    
    CASE 
        WHEN previous_quarter_revenue IS NOT NULL THEN 
        
            (total_revenue - previous_quarter_revenue) / previous_quarter_revenue * 100
            
        ELSE 
            NULL
    END AS growth_rate
    
FROM GrowthRate

ORDER BY store_name, quarter;



-- QUERY 3


SELECT 
    p.store_name,

    p.supplier_name,

    p.product_name,

    SUM(sf.total_sale) AS total_sales

FROM sales_fact sf
JOIN products_dim p ON sf.product_id = p.product_id

GROUP BY p.store_name, p.supplier_name, p.product_name

ORDER BY p.store_name, p.supplier_name, total_sales DESC

LIMIT 1000;





-- QUERY 4

SELECT 

    p.product_name,

    CASE

        WHEN MONTH(t.date) BETWEEN 3 AND 5 THEN 'Spring'

        WHEN MONTH(t.date) BETWEEN 6 AND 8 THEN 'Summer'

        WHEN MONTH(t.date) BETWEEN 9 AND 11 THEN 'Fall'

        WHEN MONTH(t.date) = 12 OR MONTH(t.date) = 1 OR MONTH(t.date) = 2 THEN 'Winter'

    END AS season,

    SUM(sf.total_sale) AS total_sales

FROM sales_fact sf

JOIN time_dim t ON sf.time_id = t.time_id

JOIN products_dim p ON sf.product_id = p.product_id

GROUP BY p.product_name, season

ORDER BY p.product_name, season;





-- QUERY 5

WITH MonthlyRevenue AS (

    SELECT 

        p.store_name,
        
        
        p.supplier_name,
        
        MONTH(t.date) AS month,
        
        YEAR(t.date) AS year,
        
        SUM(sf.total_sale) AS total_revenue
   
   FROM sales_fact sf
   
   JOIN time_dim t ON sf.time_id = t.time_id
   
   JOIN products_dim p ON sf.product_id = p.product_id
   
   GROUP BY p.store_name, p.supplier_name, MONTH(t.date), YEAR(t.date)
),


RevenueWithLag AS (

    SELECT 


        store_name,
        supplier_name,

        month,

        year,

        total_revenue,

        LAG(total_revenue) OVER (
            PARTITION BY store_name, supplier_name

            ORDER BY year, month

        ) AS previous_month_revenue
    FROM MonthlyRevenue

)
SELECT 

    store_name,

    supplier_name,

    CONCAT(year, '-', month) AS period,

    total_revenue,
    previous_month_revenue,

    CASE 
        WHEN previous_month_revenue IS NOT NULL THEN 

            (total_revenue - previous_month_revenue) / previous_month_revenue * 100

        ELSE 

            NULL

    END AS revenue_volatility

FROM RevenueWithLag

ORDER BY store_name, supplier_name, year, month;











-- QUERY 6

WITH ProductPairs AS (
    SELECT 

        p1.product_name AS product_1,

        p2.product_name AS product_2,
        COUNT(*) AS pair_frequency

    FROM sales_fact sf1
    JOIN sales_fact sf2 ON sf1.order_id = sf2.order_id AND sf1.product_id < sf2.product_id

    JOIN products_dim p1 ON sf1.product_id = p1.product_id

    JOIN products_dim p2 ON sf2.product_id = p2.product_id

    GROUP BY p1.product_name, p2.product_name
),

RankedPairs AS (
    SELECT 
        product_1,

        product_2,
        pair_frequency,

        ROW_NUMBER() OVER (ORDER BY pair_frequency DESC) AS pair_rank


    FROM ProductPairs

)
SELECT 

    product_1,

    product_2,

    pair_frequency

FROM RankedPairs

WHERE pair_rank <= 5;











-- QUERY 7

SELECT 

    p.store_name,
    p.supplier_name,

    p.product_name,
    YEAR(t.date) AS year,
    SUM(sf.total_sale) AS total_revenue

FROM sales_fact sf

JOIN time_dim t ON sf.time_id = t.time_id

JOIN products_dim p ON sf.product_id = p.product_id
GROUP BY ROLLUP(p.store_name, p.supplier_name, p.product_name, YEAR(t.date))

ORDER BY p.store_name, p.supplier_name, p.product_name, year;











-- QUERY 8



SELECT 
    p.product_name,

    SUM(CASE WHEN MONTH(t.date) BETWEEN 1 AND 6 THEN sf.total_sale ELSE 0 END) AS revenue_h1,
   
   SUM(CASE WHEN MONTH(t.date) BETWEEN 1 AND 6 THEN sf.quantity_ordered ELSE 0 END) AS quantity_h1,
   
   SUM(CASE WHEN MONTH(t.date) BETWEEN 7 AND 12 THEN sf.total_sale ELSE 0 END) AS revenue_h2,
   
   SUM(CASE WHEN MONTH(t.date) BETWEEN 7 AND 12 THEN sf.quantity_ordered ELSE 0 END) AS quantity_h2,
   
   SUM(sf.total_sale) AS total_revenue_year,
   
   SUM(sf.quantity_ordered) AS total_quantity_year

FROM sales_fact sf

JOIN time_dim t ON sf.time_id = t.time_id

JOIN products_dim p ON sf.product_id = p.product_id

GROUP BY p.product_name

ORDER BY total_revenue_year DESC;







-- QUERY 9



WITH DailyAvgSales AS (

    SELECT 

        p.product_name,

        t.date,

        SUM(sf.total_sale) AS daily_total_sales,

        AVG(SUM(sf.total_sale)) OVER (PARTITION BY p.product_name) AS daily_avg_sales

    FROM sales_fact sf

    JOIN time_dim t ON sf.time_id = t.time_id

    JOIN products_dim p ON sf.product_id = p.product_id

    GROUP BY p.product_name, t.date
),

OutlierFlagged AS (

    SELECT 

        product_name,

        date,

        daily_total_sales,

        daily_avg_sales,
        CASE 

            WHEN daily_total_sales > 2 * daily_avg_sales THEN 'Outlier'

            ELSE 'Normal'

        END AS sales_flag

    FROM DailyAvgSales
)

SELECT 

    product_name,

    date,

    daily_total_sales,

    daily_avg_sales,

    sales_flag
FROM OutlierFlagged

ORDER BY product_name, date;






-- QUERY 10

CREATE VIEW STORE_QUARTERLY_SALES AS

SELECT 

    p.store_name,

    QUARTER(t.date) AS quarter,

    YEAR(t.date) AS year,

    SUM(sf.total_sale) AS total_sales

FROM sales_fact sf

JOIN time_dim t ON sf.time_id = t.time_id

JOIN products_dim p ON sf.product_id = p.product_id

GROUP BY p.store_name, QUARTER(t.date), YEAR(t.date)

ORDER BY p.store_name, year, quarter;