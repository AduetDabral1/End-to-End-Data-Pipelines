
/*
1. What are the peak revenue-generating hours of the day?

Business Problem - Taxi operators want to identify the hours that generate the most revenue to optimize driver allocation and surge pricing strategies.
*/

SELECT
    d.pick_hour,
    COUNT(*) AS total_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_revenue_per_trip
FROM fact_table f
JOIN datetime_dim d
    ON f.datetime_id = d.datetime_id
GROUP BY d.pick_hour
ORDER BY total_revenue DESC;

/*
2. Which payment method contributes the highest revenue?

Business Problem - Understand customer payment preferences and evaluate the impact of digital versus cash transactions.
*/

SELECT
    p.payment_type_name,
    COUNT(*) AS total_trips,
    ROUND(SUM(f.total_amount), 2) AS revenue,
    ROUND(AVG(f.tip_amount), 2) AS avg_tip
FROM fact_table f
JOIN payment_type_dim p
    ON f.payment_type_id = p.payment_type_id
GROUP BY p.payment_type_name
ORDER BY revenue DESC;

/*
3. Which weekdays generate the highest revenue?

Business Problem - Determine which days contribute most to business performance for staffing and operational planning.
*/

SELECT
    d.pick_weekday,
    COUNT(*) AS total_trips,
    ROUND(SUM(f.total_amount), 2) AS revenue
FROM fact_table f
JOIN datetime_dim d
    ON f.datetime_id = d.datetime_id
GROUP BY d.pick_weekday
ORDER BY revenue DESC;

/*
4. Which rate code produces the highest average fare?

Business Problem - Identify premium trip categories and evaluate profitability of airport and negotiated rides.
*/

SELECT
    r.rate_code_name,
    COUNT(*) AS trips,
    ROUND(AVG(f.fare_amount), 2) AS avg_fare,
    ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM fact_table f
JOIN rate_code_dim r
    ON f.rate_code_id = r.rate_code_id
GROUP BY r.rate_code_name
ORDER BY avg_fare DESC;

/*
5. What is the relationship between trip distance and revenue?

Business Problem - Analyze whether longer trips proportionally increase profitability.
*/

SELECT
    CASE
        WHEN td.trip_distance < 2 THEN '0-2 Miles'
        WHEN td.trip_distance < 5 THEN '2-5 Miles'
        WHEN td.trip_distance < 10 THEN '5-10 Miles'
        ELSE '10+ Miles'
    END AS distance_bucket,
    COUNT(*) AS trips,
    ROUND(AVG(f.total_amount),2) AS avg_revenue,
    ROUND(SUM(f.total_amount),2) AS total_revenue
FROM fact_table f
JOIN trip_distance_dim td
    ON f.trip_distance_id = td.trip_distance_id
GROUP BY distance_bucket
ORDER BY total_revenue DESC;