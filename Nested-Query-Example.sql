-- This query is from the Qwiklab [Weather Data in BigQuery](https://google.qwiklabs.com/focuses/609?parent=catalog#step9)


SELECT
    descriptor,
    sum(complaint_count) as total_complaint_count,
    count(temperature) as data_count,
    ROUND(corr(temperature, avg_count),3) AS corr_count,
    ROUND(corr(temperature, avg_pct_count),3) AS corr_pct
From (
    SELECT
        avg(pct_count) as avg_pct_count,
        avg(day_count) as avg_count,
        sum(day_count) as complaint_count,
        descriptor,
        temperature
    FROM (
        SELECT
            DATE(timestamp) AS date,
            temperature
        FROM
            demos.nyc_weather) a
    JOIN (
        SELECT x.date, descriptor, day_count, day_count / all_calls_count as pct_count
        FROM
            (SELECT
                DATE(created_date) AS date,
                concat(complaint_type, ": ", descriptor) as descriptor,
                COUNT(*) AS day_count
            FROM
                `bigquery-public-data.new_york.311_service_requests`
            GROUP BY
                date,
                descriptor) x
        JOIN (
            SELECT
                DATE(timestamp) AS date,
                COUNT(*) AS all_calls_count
            FROM `<YOUR-PROJECT-NUMBER>.demos.nyc_weather`
            GROUP BY date
        ) y
        ON x.date=y.date
    ) b
    ON
        a.date = b.date
    GROUP BY
        descriptor,
        temperature
)
GROUP BY descriptor
HAVING
    total_complaint_count > 5000 AND
    ABS(corr_pct) > 0.5 AND
    data_count > 5
ORDER BY
ABS(corr_pct) DESC
