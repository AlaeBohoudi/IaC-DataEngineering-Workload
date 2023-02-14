

/*SELECT trip_data.year, trip_data.month, taxi_zone.borough, 
                CONVERT(DATE, trip_data.lpep_pickup_datetime) AS trip_date, 
                calendar.day_name AS trip_day,
                CASE WHEN calendar.day_name IN('Saturday', 'Sunday') THEN 'Y' ELSE 'N' END AS trip_day_weekend_ind,
                SUM(CASE WHEN payment_type.description = 'Credit card' THEN 1 ELSE 0 END) AS card_trip_amount,
                SUM(CASE WHEN payment_type.description = 'Cash' THEN 1 ELSE 0 END) AS cash_trip_amount
    FROM silver.trip_data_green_vw AS trip_data
    JOIN silver.taxi_zone AS taxi_zone ON (trip_data.pu_location_id = taxi_zone.location_id)
    JOIN silver.calendar AS calendar ON (calendar.date = CONVERT(DATE, trip_data.lpep_pickup_datetime))
    JOIN silver.payment_type AS payment_type ON (payment_type.payment_type = trip_data.payment_type)
    WHERE trip_data.year ='2020' AND trip_data.month = '01'

    GROUP BY trip_data.year, trip_data.month, taxi_zone.borough, CONVERT(DATE, trip_data.lpep_pickup_datetime),calendar.day_name
*/


EXEC gold.trip_data_gold_usp '2020', '01'
EXEC gold.trip_data_gold_usp '2020', '02'
EXEC gold.trip_data_gold_usp '2020', '03'
EXEC gold.trip_data_gold_usp '2020', '04'
EXEC gold.trip_data_gold_usp '2020', '05'
EXEC gold.trip_data_gold_usp '2020', '06'
EXEC gold.trip_data_gold_usp '2020', '07'
EXEC gold.trip_data_gold_usp '2020', '08'
EXEC gold.trip_data_gold_usp '2020', '09'
EXEC gold.trip_data_gold_usp '2020', '10'
EXEC gold.trip_data_gold_usp '2020', '11'
EXEC gold.trip_data_gold_usp '2020', '12'
EXEC gold.trip_data_gold_usp '2021', '01'
EXEC gold.trip_data_gold_usp '2021', '02'
EXEC gold.trip_data_gold_usp '2021', '03'
EXEC gold.trip_data_gold_usp '2021', '04'
EXEC gold.trip_data_gold_usp '2021', '05'
EXEC gold.trip_data_gold_usp '2021', '06'



SELECT trip_data.year, trip_data.month, taxi_zone.borough, 
                CONVERT(DATE, trip_data.lpep_pickup_datetime) AS trip_date, 
                calendar.day_name AS trip_day,
                CASE WHEN calendar.day_name IN('Saturday', 'Sunday') THEN 'Y' ELSE 'N' END AS trip_day_weekend_ind,
                SUM(CASE WHEN payment_type.description = 'Credit card' THEN 1 ELSE 0 END) AS card_trip_amount,
                SUM(CASE WHEN payment_type.description = 'Cash' THEN 1 ELSE 0 END) AS cash_trip_amount,
                SUM(CASE WHEN trip_type.trip_type_desc = 'Dispatch' THEN 1 ELSE 0 END) AS dispatch_trip_count,
                SUM(CASE WHEN trip_type.trip_type_desc = 'Street-hail' THEN 1 ELSE 0 END) AS streethail_trip_count,
                SUM(trip_data.trip_distance) AS total_trip_distance,
                SUM(DATEDIFF(MINUTE, trip_data.lpep_pickup_datetime, trip_data.lpep_dropoff_datetime)) AS trip_duration,
                SUM(trip_data.fare_amount) AS total_fare_amount

    FROM silver.trip_data_green_vw AS trip_data
    JOIN silver.taxi_zone AS taxi_zone ON (trip_data.pu_location_id = taxi_zone.location_id)
    JOIN silver.calendar AS calendar ON (calendar.date = CONVERT(DATE, trip_data.lpep_pickup_datetime))
    JOIN silver.payment_type AS payment_type ON (payment_type.payment_type = trip_data.payment_type)
    JOIN silver.trip_type AS trip_type ON (trip_type.trip_type = trip_data.trip_type)
    WHERE trip_data.year ='2020' AND trip_data.month = '01'

    GROUP BY trip_data.year, trip_data.month, taxi_zone.borough, CONVERT(DATE, trip_data.lpep_pickup_datetime),calendar.day_name