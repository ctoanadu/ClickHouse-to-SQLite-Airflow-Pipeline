with t1 as(
select date_trunc('month', pickup_date) as month, AVG(trip_count) as sat_mean_trip_count
    from
    
    (SELECT 
        pickup_date,
        COUNT(*) AS trip_count
    FROM 
        tripdata
     WHERE DAYOFWEEK(pickup_date) = 6 AND pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31'
    GROUP BY 
         pickup_date)
    group by 1),

t2 as(
select date_trunc('month', pickup_date) as month, AVG(fare_amount) as sat_mean_fare_per_trip
 from
    
   (SELECT 
        pickup_date, fare_amount
    FROM 
        tripdata
     WHERE DAYOFWEEK(pickup_date) = 6 AND pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31')
     group by 1),

t3 as (
SELECT 
    date_trunc('month', pickup_date) AS month,
    AVG(dateDiff('minute', pickup_datetime, dropoff_datetime)) AS sat_mean_duration_per_trip_minutes
FROM 
    tripdata
WHERE 
    DAYOFWEEK(pickup_date) = 6
    AND pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31'
GROUP BY 
    month),
    

t4 as (select date_trunc('month', pickup_date) as month, AVG(trip_count) as sun_mean_trip_count
    from   
    (SELECT 
        pickup_date,
        COUNT(*) AS trip_count
    FROM 
        tripdata
     WHERE DAYOFWEEK(pickup_date) = 7 AND pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31'
    GROUP BY 
         pickup_date)
    group by 1),


t5 as (select date_trunc('month', pickup_date) as month, AVG(fare_amount) as sun_mean_fare_per_trip
 from
    
   (SELECT 
        pickup_date, fare_amount
    FROM 
        tripdata
     WHERE DAYOFWEEK(pickup_date) = 7 AND pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31')
     group by 1),

    
t6 as (SELECT 
    date_trunc('month', pickup_date) AS month,
    AVG(dateDiff('minute', pickup_datetime, dropoff_datetime)) AS sun_mean_duration_per_trip_minutes
FROM 
    tripdata
WHERE 
    DAYOFWEEK(pickup_date) = 7
    AND pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31'
GROUP BY 
    month)


Select  t1.month as month,
        t1.sat_mean_trip_count as sat_mean_trip_count, 
        t2.sat_mean_fare_per_trip as sat_mean_fare_per_trip, 
        t3.sat_mean_duration_per_trip_minutes as sat_mean_duration_per_trip_minutes, 
        t4.sun_mean_trip_count as sun_mean_trip_count, 
        t5.sun_mean_fare_per_trip as sun_mean_fare_per_trip ,
        t6.sun_mean_duration_per_trip_minutes as sun_mean_duration_per_trip_minutes
from t1
join t2
on t1.month=t2.month
join t3
on t2.month=t3.month
join t4
on t3.month=t4.month
join t5
on t4.month=t5.month
join t6
on t5.month=t6.month