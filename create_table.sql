create or replace table my_data.stackoverflow_stream (
    activity_id string,
    ts timestamp,
    activity string,
    customer string,
    anonymous_customer_id string,
    feature_1 string,
    feature_2 string,
    feature_3 string,
    revenue_impact float64,
    link string,
    activity_occurrence int64,
    activity_repeated_at timestamp
);

update my_data.stackoverflow_stream a
set activity_occurrence = dt.activity_occurrence,
    activity_repeated_at = dt.activity_repeated_at
from (
    select
        activity_id,
        customer,
        activity,
        ts,
        row_number() over(partition by customer, activity order by ts asc) as activity_occurrence,
        lead(ts,1)   over(partition by customer, activity order by ts asc) as activity_repeated_at
    from 
        my_data.stackoverflow_stream 
)dt
where 
    dt.activity_id = a.activity_id
    and dt.customer = a.customer
    and dt.activity = a.activity
    and a.ts = dt.ts;