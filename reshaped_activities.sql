with post_activity as (
    select
        post_id,
        user_id,
        post_history_type_id,
        --since the time stamps for these activities are identical, we add a second to differentiate them
        case
            when post_history_type_id in (1,4,7) then timestamp_add(creation_date, interval 1 second)
            when post_history_type_id in (3,6,9) then timestamp_add(creation_date, interval 2 second)
            else creation_date
        end as activity_date,
        case
            when post_history_type_id = 2 then 'started_session'
            when post_history_type_id = 5 then 'viewed_product_page'
            when post_history_type_id = 1 then 'started_checkout'
            when post_history_type_id = 3 then 'opened_email'
            when post_history_type_id = 6 then 'completed_order'
            when post_history_type_id = 4 then 'clicked_email'
            when post_history_type_id in (13,12,33,8,34) then 'unsubscribed_email'
            when post_history_type_id in (10,19,9,14,38,37,20,15,53,35,16,11,7) then 'canceled_order'
        end as activity_type
    from
        bigquery-public-data.stackoverflow.post_history
    where
        user_id > 0 --exclude automated processes
        and user_id is not null
        and creation_date >= '2021-06-01' 
        and creation_date <= '2021-09-30'
)
select activity_type, count(*)
from post_activity
group by 1
order by 2 desc