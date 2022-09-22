insert my_data.stackoverflow_stream
(
    activity_id,
    ts,
    activity,
    customer,
    feature_1,
    feature_2,
    feature_3,
    link
)
select
    cast(id as string)          as activity_id,
    creation_date               as ts,
    'user_created'              as activity,
    cast(id as string)          as customer,
    display_name                as feature_1,
    cast(reputation as string)  as feature_2,
    about_me                    as feature_3,
    website_url                 as link
from
    bigquery-public-data.stackoverflow.users
where
    true
    /* ensure customer + activity + ts is unique in the whole table */
    and not exists (select  *
                    from    my_data.stackoverflow_stream
                    where   customer = cast(id as string)
                            and activity = 'user_created'
                            and ts = creation_date
                    );