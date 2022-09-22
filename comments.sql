insert my_data.stackoverflow_stream
(
    activity_id,
    ts,
    activity,
    customer,
    feature_1,
    feature_2,
    link
)
with post_types as (
    select
        id as post_id,
        creation_date,
        'question' as post_type,
        title,
        body,
        tags,
        'https://stackoverflow.com/q/' || id as post_url
    from
        bigquery-public-data.stackoverflow.posts_questions
    union all
    select
        id as post_id,
        creation_date,
        'answer' as post_type,
        title,
        body,
        tags,
        'https://stackoverflow.com/a/' || id as post_url
    from
        bigquery-public-data.stackoverflow.posts_answers
 )
select
    cast(c.id as string)       as activity_id,
    c.creation_date            as ts,
    'commented_on_post'        as activity,
    cast(c.user_id as string)  as customer,
    c.text                     as feature_1,
    cast(c.score as string)    as feature_2,
    post_url                   as link
from
    bigquery-public-data.stackoverflow.comments c
    join post_types pt
        on c.post_id = pt.post_id
where
    pt.creation_date between '2021-06-01' and '2021-09-30'
    /* ensure customer + activity + ts is unique in the whole table */
    and not exists (select  *
                    from    my_data.stackoverflow_stream
                    where   customer = cast(c.user_id as string)
                            and activity = 'commented_on_post'
                            and ts = c.creation_date
                    );