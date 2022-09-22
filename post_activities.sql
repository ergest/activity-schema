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
with post_activity as (
    select
        id,
        user_id,
        post_id,
        creation_date,
        case
            when post_history_type_id in (1,2,3) then 'created'
            when post_history_type_id in (4,5,6) then 'edited'
        end as activity_type
    from
        bigquery-public-data.stackoverflow.post_history
    where
        true 
        and user_id > 0 --exclude automated processes
        and user_id is not null
        and post_history_type_id between 1 and 6
)
,post_types as (
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
    cast(pa.id as string)       as activity_id,
    pa.creation_date            as ts,
    pt.post_type || '_' || pa.activity_type as activity,
    cast(pa.user_id as string)  as customer,
    pt.title                    as feature_1,
    pt.body                     as feature_2,
    pt.tags                     as feature_3,
    post_url                    as link
from
    post_types pt
    join post_activity pa 
        on pt.post_id = pa.post_id
where
    pt.creation_date between '2021-06-01' and '2021-09-30'
    /* ensure customer + activity + ts is unique in the whole table */
    and not exists (select  *
                    from    my_data.stackoverflow_stream
                    where   customer = cast(pa.user_id as string)
                            and activity in ('question_created', 'question_edited', 'answer_created', 'answer_edited')
                            and ts = pa.creation_date
                    );