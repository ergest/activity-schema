--first ever
with cohort as (
    select
        activity_id,
        datetime(ts, 'America/New_York') as activity_timestamp,
        customer,
        activity,
        activity_occurrence,
        feature_1,
        feature_2,
        feature_3,
        ts                   as join_ts,
        activity_id          as join_cohort_id,
        customer             as join_customer,
        activity_repeated_at as join_cohort_next_ts
    from
        my_data.stackoverflow_stream  
    where
        activity = 'answer_created'
    order by
        ts desc
),
first_ever_question as (
    select
        c.join_customer,
        c.join_cohort_id,
        min(s.ts) as first_question_timestamp
    from
        cohort c
        inner join my_data.stackoverflow_stream s
            on c.join_customer = s.customer
    where 
        s.activity = 'question_created'
    group by
        1,2
),
first_ever_question_features as (
    select
        fe.join_customer,
        fe.join_cohort_id,
        fe.first_question_timestamp,
        s.activity,
        s.feature_1,
        s.feature_2,
        s.feature_3,
        s.link
    from
        my_data.stackoverflow_stream s
        inner join first_ever_question fe
            on s.customer = fe.join_customer
            and s.ts = fe.first_question_timestamp
    where 
        s.activity = 'question_created'
)
select
    c.activity_timestamp,
    c.customer,
    c.activity,
    c.activity_occurrence,
    c.feature_1,
    c.feature_2,
    c.feature_3,
    datetime(fe.first_question_timestamp, 'America/New_York') as first_question_timestamp,
    fe.activity,
    fe.feature_1,
    fe.feature_2,
    fe.feature_3,
    fe.link
from
    cohort c
    left outer join first_ever_question_features fe
        on c.join_customer = fe.join_customer
        and c.join_cohort_id = fe.join_cohort_id
where
    fe.activity is not null;