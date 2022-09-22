# Activity Schema Implementation with SQL

## Introduction
This repository contains an ActivitySchema implementation utilizeing the StackOverflow database available for free on BigQuery

## Resources
- For information on the ActivitySchema read my blog post [here](https://www.ergestx.com/activity-schema/)
- To access the data on BigQuery go [here](https://cloud.google.com/bigquery/public-data/stackoverflow)

## How to use this repo
The SQL code here is designed to build a slice of the entire StackOverflow (SO) database as an activity schema. It utilizes 5 activities from SO (`answer_edited`, `answer_created`, `question_edited`, `question_created`, `commented_on_post`, `user_created`) to build this.

### Step 1: Create the table
Run the first portion of SQL code under [create_table.sql](create_table.sql) to create the table. Don't run the update script yet. That is supposed to be run after the table has been built:

```sql
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
```

### Step 2: Insert the activities
Run the scripts in [post_activities.sql](post_activities.sql), [comments.sql](comments.sql) and [user_created.sql](user_created.sql) to populate the table

### Step 3: Update the utility columns
ActivitySchema uses `activity_repeated_at` as a helper column to make queries faster. This column needs to be updated every time you insert data into the table. Now my script updates the entire table, but it is possible to just update the activity you inserted. Run the second portion of SQL code under [create_table.sql](create_table.sql) to update the table:

```sql
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
```