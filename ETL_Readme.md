It is expected that the output will be DAG in airflow, which will be counted every day as yesterday.

1. We will process two tables in parallel. In feed_actions for each user, we count the number of views and likes of the content. In message_actions for each user, we count how many messages he receives and sends, how many people he writes to, how many people write to him. Each upload must be in a separate task.

2. Next, we combine two tables into one.

3. For this table, we consider all these metrics in the context of gender, age and wasps. We do three different tasks for each cut.

4. And we write the final data with all the metrics into a separate table in ClickHouse.

5. Every day the table should be supplemented with new data.

The structure of the final table should be like this:

Date - event_date

Slice name - dimension

slice value - dimension_value

Number of views - views

Number of likes - likes

Number of messages received - messages_received

Number of messages sent - messages_sent

How many users received messages from - users_received

How many users sent a message - users_sent

The slice is os, gender and age
