# Bulding ETL pipeline

Creating an Airflow dag, extracting the data, transforming it and loading to the database

<a href="https://github.com/egorkapot/Karpov-Analyst-Simulator/blob/main/e-makarov-dag.py"> Working File </a>


<h1> Description of the project </h1

It is expected that the output will be DAG in airflow, which will be counted every day as yesterday.

1. We will process two tables in parallel. In feed_actions for each user, we count the number of views and likes of the content. In message_actions for each user, we count how many messages he receives and sends, how many people he writes to, how many people write to him. Each upload must be in a separate task.

2. Next, we combine two tables into one.

3. For this table, we consider all these metrics in the context of gender, age and wasps. We do three different tasks for each cut.

4. And we write the final data with all the metrics into a separate table in ClickHouse.

5. Every day the table should be supplemented with new data.

The structure of the final table should be like this:

1. Date - event_date

2. Slice name - dimension

3. Slice value - dimension_value

4. Number of views - views

5. Number of likes - likes

6. Number of messages received - messages_received

7. Number of messages sent - messages_sent

8. How many users received messages from - users_received

9. How many users sent a message - users_sent

The slice is os, gender and age
