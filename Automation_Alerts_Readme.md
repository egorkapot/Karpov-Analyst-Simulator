# Automation_Alerts 

The system should check key metrics every 15 minutes, such as active users in the feed / messenger, views, likes, CTR, the number of messages sent.

<a href="https://github.com/egorkapot/Karpov-Analyst-Simulator/blob/main/emakarov_alert_final.py"> Working File </a>

<h1> Description of the project </h1>

Study the behavior of the metrics and select the most appropriate method for anomaly detection. In practice, as a rule, statistical methods are used.
In the simplest case, you can, for example, check the deviation of the metric value in the current 15-minute from the value in the same 15-minute a day ago.

If an abnormal value is detected, an alert should be sent to the chat - a message with the following information: metric, its value, deviation value.
Additional information can be added to the message that will help in investigating the causes of the anomaly, for example, it can be a graph, links to a dashboard/chart in the BI system.



