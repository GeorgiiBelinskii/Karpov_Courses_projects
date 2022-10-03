This repository is for projects I have done under Karpov Courses bootcamp.

In this program, I improved my Python programming skills and  A\b tests. I worked with Clickhouse, Superset, and Airflow. Also, I have experience with Redash and intermediate statistical methods for analyzing.

During this program, I created a system of dashboards for monitoring the product's main characteristics, developed a system of notifications of anomalies in data, and automatized daily reports through Telegram.

| Project title | Description | Used Libraries|
| :-------------------- | :--------------------- |:---------------------------|
| [Yesterday report telegram bot ](https://github.com/GeorgiiBelinskii/bootcamp/blob/main/yesterday_report_bot_tg.py) | There is a telegram bot, which extracts data from Clickhouse, calculates main metrics, and visualizes dynamics in a telegram message. | *pandas, pandahouse, matplotlib, seaborn, io, airflow.decorators, telegram*  |
| [Detector of data anomalies](https://github.com/GeorgiiBelinskii/bootcamp/blob/main/detector_of_anomalies.py) | There is a system, which checks the main metrics every 15 minutes. In an alert case, the bot sends a message to the chat with additional information and visualizations | *pandas, pandahouse, matplotlib, seaborn, io, airflow.decorators, telegram*  |
