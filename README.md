This repository is for projects I have done under Karpov Courses bootcamp.

In this program, I improved my Python programming skills and  A\b tests. I worked with Clickhouse, Superset, and Airflow. Also, I have experience with Redash and intermediate statistical methods for analyzing.

During this program, I created a system of dashboards for monitoring the product's main characteristics, developed a system of notifications of anomalies in data, and automatized daily reports through Telegram.

| Project title | Description | Used Libraries| Example |
| :-------------------- | :--------------------- |:---------------------------| :---------------------------|
| [Yesterday report telegram bot ](https://github.com/GeorgiiBelinskii/bootcamp/blob/main/yesterday_report_bot_tg.py) | There is a telegram bot, which extracts data from Clickhouse, calculates main metrics, and visualizes dynamics in a telegram message. | *pandas, pandahouse, matplotlib, seaborn, io, airflow.decorators, telegram*  | [Example of report](https://user-images.githubusercontent.com/102724511/193519173-79a52d7e-19e2-4445-abe8-258fc015baf4.png)|
| [Detector of data anomalies](https://github.com/GeorgiiBelinskii/bootcamp/blob/main/detector_of_anomalies.py) | There is a system, which checks the main metrics every 15 minutes. In an alert case, the bot sends a message to the chat with additional information and visualizations | *pandas, pandahouse, matplotlib, seaborn, io, airflow.decorators, telegram*  |[Example of report](https://user-images.githubusercontent.com/102724511/193519709-39413875-ef60-4fa0-9350-bd3a2ea04081.png)
| [A/B test](https://github.com/GeorgiiBelinskii/bootcamp/blob/main/AB-test.ipynb) | We need to check the correctness of the splitting system. We need to analyze the results of our test with different statistical tests, including advanced approaches (bucket testing, linearization) | *pandas, pandahouse, matplotlib, seaborn, scipy* |
