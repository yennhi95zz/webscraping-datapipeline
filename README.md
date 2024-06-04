
# How to Easily Automate Web Scraping with a Simple Data Pipeline

![Author](https://img.shields.io/badge/Author-Nhi%20Yen-brightgreen)
[![Medium](https://img.shields.io/badge/Medium-Follow%20Me-blue)](https://medium.com/@yennhi95zz/subscribe)
[![GitHub](https://img.shields.io/badge/GitHub-Follow%20Me-lightgrey)](https://github.com/yennhi95zz)
[![Kaggle](https://img.shields.io/badge/Kaggle-Follow%20Me-orange)](https://www.kaggle.com/nhiyen/code)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect%20with%20Me-informational)](https://www.linkedin.com/in/yennhi95zz/)

This notebook is associated with the articles/ project below:

- Find the full code on this [GitHub repository](https://github.com/yennhi95zz/webscraping-datapipeline).
- Explore a detailed explanation in my [Medium article](https://medium.com/@yennhi95zz/how-to-easily-automate-web-scraping-with-a-simple-data-pipeline-917d0a692472).

👉Get UNLIMITED access to every story on Medium with just $1/week ▶ [HERE](https://medium.com/@yennhi95zz/membership)

👉Troubling with Machine Learning, Data Engineering or Web Scraping? Hire me in [Upwork](https://medium.com/r/?url=https%3A%2F%2Fwww.upwork.com%2Ffreelancers%2F~018cb35a4fd005fbff).

# Table of Contents

![alt text](<Web Scraping Data Pipeline.png>)

## 1. Prerequisites

- Docker and VS Code installation required.
- Obtain Comet API and OpenAI keys.
- Install necessary Python libraries from the `requirements.txt` file.

## 2. Docker Services Overview

1. **Zookeeper**: Manages and coordinates Kafka brokers.
2. **Kafka Broker**: Handles message streams between producers and consumers.
3. **Control Center**: Provides a UI for managing and monitoring the Kafka cluster.
4. **Debezium**: Captures changes from the PostgreSQL database and streams them to Kafka topics.
5. **Debezium UI**: Provides a UI to manage Debezium connectors.
6. **PostgreSQL**: Source database where web scraping data is stored.

To run Docker Compose, use the following command in the VS Code Terminal:

```
docker-compose up -d
```

## 3. Raw Data Extraction

The detailed steps of using LLM were explained in a previous article [Web Scraping With 5 Different Methods: All You Need to Know](https://medium.com/r/?url=https%3A%2F%2Fheartbeat.comet.ml%2Fweb-scraping-with-5-different-methods-all-you-need-to-know-403a59fceea0). We will focus on the main code for discussion purposes. The complete code can be found in this *GitHub repository > extract.py*.

## 4. Store the data in PostgresSQL

In this step, there are three main functions:

- `create_table(conn)`: Creates the "crypto" table if it does not exist.
- `insert_crypto(conn, crypto)`: Inserts a new row into the "crypto" table.
- `get_latest_timestamp(conn)`: Gets the latest timestamp of the 'crypto' table and checks if the row's timestamp is newer than the last update. Only new data is inserted into the database.

These functions are used in the main function to send data to PostgreSQL.

## 5. Debezium Connector Configuration

Debezium specializes in Change Data Capture (CDC), which detects and captures changes made to a database. Changes are propagated to downstream systems as events.

## 6. Monitor and manage the data in Control Center

You can view the results in the Control Center.

# Contributing

If you have improvements or new methods to add, follow these steps:

1. **Fork the Repository**: Click "Fork" to create a copy in your GitHub account.
2. **Clone the Forked Repository**: Use `git clone` to get a local copy.
3. **Create a New Branch**: Make a new branch for your changes.
4. **Make Changes**: Add or modify code, documentation, etc.
5. **Commit Changes**: Commit with a clear message.
6. **Push Changes**: Push to your forked repo.
7. **Create a Pull Request (PR)**: Open a PR from your branch to the main repo.

Feel free to contribute and make this project even better!

# License

This project is licensed under the [MIT License](https://github.com/git/git-scm.com/blob/main/MIT-LICENSE.txt), making it open for collaboration and use in various projects.