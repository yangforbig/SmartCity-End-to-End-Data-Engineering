# SmartCity End-to-End Data Streaming Project

## Table of Contents
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [System Architecture](#system-architecture)
  - [Technologies](#technologies)

## Introduction

In this project, we'll demonstrate how to construct a real-time data pipeline for a smart city. We'll use technologies such as IoT, Kafka, Spark, and AWS technologies including AWS Glue, AWS Athena and AWS Redshift to ingest, process, and store data.

## System Architecture

![System Architecture](https://github.com/yangforbig/SmartCity-End-to-End-Data-Engineering/blob/main/System%20Architecture.png)

The project is designed with the following components:

- **IoT**: We use main.py to simulate data streaming from IoT based on the vehicle travel from London to Birmingham, including vvehicle information, GPS, Camera information, Weather Information and Emergency information. 
- **Apache Kafka and Zookeeper**: Used for streaming data from IoT to the processing engine.
- **Apache Spark**: For data processing with its master and worker nodes.
- **AWS**: Used for consume and store the data streaming into a cloud-based database for semantic layer processing. 

## Technologies

- Python
- Apache Kafka
- Apache Spark
- AWS Glue
- AWS Athena
- Redshift
- Docker
