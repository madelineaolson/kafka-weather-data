# Kafka Streaming for Weather Data

## Objective:
The goal of this project is to implement a Kafka-based system for processing daily weather data. The key objectives include writing producer and consumer Python programs, applying streaming techniques to achieve "exactly once" semantics, and using both manual and automatic assignment of Kafka topics and partitions. After streaming data, collect in a JSON file for use in a web dashboard where a real-time graph can be generated. 

## Implementation:
**Cluster Setup:** Docker containers are used to set up a Kafka broker along with three programs - producer.py, debug.py, and consumer.py. The producer generates daily weather data and publishes it to a Kafka stream, while the consumers process this data for statistical analysis.

**Kafka Producer (producer.py):** The producer initializes a "temperatures" topic with 4 partitions and 1 replica. It uses a gRPC protobuf to encode the weather data and publishes it to the Kafka stream with specific settings for retries and acknowledgment.

**Kafka Stats Consumer (consumer.py):** The stats consumer uses manual partition assignment, processes messages, and computes statistics on the "temperatures" topic. It updates JSON files for each partition with information on count, sum, average, start date, and end date for each month and year combination.

**Plotting Stats (plot.py):** The plotting program generates a visual representation of average max-temperatures for specific months based on the recorded data. It produces an SVG file displaying the average max-temperature for each month.

## Outcome:
The implemented Kafka-based system successfully handles the generation, processing, and analysis of daily weather data. The producers and consumers operate continuously, ensuring data consistency with exactly-once semantics. The generated JSON files provide detailed statistics for each partition, while the plotting program offers a visual summary of average max-temperatures and could be utilized for a dashboard. The project enhances understanding of Kafka producers and consumers, stream processing, and data consistency in a distributed system.
