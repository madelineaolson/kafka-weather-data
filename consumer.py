import os
import sys
import json
from kafka import KafkaConsumer
from kafka import TopicPartition
import report_pb2
import calendar

broker = "localhost:9092"

def update_stats(stats, date, degrees):
    month, year = date.split('-')[1], date.split('-')[0]
    month = calendar.month_name[int(month)]
    if month not in stats:
        stats[month] = {}
    if year not in stats[month]:
        stats[month][year] = {
            "count": 0,
            "sum": 0,
            "avg": 0,
            "end": None
        }
    if stats[month][year]["end"] == None or date > stats[month][year]["end"]:
        stats[month][year]["count"] += 1
        stats[month][year]["sum"] += degrees
        stats[month][year]["end"] = date
        stats[month][year]["avg"] = stats[month][year]["sum"] / stats[month][year]["count"]                                                       

def load_partition_data(partition):
    try:
        with open(f'partition-{partition}.json', 'r') as file:
            return json.load(file)

    except FileNotFoundError:
        return {"partition": partition, "offset": 0}

def save_partition_data(partition_data):                                         
    file_name = f'partition-{partition_data["partition"]}.json'
    file_path = os.path.join(os.path.dirname(__file__), file_name)

    temp_file_path = file_path + ".tmp"
    
    with open(temp_file_path, 'w') as file:
        json.dump(partition_data, file)

    os.rename(temp_file_path, file_path)

    print(f"Saving json file to: {os.path.abspath(file_path)}")

    
    

def temp_consumer(partitions):
    consumer = KafkaConsumer(bootstrap_servers=[broker])

    partitions = list(map(int, partitions))

    assigned_partitions = [TopicPartition("temperatures", p) for p in partitions]

    stats = {p: load_partition_data(p) for p in partitions}

    print(stats)

    consumer.assign(assigned_partitions)

   # consumer.seek(TopicPartition("temperatures", p), stats["offset"])                                                                                                                                                                                                                                                                                                                                                                                                                      


    for p in partitions:
       # json_file = load_partition_data(p)                                                                                                                                                                                                                                                                                                                                                                                                                                                 

        consumer.seek(TopicPartition("temperatures", p), stats[p]["offset"])

    try:

        while True:
            batch = consumer.poll(1000)
            for topic_partition, messages in batch.items():
               # consumer.seek(topic_partition, stats[topic_partition.partition]["offset"])                                                                                                                                                                                                                                                                                                                                                                                                 
                tp_p = topic_partition.partition


                for msg in messages:
                    report_message = report_pb2.Report()
                    report_message.ParseFromString(msg.value)
                    key = msg.key.decode("utf-8") if msg.key else None
                    date = report_message.date
                    degrees = report_message.degrees

                    update_stats(stats[tp_p], date, degrees)

                    # Update the offset in the stats dictionary                                                                                                                                                                                                                                                                                                                                                                                                                             
                    stats[tp_p]["offset"] = consumer.position(topic_partition)
                    save_partition_data(stats[tp_p])

  #              print(stats[tp_p])                                                                                                                                                                                                                                                                                                                                                                                                                                                         
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    partitions = sys.argv[1:]
    temp_consumer(partitions)
