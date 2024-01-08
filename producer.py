from kafka import KafkaAdminClient
from kafka import KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import weather
import time
import grpc
import report_pb2
import calendar


broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except Exception as e:
    print(f"Error deleting topics: {e}")
    print("Continuing")

print(admin_client.list_topics())

time.sleep(3)

try:
    admin_client.create_topics([NewTopic("temperatures", num_partitions=4, replication_factor=1)])
except Exception as e:
    print(f"error: {e}")
    print(admin_client.list_topics())
    
print("Topics:", admin_client.list_topics())

producer = KafkaProducer(
    bootstrap_servers=[broker],
    retries=10,
    acks="all"
)

for date, degrees in weather.get_next_weather(delay_sec=0.1):
    report_message = report_pb2.Report()
    report_message.date = date
    report_message.degrees = degrees

    
    month = date.split('-')[1]
    month_key = calendar.month_name[int(month)].encode('utf-8')
    protobuf_bytes = report_message.SerializeToString()
    
    producer.send("temperatures", key=month_key, value=protobuf_bytes)

producer.close()
