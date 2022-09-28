import pandas as pd

from kafka import KafkaProducer
import time
from json import dumps

KAFKA_TOPIC_NAME = "orders_topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    file_path = "data_sources/orders.csv"

    orders_pd_df = pd.read_csv(file_path)

    print(orders_pd_df.head)

    orders_list = orders_pd_df.to_dict(orient="records")

    # print(orders_list.head[0])

    for order in orders_list:
        message = order
        print("Message to be Sent : ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME, message)
        time.sleep(1)

