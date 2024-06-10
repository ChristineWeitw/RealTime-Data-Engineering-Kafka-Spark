from yahoo_fin import stock_info as si
from datetime import datetime
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid
import json
import time


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 5, 31, 10, 00)
}

def get_stock_price(symbol):
    try:
        price = si.get_live_price(symbol)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return price, timestamp
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None

def send_to_kafka(producer, topic, data):
    try:
        producer.send(topic, value=data)
        producer.flush()
        print(f"Sent data to Kafka: {data}")
    except Exception as e:
        print(f"Failed to send data to Kafka: {e}")

def stream_data():

    producer = KafkaProducer(bootstrap_servers=['broker:29092', 'localhost:9092'],
                             api_version=(2, 7, 0),
                             value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'), 
                             max_block_ms=5000)
    topic = 'stock_prices'
    symbols = ["0050.TW", "VOO", "NVDA"]

    while True:
        for symbol in symbols:
            price, timestamp = get_stock_price(symbol)
            if price is not None:
                data = {
                    'id' : str(uuid.uuid4()),
                    'symbol': symbol,
                    'price': price,
                    'timestamp': timestamp
                }
                send_to_kafka(producer, topic, data)
        # Wait for 3 hours
        time.sleep(10)

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

stream_data();