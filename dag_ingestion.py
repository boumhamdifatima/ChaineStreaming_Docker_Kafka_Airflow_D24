import csv
import os
import json
import time
import random
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


args = {
    'owner': 'Rachid_Kafka',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='aadag_ingestion', default_args=args, schedule_interval=None)

# Producer Data
def producer_data():
    try:
        from kafka import KafkaProducer  
    except ModuleNotFoundError:
        print("❌ Kafka  module not found! Make sure it's installed in the Airflow container.")
        return 
    try:
        from faker import Faker
    except ModuleNotFoundError:
        print("❌ Kafka  module not found! Make sure it's installed in the Airflow container.")
        return       

    # Initialize Faker
    fake = Faker()

    # Set up Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert Python dict to JSON
    )

    # Topic name
    topic = 'real_time_messages'

    print(f"🔥 Sending fake messages to Kafka topic: {topic}...")

    # Generate real-time messages
    for _ in range(15):
        message = {
            "patient_id": fake.uuid4(),
            "name": fake.name(),
            "age": random.randint(20, 90),
            "blood_pressure": f"{random.randint(90, 140)}/{random.randint(60, 100)}",
            "heart_rate": random.randint(50, 120),
            "temperature": round(random.uniform(36.0, 40.0), 1),
            "timestamp": time.time()
        }

        # Send message to Kafka
        producer.send(topic, message)
        print(f"✅ Sent: {message}")

        time.sleep(2)  

# Consumer Data
def consumer_data():
    from kafka import KafkaConsumer

    # Set up Kafka consumer
    consumer = KafkaConsumer(
        'real_time_messages',
        bootstrap_servers='kafka:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))  
    )

    print("📥 Listening for messages...")
    message_count = 0

    # Receive messages in real-time
    for message in consumer:
        print(f"📝 Received: {message.value}")
        message_count += 1

        if message_count >= 15:
            print("⏹️ Stopping consumer after 5 messages.")
            break  

with dag:
    task_1 = PythonOperator(
        task_id='producer_data',
        python_callable=producer_data
    )
    task_2 = PythonOperator(
        task_id='consumer_data',
        python_callable=consumer_data
    )
    
    # task_1.set_downstream(task_2)
    # task_1  >> task_2 
    
    # Run both tasks in parallel
    [task_1, task_2]

