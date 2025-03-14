import json
import time
import random
from datetime import datetime
import redis
from kafka import KafkaProducer
from kafka import KafkaConsumer
from faker import Faker
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# Définition des arguments du DAG
args = {
    'owner': 'Team_Rachid_Fatima',
    'start_date': days_ago(1),
    'catchup': False
}

dag = DAG(
            dag_id='a_kafka_redis_airflow', 
            default_args=args, 
            schedule_interval="*/5 * * * *",  # Runs every 5 minute
            catchup=False,  # Prevents backfilling old runs
        )

# Tache 1: Production et envoi des messages vers kafka
def producer_data():

    # Initialiser Faker
    fake = Faker()

    # Set up Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convertir dict Python en JSON
    )

    # Nom Topic
    topic = 'real_time_medical_messages'

    print(f"🔥 Sending fake messages to Kafka topic: {topic}...")

    # Generate real-time messages
    for _ in range(20):
        message = {
            "patient_id": fake.uuid4(),
            "name": fake.name(),
            "age": random.randint(20, 90),
            "blood_pressure": f"{random.randint(70, 140)}/{random.randint(40, 100)}",
            "heart_rate": random.randint(50, 120),
            "temperature": round(random.uniform(36.0, 40.0), 1),
            "timestamp": time.time()
        }

        # Send message to Kafka
        producer.send(topic, message)
        print(f"✅ Sent: {message}")

        time.sleep(0.5)  

# Tache 2: Consommation des messages a partir de kafka
def consumer_data(**kwargs):
    
    # Récupérer la tâche instance pour XCom
    ti = kwargs['ti']
    
    # Set up Kafka consumer
    consumer = KafkaConsumer(
        'real_time_medical_messages',
        bootstrap_servers='kafka:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        #consumer_timeout_ms=10000  # Arrête après 10s sans nouveaux messages        
    )

    print("📥 Listening for messages...")
    messages = []

    # Receive messages in real-time
    for message in consumer:
        messages.append(message.value)
        print(f"📝 Received: {message.value}")

        if len(messages) >= 15:
            print("⏹️ Stopping consumer after 15 messages.")
            break  
    
    # Stocker les messages pour les étapes suivantes
    ti.xcom_push(key='messages', value=messages)
    
    # Fermer proprement le consumer
    consumer.close()
    
    
# Tache 3: Extraction du prénom et nom a partir de "name" 
def transform_extract_name(**kwargs):
    ti = kwargs['ti']
    messages = ti.xcom_pull(key='messages', task_ids='consommer_data')
    
    if not messages:
        print("⚠️ No messages received for name extraction!")
        return

    transformed_messages = []
    for msg in messages:
        first_name, last_name = msg["name"].split(" ", 1)
        msg["first_name"] = first_name
        msg["last_name"] = last_name if len(msg["name"]) > 1 else "Inconnu"
        del msg["name"]
        # Réorganiser les clés pour mettre first_name et last_name au debut
        reordered_msg = {
            "patient_id": msg["patient_id"],
            "first_name": msg["first_name"],
            "last_name": msg["last_name"],
            "age": msg["age"],
            "blood_pressure": msg["blood_pressure"],
            "heart_rate": msg["heart_rate"],
            "temperature": msg["temperature"],
            "timestamp": msg["timestamp"]
        }
        
        transformed_messages.append(reordered_msg)
        print(f"📝 Transformation name: {reordered_msg}")

    ti.xcom_push(key='messages_transformed', value=transformed_messages)


# Tache 4: Formatage du timestamp
def transform_format_timestamp(**kwargs):
    ti = kwargs['ti']
    messages = ti.xcom_pull(key='messages_transformed', task_ids='transformer_name')
    
    if not messages:
        print("⚠️ No messages received for timestamp formatting!")
        return

    for msg in messages:
        msg["timestamp"] = datetime.fromtimestamp(msg["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        print(f"📝 Transformation timestamp: {msg}")

    ti.xcom_push(key='messages_transformed', value=messages)

# Fonction qui retourne l'état de pression en fonction de l'age et la pression artérielle 
def classify_blood_pressure(blood_pressure, age):
    """Détermine l'état de la pression artérielle en fonction de l'âge et de la pression."""
    systolic, diastolic = map(int, blood_pressure.split("/"))

    if age < 40:  # Jeunes adultes (18-39 ans)
        if systolic < 90 or diastolic < 60:
            return "Low"
        elif systolic >= 130 or diastolic >= 80:
            return "High"
        else:
            return "Normal"
    elif age < 60:  # Adultes d'âge moyen (40-59 ans)
        if systolic < 90 or diastolic < 60:
            return "Low"
        elif systolic >= 140 or diastolic >= 90:
            return "High"
        else:
            return "Normal"
    else:  # Personnes âgées (60+ ans)
        if systolic < 90 or diastolic < 60:
            return "Low"
        elif systolic >= 150 or diastolic >= 90:
            return "High"
        else:
            return "Normal"

# Tache 5: Calcul et ajout de l'etat de la pression artérielle 
def transform_add_blood_pressure_status(**kwargs):
    ti = kwargs['ti']
    messages = ti.xcom_pull(key='messages_transformed', task_ids='transformer_format_timestamp')

    if not messages:
        print("⚠️ No messages received for blood pressure classification!")
        return

    transformed_messages = []
    for msg in messages:
        msg["blood_pressure_status"] = classify_blood_pressure(msg["blood_pressure"], msg["age"])
        
        # Réorganiser les clés pour mettre l'etat de pression apres la pression
        reordered_msg = {
            "patient_id": msg["patient_id"],
            "first_name": msg["first_name"],
            "last_name": msg["last_name"],
            "age": msg["age"],
            "blood_pressure": msg["blood_pressure"],
            "blood_pressure_status": msg["blood_pressure_status"],
            "heart_rate": msg["heart_rate"],
            "temperature": msg["temperature"],
            "timestamp": msg["timestamp"]
        }
        
        transformed_messages.append(reordered_msg)
        print(f"📝 Transformation name: {reordered_msg}")

    ti.xcom_push(key='messages_final', value=transformed_messages)

# Stockage des messages dans Redis sous forme de string.
# def store_messages_in_redis(**kwargs):
    # """Stocke les messages transformés dans Redis sous forme STRING."""
    # ti = kwargs['ti']
    # messages = ti.xcom_pull(key='messages_final', task_ids='ajouter_etat_pression')


    # if not messages:
        # print("⚠️ No messages received for Redis storage!")
        # return

    # print(f"📦 Messages to store: {messages}")
    
    # # Connexion à Redis
    # redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

    # for msg in messages:
        # msg_key = f"patient:{msg['patient_id']}"  # Clé unique pour chaque patient
        # redis_client.set(msg_key, json.dumps(msg))  # Stockage du message JSON
        # print(f"✅ Sauvegarde en Redis: {msg_key}")

# Tache 6: Stockage des messages dans Redis sous forme de liste.
def store_messages_in_redis(**kwargs):
    """Stocke les messages transformés dans Redis sous forme de liste."""
    ti = kwargs['ti']
    messages = ti.xcom_pull(key='messages_final', task_ids='ajouter_etat_pression')

    if not messages:
        print("⚠️ No messages received for Redis storage!")
        return

    print(f"📦 Messages to store: {messages}")
    
    # Connexion à Redis
    redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

    for msg in messages:
        msg_key = f"liste_patients"  # Une seule clé pour stocker tous les patients dans une liste
        # Ajout du message JSON à la liste Redis
        redis_client.rpush(msg_key, json.dumps(msg))  
        print(f"✅ Sauvegarde en Redis (liste): {msg_key}")


#------------------------------
# Définition des tâches Airflow
#------------------------------

# Tache de production des messages vers kafka
tache_1 = PythonOperator(
    task_id='generer_data',
    python_callable=producer_data,
    dag=dag
)

# Tache de consommation des messages a partir de kafka
tache_2 = PythonOperator(
    task_id='consommer_data',
    python_callable=consumer_data,
    #provide_context=True,  # Passe le contexte Airflow
    dag=dag
)

# Tache de transformation: extraction du nom et prenom
tache_3 = PythonOperator(
    task_id='transformer_name',
    python_callable=transform_extract_name,
    #provide_context=True,  # Passe le contexte Airflow
    dag=dag
)

# Tache de formatage du timestamp
tache_4 = PythonOperator(
    task_id='transformer_format_timestamp',
    python_callable=transform_format_timestamp,
    #provide_context=True,  # Passe le contexte Airflow
    dag=dag
)

# Tache de determination de l'etat de la pression artérielle
tache_5 = PythonOperator(
    task_id='ajouter_etat_pression',
    python_callable=transform_add_blood_pressure_status,
    #provide_context=True,  # Passe le contexte Airflow
    dag=dag
)

# Tache de stockage des messages dans DB redis
tache_6 = PythonOperator(
    task_id="stocker_data_redis",
    python_callable=store_messages_in_redis,
    #provide_context=True,
    dag=dag
)

# Définition de l'ordre d'exécution
# Tache 1 et 2 en parallèle suivis successivement par les taches 3, 4, 5 et 6
[tache_1, tache_2] >> tache_3 >> tache_4 >> tache_5 >> tache_6
