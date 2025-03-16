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
    '''
    Génère et envoie des messages simulés vers un topic Kafka.

    Cette fonction crée des données fictives sur les paramètres vitaux des patients
    (ID, nom, âge, pression artérielle, fréquence cardiaque, température, timestamp)
    et les envoie à un topic Kafka en continu.

    Fonctionnement :
    - Utilise la bibliothèque Faker pour générer des données médicales fictives.
    - Configure un producteur Kafka pour envoyer les messages.
    - Envoie 20 messages à intervalles de 0,5 seconde vers le topic Kafka 'real_time_medical_messages'.
    
    Données générées par message :
    - patient_id (UUID) : Identifiant unique du patient.
    - name (str) : Nom complet du patient.
    - age (int) : Âge du patient (entre 20 et 90 ans).
    - blood_pressure (str) : Pression artérielle sous format "systolique/diastolique".
    - heart_rate (int) : Fréquence cardiaque (entre 50 et 120 bpm).
    - temperature (float) : Température corporelle (entre 36.0 et 40.0 °C).
    - timestamp (float) : Horodatage Unix du moment d’envoi du message.

    Cette fonction est utile pour tester un pipeline de traitement en temps réel basé sur Kafka.
    '''
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
    '''
    Consomme des messages depuis un topic Kafka et les stocke via XCom pour les étapes suivantes.

    Cette fonction écoute en temps réel les messages envoyés au topic Kafka 
    'real_time_medical_messages' et récupère jusqu'à 15 messages avant de s'arrêter.

    Fonctionnement :
    - Configure un consommateur Kafka connecté au broker 'kafka:9092'.
    - Écoute les messages provenant du topic spécifié.
    - Ajoute chaque message reçu à une liste et affiche son contenu.
    - Arrête la consommation après avoir reçu 15 messages.
    - Utilise XCom pour stocker les messages et les transmettre aux prochaines tâches Airflow.
    - Ferme proprement le consommateur après l'arrêt.

    Paramètres :
    - kwargs (dict) : Contient les informations du contexte d'exécution d'Airflow.
                      La clé 'ti' est utilisée pour stocker les messages avec XCom.

    Données traitées :
    - patient_id (UUID) : Identifiant unique du patient.
    - name (str) : Nom complet du patient.
    - age (int) : Âge du patient.
    - blood_pressure (str) : Pression artérielle du patient.
    - heart_rate (int) : Fréquence cardiaque du patient.
    - temperature (float) : Température corporelle en °C.
    - timestamp (float) : Horodatage du message.

    Cette fonction est conçue pour être utilisée dans un pipeline Airflow avec Kafka 
    pour le traitement en temps réel des paramètres vitaux des patients.
    '''
    
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
    '''
    Cette fonction extrait et transforme les noms complets des messages reçus, en les divisant en prénoms et noms de famille.
    Elle réorganise ensuite les données des messages, en mettant les prénoms et noms de famille en premier dans un format structuré.
    Les messages transformés sont ensuite envoyés à un autre processus via XCom.

    Paramètres :
        **kwargs: Arguments passés à la fonction, y compris l'objet 'ti' permettant d'accéder aux données du contexte du task.
        
    Retourne :
        Rien. La fonction pousse les données transformées vers XCom pour être utilisées par d'autres tâches.
    '''
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
    '''
    Cette fonction prend une liste de messages contenant des timestamps sous forme de valeurs UNIX et les transforme en un format lisible 
    (YYYY-MM-DD HH:MM:SS). Elle modifie les messages en conséquence et les renvoie dans un format transformé.

    Paramètres :
        **kwargs: Arguments passés à la fonction, y compris l'objet 'ti' permettant d'accéder aux données du contexte du task.
        
    Retourne :
        Rien. La fonction pousse les messages transformés (avec des timestamps formatés) vers XCom pour être utilisés par d'autres tâches.
    '''
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
    '''
    Détermine l'état de la pression artérielle en fonction de l'âge et de la pression artérielle systolique et diastolique.
    La classification est effectuée selon les seuils suivants :
    - Faible (Low) : systolique < 90 ou diastolique < 60
    - Normale (Normal) : entre les seuils bas et élevés selon l'âge
    - Haute (High) : systolique >= 130 ou diastolique >= 80 pour les jeunes adultes, 
      systolique >= 140 ou diastolique >= 90 pour les adultes d'âge moyen, et systolique >= 150 ou diastolique >= 90 pour les personnes âgées.
    
    Paramètres :
        blood_pressure (str) : La pression artérielle sous forme de chaîne de caractères, avec la systolique et la diastolique séparées par "/".
        age (int) : L'âge du patient.
    
    Retourne :
        str : Le statut de la pression artérielle ("Low", "Normal", "High").
    '''
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
    '''
    Cette fonction ajoute un statut de pression artérielle à chaque message en fonction de la pression artérielle et de l'âge du patient.
    Le statut est déterminé par la fonction `classify_blood_pressure` et est ajouté sous la clé "blood_pressure_status".
    Ensuite, la fonction réorganise les données des messages pour inclure ce statut après la pression artérielle.

    Paramètres :
        **kwargs : Arguments passés à la fonction, y compris l'objet 'ti' permettant d'accéder aux données du contexte du task.
        
    Retourne :
        Rien. La fonction pousse les messages transformés (avec le statut de pression artérielle ajouté) vers XCom pour être utilisés par d'autres tâches.
    '''
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
    '''
    Cette fonction stocke les messages transformés dans Redis sous forme de liste. 
    Chaque message est sérialisé en JSON et ajouté à une liste Redis unique. 
    Redis est utilisé ici pour sauvegarder les données de manière persistante, afin qu'elles puissent être récupérées par d'autres processus.

    Paramètres :
        **kwargs : Arguments passés à la fonction, y compris l'objet 'ti' permettant d'accéder aux données du contexte du task.
        
    Retourne :
        Rien. La fonction pousse les messages dans Redis pour un stockage durable.
    '''
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
    dag=dag
)

# Tache de transformation: extraction du nom et prenom
tache_3 = PythonOperator(
    task_id='transformer_name',
    python_callable=transform_extract_name,
    dag=dag
)

# Tache de formatage du timestamp
tache_4 = PythonOperator(
    task_id='transformer_format_timestamp',
    python_callable=transform_format_timestamp,
    dag=dag
)

# Tache de determination de l'etat de la pression artérielle
tache_5 = PythonOperator(
    task_id='ajouter_etat_pression',
    python_callable=transform_add_blood_pressure_status,
    dag=dag
)

# Tache de stockage des messages dans DB redis
tache_6 = PythonOperator(
    task_id="stocker_data_redis",
    python_callable=store_messages_in_redis,
    dag=dag
)

# Définition de l'ordre d'exécution
# Tache 1 et 2 en parallèle suivis successivement par les taches 3, 4, 5 et 6
[tache_1, tache_2] >> tache_3 >> tache_4 >> tache_5 >> tache_6
