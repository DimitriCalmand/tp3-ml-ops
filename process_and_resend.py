#!/usr/bin/env python3
"""
Kafka Processor pour le TP3 ML-Ops
Script hybride Consumer + Producer
- Consomme les messages JSON du topic 'calmand'
- Traite les données (calcule la somme)
- Renvoie le résultat sur le topic 'processed'
"""

from kafka import KafkaConsumer, KafkaProducer
import json
import numpy as np
import logging
import time

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_and_send_result(consumer, producer, json_str):
    """
    Traite un message JSON reçu et envoie le résultat :
    - Convertit en dictionnaire
    - Transforme les données en tableau numpy
    - Calcule la somme des valeurs
    - Envoie le résultat sur le topic 'processed'
    """
    try:
        # Conversion JSON vers dictionnaire
        data_dict = json.loads(json_str)
        logger.info(f"Message JSON reçu: {json.dumps(data_dict, indent=2)}")

        # Extraction des données
        data_array = data_dict.get('data', [])
        if not data_array:
            logger.warning("Aucune donnée trouvée dans le message JSON")
            return False

        # Conversion en tableau numpy
        numpy_array = np.array(data_array)
        logger.info(f"Tableau numpy créé: {numpy_array}")

        # Calcul de la somme des valeurs
        total_sum = np.sum(numpy_array)
        logger.info(f"Somme des valeurs calculée: {total_sum}")

        # Création du message résultat
        result_message = {
            "original_data": data_array,
            "sum_result": int(total_sum),  # Convertir en int pour JSON
            "processed_at": time.time(),
            "processor": "process_and_resend.py"
        }

        # Envoi du résultat sur le topic 'processed'
        logger.info(f"Envoi du résultat sur topic 'processed': {json.dumps(result_message, indent=2)}")

        future = producer.send('processed', value=result_message)
        record_metadata = future.get(timeout=10)

        logger.info("Résultat envoyé avec succès!")
        logger.info(f"  Topic: {record_metadata.topic}")
        logger.info(f"  Partition: {record_metadata.partition}")
        logger.info(f"  Offset: {record_metadata.offset}")

        return True

    except json.JSONDecodeError as e:
        logger.error(f"Erreur de décodage JSON: {e}")
        return False
    except Exception as e:
        logger.error(f"Erreur lors du traitement du message: {e}")
        return False

def main():
    # Configuration commune
    bootstrap_servers = ['nowledgeable.com:9092']

    # Création du consumer
    consumer = KafkaConsumer(
        'calmand',  # Topic source
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='processor-group-1',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )

    # Création du producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10
    )

    logger.info("Processor hybride Kafka démarré")
    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    logger.info("Topic source (consumer): calmand")
    logger.info("Topic destination (producer): processed")
    logger.info("En attente de messages JSON à traiter... (Ctrl+C pour arrêter)")

    try:
        # Boucle principale de traitement
        for message in consumer:
            logger.info("=" * 80)
            logger.info("Nouveau message reçu à traiter:")
            logger.info(f"  Topic: {message.topic}")
            logger.info(f"  Partition: {message.partition}")
            logger.info(f"  Offset: {message.offset}")
            logger.info(f"  Timestamp: {message.timestamp}")

            # Traitement du message et envoi du résultat
            success = process_and_send_result(consumer, producer, message.value)

            if success:
                print("✅ Message traité et résultat envoyé avec succès!")
            else:
                print("❌ Erreur lors du traitement du message")

            print("-" * 80)

    except KeyboardInterrupt:
        logger.info("Arrêt du processor demandé par l'utilisateur")
    except Exception as e:
        logger.error(f"Erreur dans le processor: {e}")
    finally:
        # Fermeture propre des connexions
        consumer.close()
        producer.flush()
        producer.close()
        logger.info("Processor fermé")

if __name__ == "__main__":
    main()