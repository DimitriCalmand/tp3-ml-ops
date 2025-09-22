#!/usr/bin/env python3
"""
Kafka Consumer JSON pour le TP3 ML-Ops
Reçoit et traite des messages JSON du topic 'calmand'
"""

from kafka import KafkaConsumer
import json
import numpy as np
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_json_message(json_str):
    """
    Traite un message JSON reçu :
    - Convertit en dictionnaire
    - Transforme les données en tableau numpy
    - Calcule la somme des valeurs
    """
    try:
        # Conversion JSON vers dictionnaire
        data_dict = json.loads(json_str)
        logger.info(f"Message JSON reçu: {json.dumps(data_dict, indent=2)}")

        # Extraction des données
        data_array = data_dict.get('data', [])
        if not data_array:
            logger.warning("Aucune donnée trouvée dans le message JSON")
            return None

        # Conversion en tableau numpy
        numpy_array = np.array(data_array)
        logger.info(f"Tableau numpy créé: {numpy_array}")

        # Calcul de la somme des valeurs
        total_sum = np.sum(numpy_array)
        logger.info(f"Somme des valeurs: {total_sum}")

        return total_sum

    except json.JSONDecodeError as e:
        logger.error(f"Erreur de décodage JSON: {e}")
        return None
    except Exception as e:
        logger.error(f"Erreur lors du traitement du message: {e}")
        return None

def main():
    # Configuration du consumer Kafka
    bootstrap_servers = ['nowledgeable.com:9092']

    # Création de l'instance KafkaConsumer
    consumer = KafkaConsumer(
        'calmand',  # Topic à écouter
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='2',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )

    logger.info("Consumer JSON Kafka démarré")
    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    logger.info("Topic écouté: calmand")
    logger.info("En attente de messages JSON... (Ctrl+C pour arrêter)")

    try:
        # Boucle pour récupérer les messages
        for message in consumer:
            logger.info("=" * 60)
            logger.info("Nouveau message reçu:")
            logger.info(f"  Topic: {message.topic}")
            logger.info(f"  Partition: {message.partition}")
            logger.info(f"  Offset: {message.offset}")
            logger.info(f"  Timestamp: {message.timestamp}")

            # Traitement du message JSON
            result = process_json_message(message.value)

            if result is not None:
                print(f"🎯 Résultat du calcul: {result}")
            else:
                print("❌ Erreur lors du traitement du message")

            print("-" * 60)

    except KeyboardInterrupt:
        logger.info("Arrêt du consumer JSON demandé par l'utilisateur")
    except Exception as e:
        logger.error(f"Erreur dans le consumer JSON: {e}")
    finally:
        consumer.close()
        logger.info("Consumer JSON fermé")

if __name__ == "__main__":
    main()