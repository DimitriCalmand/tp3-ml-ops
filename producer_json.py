#!/usr/bin/env python3
"""
Kafka Producer JSON pour le TP3 ML-Ops
Envoie des messages JSON sur le topic 'calmand'
"""

from kafka import KafkaProducer
import json
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Configuration du producer Kafka
    bootstrap_servers = ['nowledgeable.com:9092']

    # Création de l'instance KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),  # Sérialisation JSON en UTF-8
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10
    )

    logger.info("Producer JSON Kafka démarré")
    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    logger.info("Topic cible: calmand")

    try:
        # Création du message JSON avec features de maison
        message = {
            "features": [150, 3, 10]  # taille_m2, nb_chambres, age_maison
        }

        logger.info(f"Envoi du message JSON: {json.dumps(message, indent=2)}")

        # Envoi du message sur le topic 'calmand'
        future = producer.send('calmand', value=message)

        # Attendre la confirmation d'envoi
        record_metadata = future.get(timeout=10)

        logger.info("Message JSON envoyé avec succès!")
        logger.info(f"  Topic: {record_metadata.topic}")
        logger.info(f"  Partition: {record_metadata.partition}")
        logger.info(f"  Offset: {record_metadata.offset}")
        logger.info(f"  Timestamp: {record_metadata.timestamp}")

    except Exception as e:
        logger.error(f"Erreur lors de l'envoi du message JSON: {e}")
    finally:
        # Fermeture propre du producer
        producer.flush()
        producer.close()
        logger.info("Producer JSON fermé")

if __name__ == "__main__":
    main()