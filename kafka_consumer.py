#!/usr/bin/env python3
"""
Kafka Consumer pour le TP3 ML-Ops
Ce script écoute les messages sur le topic 'exo1'
"""

from kafka import KafkaConsumer
import json
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    bootstrap_servers = ['nowledgeable.com:9092']
    
    # Création de l'instance KafkaConsumer
    consumer = KafkaConsumer(
        'exo1',  # Topic à écouter
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  # Lit depuis le début du topic
        enable_auto_commit=True,
        group_id='consumer-group-1',  # Groupe de consommateurs
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )
    
    logger.info(f"Consumer Kafka démarré, en écoute sur le topic 'exo1'")
    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    logger.info("En attente de messages... (Ctrl+C pour arrêter)")
    
    try:
        # Boucle pour récupérer les messages
        for message in consumer:
            logger.info(f"Message reçu:")
            logger.info(f"  Topic: {message.topic}")
            logger.info(f"  Partition: {message.partition}")
            logger.info(f"  Offset: {message.offset}")
            logger.info(f"  Timestamp: {message.timestamp}")
            logger.info(f"  Valeur: {message.value}")
            print("-" * 50)
            
    except KeyboardInterrupt:
        logger.info("Arrêt du consumer demandé par l'utilisateur")
    except Exception as e:
        logger.error(f"Erreur dans le consumer: {e}")
    finally:
        consumer.close()
        logger.info("Consumer fermé")

if __name__ == "__main__":
    main()