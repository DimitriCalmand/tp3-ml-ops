#!/usr/bin/env python3
"""
Kafka Producer pour le TP3 ML-Ops
Ce script envoie des messages sur le topic 'exo1'
"""

from kafka import KafkaProducer
import json
import logging
import time

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    bootstrap_servers = ['nowledgeable.com:9092']
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: x.encode('utf-8'),
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10
    )
    
    logger.info(f"Producer Kafka démarré")
    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    logger.info(f"Topic cible: exo1")
    
    try:
        message = "C'est Dimitri"
        
        logger.info(f"Envoi du message: '{message}'")
        
        future = producer.send('exo1', value=message)
        
        record_metadata = future.get(timeout=10)
        
        logger.info(f"Message envoyé avec succès!")
        logger.info(f"  Topic: {record_metadata.topic}")
        logger.info(f"  Partition: {record_metadata.partition}")
        logger.info(f"  Offset: {record_metadata.offset}")
        logger.info(f"  Timestamp: {record_metadata.timestamp}")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'envoi du message: {e}")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer fermé")

if __name__ == "__main__":
    main()