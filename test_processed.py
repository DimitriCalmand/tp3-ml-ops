#!/usr/bin/env python3
"""
Consumer simple pour vérifier les messages du topic 'processed'
"""

from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    consumer = KafkaConsumer(
        'processed',
        bootstrap_servers=['nowledgeable.com:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test-processed-group',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )

    logger.info("Consumer de test pour topic 'processed' démarré")

    try:
        for message in consumer:
            logger.info(f"Message reçu sur 'processed': {message.value}")
            data = json.loads(message.value)
            print(f"📊 Résultat traité: {data.get('sum_result', data.get('sum', 'N/A'))}")
            break  # On arrête après le premier message pour le test
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()