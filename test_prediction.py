#!/usr/bin/env python3
"""
Consumer de test pour le topic 'prediction_dimitri'
"""

from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    consumer = KafkaConsumer(
        'prediction_dimitri',
        bootstrap_servers=['nowledgeable.com:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test-prediction-group',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )

    logger.info("Consumer de test pour topic 'prediction_dimitri' démarré")

    try:
        for message in consumer:
            logger.info(f"Prédiction reçue: {message.value}")
            data = json.loads(message.value)
            print(f"🎯 Prédiction: {data.get('predicted_price', 'N/A')} € pour features {data.get('features', [])}")
            break  # Arrêter après le premier message
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()