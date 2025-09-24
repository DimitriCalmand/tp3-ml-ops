#!/usr/bin/env python3
"""
Consumer ML pour prédire le prix des maisons
Consomme les features depuis 'calmand' et affiche la prédiction
"""

from kafka import KafkaConsumer, KafkaProducer
import json
import logging
from model_utils import load_model, predict_house_price

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Charger le modèle au démarrage
    model = load_model()
    if model is None:
        logger.error("Impossible de charger le modèle. Arrêt.")
        return

    # Configuration commune
    bootstrap_servers = ['nowledgeable.com:9092']

    # Configuration du consumer
    consumer = KafkaConsumer(
        'calmand',
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='ml-consumer-group',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )

    # Configuration du producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10
    )

    logger.info("Consumer ML démarré - en attente de features de maisons...")
    logger.info("Prédictions seront envoyées sur le topic 'prediction_dimitri'")

    try:
        for message in consumer:
            logger.info("=" * 60)
            logger.info(f"Message reçu - Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")

            try:
                # Parser le JSON
                data = json.loads(message.value)
                logger.info(f"Données reçues: {json.dumps(data, indent=2)}")

                # Extraire les features
                features = data.get('features', [])
                if not features or len(features) != 3:
                    logger.warning("Features manquantes ou invalides")
                    continue

                # Faire la prédiction
                prediction = predict_house_price(model, features)
                if prediction is not None:
                    print(f"🏠 Prédiction de prix: {prediction:.2f} € pour maison {features} (taille: {features[0]}m², chambres: {features[1]}, âge: {features[2]} ans)")

                    # Créer le message de prédiction
                    prediction_message = {
                        "features": features,
                        "predicted_price": round(prediction, 2),
                        "timestamp": message.timestamp,
                        "source_offset": message.offset
                    }

                    # Envoyer sur le topic de prédictions
                    logger.info(f"Envoi de la prédiction sur 'prediction_dimitri': {json.dumps(prediction_message, indent=2)}")
                    future = producer.send('prediction_dimitri', value=prediction_message)
                    record_metadata = future.get(timeout=10)
                    logger.info(f"Prédiction envoyée - Topic: {record_metadata.topic}, Offset: {record_metadata.offset}")

            except json.JSONDecodeError as e:
                logger.error(f"Erreur JSON: {e}")
            except Exception as e:
                logger.error(f"Erreur traitement: {e}")

    except KeyboardInterrupt:
        logger.info("Arrêt du consumer ML")
    finally:
        consumer.close()
        producer.flush()
        producer.close()
        logger.info("Consumer ML fermé")

if __name__ == "__main__":
    main()