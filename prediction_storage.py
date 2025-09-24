#!/usr/bin/env python3
"""
Consumer qui stocke les prédictions dans une base de données SQLite
"""

from kafka import KafkaConsumer
import json
import logging
import sqlite3
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_database():
    """Initialise la base de données SQLite"""
    conn = sqlite3.connect('predictions.db')
    cursor = conn.cursor()

    # Créer la table si elle n'existe pas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            features TEXT NOT NULL,
            predicted_price REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            source_offset INTEGER,
            received_at REAL
        )
    ''')

    conn.commit()
    return conn

def store_prediction(conn, prediction_data):
    """Stocke une prédiction dans la base de données"""
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO predictions (features, predicted_price, timestamp, source_offset, received_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (
        json.dumps(prediction_data['features']),
        prediction_data['predicted_price'],
        prediction_data['timestamp'],
        prediction_data.get('source_offset'),
        time.time()
    ))

    conn.commit()
    logger.info(f"Prédiction stockée: {prediction_data['predicted_price']} €")

def main():
    # Initialiser la base de données
    conn = init_database()
    logger.info("Base de données initialisée")

    # Configuration du consumer
    consumer = KafkaConsumer(
        'prediction_dimitri',
        bootstrap_servers=['nowledgeable.com:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='storage-consumer-group',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )

    logger.info("Consumer de stockage démarré - en attente de prédictions...")

    try:
        for message in consumer:
            logger.info(f"Prédiction reçue: {message.value}")

            try:
                prediction_data = json.loads(message.value)
                store_prediction(conn, prediction_data)
                print(f"💾 Prédiction stockée: {prediction_data['predicted_price']} €")

            except json.JSONDecodeError as e:
                logger.error(f"Erreur JSON: {e}")
            except Exception as e:
                logger.error(f"Erreur stockage: {e}")

    except KeyboardInterrupt:
        logger.info("Arrêt du consumer de stockage")
    finally:
        conn.close()
        consumer.close()
        logger.info("Consumer de stockage fermé")

if __name__ == "__main__":
    main()