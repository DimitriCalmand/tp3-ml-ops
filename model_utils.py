#!/usr/bin/env python3
"""
Utilitaires pour charger et utiliser le modèle de prédiction
"""

import joblib
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_model(filename='house_price_model.joblib'):
    """
    Charge le modèle depuis le fichier joblib
    """
    try:
        model = joblib.load(filename)
        logger.info(f"Modèle chargé depuis {filename}")
        return model
    except Exception as e:
        logger.error(f"Erreur lors du chargement du modèle: {e}")
        return None

def predict_house_price(model, features):
    """
    Prédit le prix d'une maison avec les features données
    features: [taille_m2, nb_chambres, age_maison]
    """
    try:
        prediction = model.predict([features])[0]
        logger.info(f"Prédiction pour {features}: {prediction:.2f} €")
        return prediction
    except Exception as e:
        logger.error(f"Erreur lors de la prédiction: {e}")
        return None