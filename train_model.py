#!/usr/bin/env python3
"""
Script d'entraînement d'un modèle de prédiction de prix de maisons
Utilise un modèle de régression linéaire simple
"""

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import joblib
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def train_house_price_model():
    """
    Entraîne un modèle simple de prédiction de prix de maisons
    Utilise des données synthétiques pour la démonstration
    """
    # Génération de données synthétiques
    np.random.seed(42)
    n_samples = 1000

    # Features: taille (m²), nombre de chambres, âge de la maison
    X = np.random.rand(n_samples, 3) * np.array([200, 5, 50])  # Normalisation

    # Prix = 1000 * taille + 50000 * chambres - 1000 * âge + bruit
    y = (X[:, 0] * 1000 + X[:, 1] * 50000 - X[:, 2] * 1000 +
         np.random.normal(0, 10000, n_samples))

    logger.info(f"Données générées: {n_samples} échantillons")
    logger.info(f"Shape X: {X.shape}, Shape y: {y.shape}")

    # Split des données
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Entraînement du modèle
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Évaluation
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    logger.info(f"Mean Squared Error: {mse:.2f}")

    # Coefficients
    logger.info(f"Coefficients: {model.coef_}")
    logger.info(f"Intercept: {model.intercept_}")

    return model

def save_model(model, filename='house_price_model.joblib'):
    """
    Sauvegarde le modèle avec joblib
    """
    joblib.dump(model, filename)
    logger.info(f"Modèle sauvegardé dans {filename}")

def load_model(filename='house_price_model.joblib'):
    """
    Charge le modèle depuis le fichier
    """
    model = joblib.load(filename)
    logger.info(f"Modèle chargé depuis {filename}")
    return model

def test_model(model):
    """
    Teste le modèle avec des données d'exemple
    """
    # Données de test
    test_data = np.array([
        [150, 3, 10],  # Maison moyenne
        [50, 1, 30],   # Petit appartement vieux
        [300, 5, 5]    # Grande maison neuve
    ])

    predictions = model.predict(test_data)

    logger.info("Prédictions de test:")
    for i, pred in enumerate(predictions):
        logger.info(f"  Maison {i+1}: {pred:.2f} € (taille: {test_data[i][0]}m², chambres: {test_data[i][1]}, âge: {test_data[i][2]} ans)")

    return predictions

if __name__ == "__main__":
    # Entraînement
    model = train_house_price_model()

    # Sauvegarde
    save_model(model)

    # Test
    test_model(model)