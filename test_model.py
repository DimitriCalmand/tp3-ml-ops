#!/usr/bin/env python3
"""
Test du chargement et de la prédiction du modèle
"""

from model_utils import load_model, predict_house_price

def main():
    # Charger le modèle
    model = load_model()
    if model is None:
        print("Erreur: Impossible de charger le modèle")
        return

    # Tester avec des données d'exemple
    test_cases = [
        [150, 3, 10],  # Maison moyenne
        [50, 1, 30],   # Petit appartement vieux
        [300, 5, 5],   # Grande maison neuve
        [100, 2, 20]   # Maison standard
    ]

    print("Test des prédictions:")
    for features in test_cases:
        prediction = predict_house_price(model, features)
        if prediction is not None:
            print(f"  Features {features} -> Prix prédit: {prediction:.2f} €")

if __name__ == "__main__":
    main()