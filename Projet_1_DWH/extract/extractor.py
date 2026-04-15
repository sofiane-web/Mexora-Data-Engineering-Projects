import pandas as pd
import json
import logging
from pathlib import Path
from typing import Optional

class MexoraExtractor:
    """
    Classe responsable de l'extraction des données brutes de Mexora.
    Gère la lecture des fichiers CSV et JSON avec gestion des erreurs.
    """
    def __init__(self, data_folder: str = 'data'):
        # Utilisation de Pathlib pour une meilleure gestion des chemins (Windows/Mac/Linux)
        self.data_folder = Path(data_folder)

    def _safe_read_csv(self, filename: str) -> pd.DataFrame:
        """Méthode interne (Encapsulation) pour lire les CSV en toute sécurité."""
        filepath = self.data_folder / filename
        try:
            # dtype=str pour éviter les conversions implicites (Règle du prof)
            df = pd.read_csv(filepath, encoding='utf-8', dtype=str)
            logging.info(f"[EXTRACT] {filename} : {len(df)} lignes extraites avec succès.")
            return df
        except FileNotFoundError:
            logging.error(f"[EXTRACT ERROR] Le fichier {filepath} est introuvable.")
            return pd.DataFrame() # Retourne un DataFrame vide pour ne pas crasher le pipeline
        except Exception as e:
            logging.error(f"[EXTRACT ERROR] Erreur inattendue sur {filename}: {str(e)}")
            return pd.DataFrame()

    def _safe_read_json(self, filename: str, root_key: str) -> pd.DataFrame:
        """Méthode interne pour lire les JSON en toute sécurité."""
        filepath = self.data_folder / filename
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            df = pd.DataFrame(data.get(root_key, []))
            logging.info(f"[EXTRACT] {filename} : {len(df)} lignes extraites avec succès.")
            return df
        except FileNotFoundError:
            logging.error(f"[EXTRACT ERROR] Le fichier {filepath} est introuvable.")
            return pd.DataFrame()

    # --- Méthodes publiques d'extraction ---

    def extract_commandes(self, filename: str = 'commandes_mexora.csv') -> pd.DataFrame:
        return self._safe_read_csv(filename)

    def extract_produits(self, filename: str = 'produits_mexora.json') -> pd.DataFrame:
        return self._safe_read_json(filename, root_key='produits')

    def extract_clients(self, filename: str = 'clients_mexora.csv') -> pd.DataFrame:
        return self._safe_read_csv(filename)

    def extract_regions(self, filename: str = 'regions_maroc.csv') -> pd.DataFrame:
        return self._safe_read_csv(filename)