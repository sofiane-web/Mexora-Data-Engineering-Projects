import pandas as pd
import json
import logging
from pathlib import Path
from typing import Optional

# Configuration du logger (idéalement géré dans utils/logger.py)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_csv(filepath: str | Path, source_name: str) -> Optional[pd.DataFrame]:
    """Fonction générique pour extraire un fichier CSV de manière sécurisée."""
    path = Path(filepath)
    if not path.exists():
        logging.error(f"[EXTRACT] Le fichier {source_name} est introuvable au chemin : {path}")
        return None
        
    try:
        # Forcer le type 'str' partout à l'extraction pour éviter les conversions implicites
        df = pd.read_csv(path, encoding='utf-8', dtype=str)
        logging.info(f"[EXTRACT] {source_name}: {len(df)} lignes extraites avec succès.")
        return df
    except Exception as e:
        logging.error(f"[EXTRACT] Erreur lors de la lecture de {source_name} : {e}")
        return None

def extract_commandes(filepath: str | Path) -> Optional[pd.DataFrame]:
    """Extrait les commandes depuis le fichier CSV source."""
    return extract_csv(filepath, "Commandes")

def extract_clients(filepath: str | Path) -> Optional[pd.DataFrame]:
    """Extrait les clients depuis le fichier CSV source."""
    return extract_csv(filepath, "Clients")

def extract_regions(filepath: str | Path) -> Optional[pd.DataFrame]:
    """Extrait le référentiel des régions depuis le fichier CSV source."""
    return extract_csv(filepath, "Régions (Référentiel)")

def extract_produits(filepath: str | Path) -> Optional[pd.DataFrame]:
    """Extrait les produits depuis le fichier JSON de manière sécurisée."""
    path = Path(filepath)
    if not path.exists():
        logging.error(f"[EXTRACT] Le fichier JSON Produits est introuvable : {path}")
        return None
        
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if 'produits' not in data:
            logging.error("[EXTRACT] Clé 'produits' manquante dans le JSON.")
            return None
            
        df = pd.DataFrame(data['produits'])
        logging.info(f"[EXTRACT] Produits : {len(df)} lignes extraites avec succès.")
        return df
    except json.JSONDecodeError as e:
        logging.error(f"[EXTRACT] Erreur de parsing JSON pour les produits : {e}")
        return None
    except Exception as e:
        logging.error(f"[EXTRACT] Erreur inattendue lors de la lecture des produits : {e}")
        return None