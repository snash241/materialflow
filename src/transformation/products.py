# src/transformation/products.py

import json
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Mots-clés pour détecter les matériaux d'emballage.
# Chaque entrée = (mot_clé_à_chercher, colonne_cible)
# On cherche en minuscules pour ignorer la casse.
PACKAGING_KEYWORDS = {
    "has_plastic": [
        "plastic", "plastique", "pet", "pehd", "hdpe",
        "polypropylene", "polypropylène", "polyethylene",
        "polyéthylène", "pp", "ps", "pvc",
    ],
    "has_cardboard": [
        "cardboard", "carton", "papier", "paper",
        "kraft", "cellulose",
    ],
    "has_glass": [
        "glass", "verre",
    ],
    "has_metal": [
        "metal", "métal", "aluminium", "aluminum",
        "acier", "steel", "tin", "fer",
    ],
}


def _detect_packaging(packaging_raw: Optional[str]) -> dict:
    """
    Détecte les matériaux d'emballage depuis le texte brut.

    Args:
        packaging_raw : texte brut ex. "Plastic bottle, Cardboard box"

    Returns:
        dict avec has_plastic, has_cardboard, has_glass, has_metal

    Exemple :
        "Plastic bottle, Verre"
        → {"has_plastic": True, "has_cardboard": False,
           "has_glass": True, "has_metal": False}

    Pourquoi on passe en minuscules :
    "Plastic" et "plastic" et "PLASTIC" sont le même matériau.
    Sans lower(), on raterait les variantes de casse.
    """
    result = {key: False for key in PACKAGING_KEYWORDS}

    if not packaging_raw or pd.isna(packaging_raw):
        return result

    # On passe tout en minuscules pour la comparaison
    packaging_lower = packaging_raw.lower()

    for column, keywords in PACKAGING_KEYWORDS.items():
        for keyword in keywords:
            if keyword in packaging_lower:
                result[column] = True
                break  # un seul match suffit pour cette colonne

    return result


def _extract_nutriments(nutriments_json: Optional[str]) -> dict:
    """
    Extrait les valeurs nutritionnelles depuis le JSON brut.

    Args:
        nutriments_json : chaîne JSON ex. '{"energy-kcal_100g": 539}'

    Returns:
        dict avec energy_kcal, fat_g, etc. — None si absent ou invalide

    Pourquoi on retourne None et pas 0 pour les valeurs absentes :
    0 signifie "ce produit contient 0g de graisses" — une vraie valeur.
    None signifie "on ne sait pas" — une information manquante.
    Ce sont deux choses différentes, on ne les confond pas.
    """
    empty = {
        "energy_kcal": None,
        "fat_g": None,
        "saturated_fat_g": None,
        "carbohydrates_g": None,
        "sugars_g": None,
        "proteins_g": None,
        "salt_g": None,
    }

    if not nutriments_json or pd.isna(nutriments_json):
        return empty

    try:
        data = json.loads(nutriments_json)
    except (json.JSONDecodeError, TypeError):
        # Le JSON est malformé — on retourne des nulls
        # plutôt que de planter toute la transformation
        logger.warning(
            f"JSON nutriments malformé : {nutriments_json[:50]}..."
        )
        return empty

    def safe_float(value) -> Optional[float]:
        """
        Convertit une valeur en float sans planter.
        Retourne None si la conversion est impossible.
        """
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    return {
        "energy_kcal":      safe_float(data.get("energy-kcal_100g")),
        "fat_g":            safe_float(data.get("fat_100g")),
        "saturated_fat_g":  safe_float(data.get("saturated-fat_100g")),
        "carbohydrates_g":  safe_float(data.get("carbohydrates_100g")),
        "sugars_g":         safe_float(data.get("sugars_100g")),
        "proteins_g":       safe_float(data.get("proteins_100g")),
        "salt_g":           safe_float(data.get("salt_100g")),
    }


def _clean_nutriscore(value: Optional[str]) -> Optional[str]:
    """
    Nettoie et valide le Nutri-Score.

    Valeurs valides : a, b, c, d, e (en minuscules)
    Tout le reste → None

    Pourquoi on normalise en minuscules :
    La contrainte CHECK en base attend 'a','b','c','d','e'.
    L'API peut renvoyer 'A', 'B', etc.
    """
    if not value or pd.isna(value):
        return None

    cleaned = str(value).strip().lower()

    if cleaned in ("a", "b", "c", "d", "e"):
        return cleaned

    # Valeur invalide ex. "not-applicable", "unknown", ""
    return None


def _first_value(text: Optional[str], separator: str = ",") -> Optional[str]:
    """
    Extrait la première valeur d'une liste séparée par des virgules.

    Exemple :
        "Ferrero, Nutella Brand, Ferrero Group" → "Ferrero"
        "France" → "France"
        None → None
    """
    if not text or pd.isna(text):
        return None

    parts = text.split(separator)
    first = parts[0].strip()
    return first if first else None


def transform_raw_to_staging(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme un DataFrame de raw.products vers le format staging.

    Args:
        df_raw : DataFrame lu depuis raw.products

    Returns:
        DataFrame prêt à être chargé dans staging.products

    C'est la fonction principale — elle appelle toutes
    les fonctions de nettoyage définies au-dessus.
    """
    if df_raw.empty:
        logger.warning("DataFrame raw vide — rien à transformer")
        return pd.DataFrame()

    logger.info(f"Transformation de {len(df_raw)} lignes raw...")

    rows = []
    rejected = 0

    for _, row in df_raw.iterrows():
        try:
            # Nettoyage de chaque champ
            packaging_info = _detect_packaging(row.get("packaging"))
            nutriments_info = _extract_nutriments(row.get("nutriments_json"))
            nutriscore = _clean_nutriscore(row.get("nutriscore_grade"))
            brand = _first_value(row.get("brands"))
            country = _first_value(row.get("countries"))
            category = _first_value(row.get("categories"), separator=",")

            # Un produit sans barcode ni nom n'est pas utilisable
            barcode = row.get("barcode")
            product_name = row.get("product_name")

            if not barcode and not product_name:
                rejected += 1
                continue

            staged_row = {
                "barcode":          barcode or "UNKNOWN",
                "product_name":     product_name or "Sans nom",
                "brand":            brand,
                "country":          country,
                "packaging_raw":    row.get("packaging"),
                "nutriscore_grade": nutriscore,
                "ecoscore_grade":   row.get("ecoscore_grade"),
                "main_category":    category,
                "raw_product_id":   row.get("id"),
                "pipeline_run_id":  row.get("pipeline_run_id"),
                **packaging_info,   # has_plastic, has_cardboard, etc.
                **nutriments_info,  # energy_kcal, fat_g, etc.
            }

            rows.append(staged_row)

        except Exception as e:
            # Une ligne qui plante ne doit pas bloquer les autres
            logger.warning(
                f"Ligne rejetée (barcode={row.get('barcode')}) : {e}"
            )
            rejected += 1
            continue

    df_staging = pd.DataFrame(rows)

    # Dédoublonnage sur barcode — garde la ligne la plus récente
    if not df_staging.empty and "barcode" in df_staging.columns:
        before = len(df_staging)
        df_staging = df_staging.drop_duplicates(
            subset=["barcode"],
            keep="last"
        )
        dupes = before - len(df_staging)
        if dupes > 0:
            logger.info(f"{dupes} doublons supprimés")

    logger.info(
        f"Transformation terminée — "
        f"{len(df_staging)} lignes OK, {rejected} rejetées"
    )

    return df_staging