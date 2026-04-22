# tests/test_transformation.py
#
# Tests unitaires pour src/transformation/products.py
#
# Principe : on teste chaque fonction de transformation
# avec des données connues et des cas limites.
# On ne teste PAS la connexion DB ni l'API — juste la logique pure.

import json
import pytest
import pandas as pd

from src.transformation.products import (
    _detect_packaging,
    _extract_nutriments,
    _clean_nutriscore,
    _first_value,
    transform_raw_to_staging,
)


# ══════════════════════════════════════════════════════════════
# TESTS — _detect_packaging
# ══════════════════════════════════════════════════════════════

class TestDetectPackaging:
    """
    Groupe de tests pour la détection des matériaux d'emballage.

    Pourquoi une classe et pas des fonctions isolées :
    Regrouper les tests liés à une même fonction aide à naviguer.
    Si 5 tests de _detect_packaging échouent, tu sais immédiatement
    que le problème est dans cette fonction.
    """

    def test_detecte_plastique(self):
        """Cas nominal : plastique dans le texte."""
        result = _detect_packaging("Plastic bottle")
        assert result["has_plastic"] is True
        assert result["has_glass"] is False
        assert result["has_cardboard"] is False
        assert result["has_metal"] is False

    def test_detecte_verre(self):
        """Cas nominal : verre dans le texte."""
        result = _detect_packaging("Verre consigné")
        assert result["has_glass"] is True
        assert result["has_plastic"] is False

    def test_detecte_plusieurs_materiaux(self):
        """Cas réaliste : plusieurs matériaux dans le même texte."""
        result = _detect_packaging("Plastic bottle, Verre, Cardboard box")
        assert result["has_plastic"] is True
        assert result["has_glass"] is True
        assert result["has_cardboard"] is True
        assert result["has_metal"] is False

    def test_insensible_a_la_casse(self):
        """
        La casse ne doit pas influencer la détection.

        Pourquoi ce test est important :
        L'API Open Food Facts renvoie des données saisies par des
        humains. On trouve "PLASTIC", "Plastic", "plastic" dans
        le même dataset. Sans ce test, une majuscule rate la détection.
        """
        result_upper = _detect_packaging("PLASTIC BOTTLE")
        result_lower = _detect_packaging("plastic bottle")
        result_mixed = _detect_packaging("Plastic Bottle")

        assert result_upper["has_plastic"] is True
        assert result_lower["has_plastic"] is True
        assert result_mixed["has_plastic"] is True

    def test_valeur_none(self):
        """
        None doit retourner tous les matériaux à False.

        Pourquoi ce test est critique :
        Beaucoup de produits Open Food Facts n'ont pas de champ
        packaging renseigné. Sans ce test, un None ferait planter
        la transformation sur `.lower()`.
        """
        result = _detect_packaging(None)
        assert result["has_plastic"] is False
        assert result["has_glass"] is False
        assert result["has_cardboard"] is False
        assert result["has_metal"] is False

    def test_chaine_vide(self):
        """Une chaîne vide doit retourner tous les matériaux à False."""
        result = _detect_packaging("")
        assert result["has_plastic"] is False

    def test_detecte_aluminium(self):
        """Aluminium est un alias de métal."""
        result = _detect_packaging("Aluminium can")
        assert result["has_metal"] is True

    def test_detecte_pet(self):
        """PET est un type de plastique courant."""
        result = _detect_packaging("Bouteille PET")
        assert result["has_plastic"] is True

    def test_retourne_toutes_les_cles(self):
        """
        Le dict retourné doit toujours contenir les 4 clés.

        Pourquoi ce test :
        Si on ajoute un nouveau matériau demain (ex: has_bioplastic),
        ce test échoue — signal qu'il faut mettre à jour les appelants.
        """
        result = _detect_packaging("quelque chose")
        assert set(result.keys()) == {
            "has_plastic", "has_cardboard", "has_glass", "has_metal"
        }


# ══════════════════════════════════════════════════════════════
# TESTS — _clean_nutriscore
# ══════════════════════════════════════════════════════════════

class TestCleanNutriscore:

    def test_valeurs_valides_minuscules(self):
        """Les valeurs a-e en minuscules sont valides."""
        for grade in ["a", "b", "c", "d", "e"]:
            assert _clean_nutriscore(grade) == grade

    def test_valeurs_valides_majuscules(self):
        """
        Les valeurs A-E en majuscules doivent être normalisées.

        Pourquoi : l'API renvoie parfois "A" au lieu de "a".
        La contrainte CHECK en base attend des minuscules.
        Sans cette normalisation, l'insertion plante.
        """
        assert _clean_nutriscore("A") == "a"
        assert _clean_nutriscore("E") == "e"

    def test_valeur_none(self):
        """None doit retourner None — pas une erreur."""
        assert _clean_nutriscore(None) is None

    def test_valeur_invalide(self):
        """
        Les valeurs invalides doivent retourner None.

        Pourquoi : l'API renvoie parfois "not-applicable",
        "unknown", ou des chaînes vides. En base, la contrainte
        CHECK n'accepte que a-e. On filtre ici avant l'insertion.
        """
        assert _clean_nutriscore("not-applicable") is None
        assert _clean_nutriscore("unknown") is None
        assert _clean_nutriscore("") is None
        assert _clean_nutriscore("z") is None

    def test_espaces_ignores(self):
        """Les espaces en début/fin doivent être ignorés."""
        assert _clean_nutriscore("  a  ") == "a"
        assert _clean_nutriscore(" B ") == "b"


# ══════════════════════════════════════════════════════════════
# TESTS — _extract_nutriments
# ══════════════════════════════════════════════════════════════

class TestExtractNutriments:

    def test_extraction_normale(self):
        """Cas nominal : JSON bien formé avec les bonnes clés."""
        nutriments = json.dumps({
            "energy-kcal_100g": 539,
            "fat_100g": 30.9,
            "proteins_100g": 6.3,
            "salt_100g": 0.107,
        })
        result = _extract_nutriments(nutriments)

        assert result["energy_kcal"] == 539.0
        assert result["fat_g"] == 30.9
        assert result["proteins_g"] == 6.3
        assert result["salt_g"] == 0.107

    def test_valeur_none(self):
        """None doit retourner un dict avec toutes les valeurs à None."""
        result = _extract_nutriments(None)

        assert result["energy_kcal"] is None
        assert result["fat_g"] is None
        assert result["proteins_g"] is None

    def test_json_malforme(self):
        """
        Un JSON malformé doit retourner des nulls sans planter.

        Pourquoi ce test est critique :
        Open Food Facts est saisi par des humains. On trouve des JSON
        malformés dans le dataset. Sans ce test, une seule ligne
        corrompue ferait planter toute la transformation.
        """
        result = _extract_nutriments("{invalid json}")
        assert result["energy_kcal"] is None
        assert result["fat_g"] is None

    def test_cles_absentes(self):
        """
        Un JSON valide mais sans les bonnes clés retourne des nulls.

        Pourquoi : si l'API change ses noms de clés, on ne plante
        pas — on retourne None et on continue.
        """
        nutriments = json.dumps({"energy": 100})  # mauvaise clé
        result = _extract_nutriments(nutriments)
        assert result["energy_kcal"] is None

    def test_retourne_toutes_les_cles(self):
        """Le dict retourné doit toujours contenir les 7 clés."""
        result = _extract_nutriments(None)
        expected_keys = {
            "energy_kcal", "fat_g", "saturated_fat_g",
            "carbohydrates_g", "sugars_g", "proteins_g", "salt_g"
        }
        assert set(result.keys()) == expected_keys


# ══════════════════════════════════════════════════════════════
# TESTS — _first_value
# ══════════════════════════════════════════════════════════════

class TestFirstValue:

    def test_extrait_premiere_valeur(self):
        """Cas nominal : liste de valeurs séparées par virgule."""
        assert _first_value("Ferrero, Nutella Brand, Ferrero Group") == "Ferrero"

    def test_valeur_unique(self):
        """Une seule valeur sans séparateur."""
        assert _first_value("France") == "France"

    def test_none(self):
        """None retourne None."""
        assert _first_value(None) is None

    def test_espaces_nettoyes(self):
        """Les espaces autour de la première valeur sont supprimés."""
        assert _first_value("  Ferrero  , Nutella") == "Ferrero"


# ══════════════════════════════════════════════════════════════
# TESTS — transform_raw_to_staging (fonction principale)
# ══════════════════════════════════════════════════════════════

class TestTransformRawToStaging:
    """
    Tests d'intégration légère — on teste la fonction principale
    avec un DataFrame construit en mémoire, sans connexion DB.
    """

    def _make_raw_df(self, **overrides) -> pd.DataFrame:
        """
        Crée un DataFrame raw minimal pour les tests.

        Pourquoi une méthode helper :
        Évite de répéter la construction du DataFrame dans chaque test.
        Si la structure de raw change, on met à jour ici une seule fois.
        """
        base = {
            "id": [1],
            "barcode": ["TEST001"],
            "product_name": ["Produit test"],
            "brands": ["Marque A, Marque B"],
            "countries": ["France, Belgique"],
            "packaging": ["Plastic bottle, Verre"],
            "nutriscore_grade": ["b"],
            "ecoscore_grade": ["c"],
            "categories": ["Boissons, Eaux"],
            "ingredients_text": ["eau, sucre"],
            "nutriments_json": [json.dumps({
                "energy-kcal_100g": 45,
                "fat_100g": 0.0,
                "proteins_100g": 0.5,
            })],
            "pipeline_run_id": ["test_run"],
            "source": ["openfoodfacts"],
        }
        base.update(overrides)
        return pd.DataFrame(base)

    def test_transformation_nominale(self):
        """Cas nominal : une ligne valide est transformée correctement."""
        df_raw = self._make_raw_df()
        df_staging = transform_raw_to_staging(df_raw)
        assert len(df_staging) == 1
        assert df_staging.iloc[0]["barcode"] == "TEST001"
        assert df_staging.iloc[0]["brand"] == "Marque A"
        assert df_staging.iloc[0]["has_plastic"] == True
        assert df_staging.iloc[0]["has_glass"] == True
        assert df_staging.iloc[0]["nutriscore_grade"] == "b"
        assert df_staging.iloc[0]["energy_kcal"] == 45.0

    def test_dataframe_vide(self):
        """Un DataFrame vide retourne un DataFrame vide."""
        df_raw = pd.DataFrame()
        df_staging = transform_raw_to_staging(df_raw)
        assert df_staging.empty

    def test_ligne_sans_barcode_ni_nom_rejetee(self):
        """
        Une ligne sans barcode ET sans nom est rejetée.

        Pourquoi ce seuil précis :
        Sans barcode ni nom, le produit est non identifiable.
        Un barcode seul ou un nom seul suffit — on ne rejette
        que si les deux manquent simultanément.
        """
        df_raw = self._make_raw_df(
            barcode=[None],
            product_name=[None]
        )
        df_staging = transform_raw_to_staging(df_raw)
        assert len(df_staging) == 0

    def test_deduplication_garde_derniere_occurrence(self):
        """
        Si deux lignes ont le même barcode, on garde la dernière.

        Pourquoi keep='last' :
        Si un pipeline tourne deux fois sur les mêmes données,
        la deuxième occurrence est la plus récente — on la garde.
        """
        df_raw = pd.DataFrame({
            "id": [1, 2],
            "barcode": ["SAME001", "SAME001"],  # même barcode
            "product_name": ["Version 1", "Version 2"],
            "brands": ["Marque", "Marque"],
            "countries": ["France", "France"],
            "packaging": ["Plastic", "Plastic"],
            "nutriscore_grade": ["a", "b"],  # différent
            "ecoscore_grade": ["a", "b"],
            "categories": ["cat", "cat"],
            "ingredients_text": ["x", "x"],
            "nutriments_json": [None, None],
            "pipeline_run_id": ["run1", "run2"],
            "source": ["off", "off"],
        })
        df_staging = transform_raw_to_staging(df_raw)

        assert len(df_staging) == 1
        # La dernière occurrence a nutriscore "b"
        assert df_staging.iloc[0]["nutriscore_grade"] == "b"

    def test_nutriscore_invalide_devient_null(self):
        """Un nutriscore invalide est remplacé par NULL."""
        df_raw = self._make_raw_df(nutriscore_grade=["not-applicable"])
        df_staging = transform_raw_to_staging(df_raw)

        assert df_staging.iloc[0]["nutriscore_grade"] is None