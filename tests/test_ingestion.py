# tests/test_ingestion.py
#
# Tests unitaires pour src/ingestion/openfoodfacts.py
#
# On ne teste PAS les appels réseau réels.
# On utilise des mocks pour simuler les réponses de l'API
# et tester uniquement la logique de notre code.

import pytest
from unittest.mock import patch, MagicMock

from src.ingestion.openfoodfacts import (
    fetch_products_by_category,
    fetch_all_pages,
)


# ══════════════════════════════════════════════════════════════
# DONNÉES DE TEST
# ══════════════════════════════════════════════════════════════

# Réponse API simulée — reproduit exactement la structure
# que l'API Open Food Facts renvoie réellement
FAKE_API_RESPONSE = {
    "count": 222631,
    "page": 1,
    "page_size": 3,
    "products": [
        {
            "code": "3017620422003",
            "product_name": "Nutella",
            "brands": "Ferrero",
            "countries": "France",
            "packaging": "Verre, Plastique",
            "nutriscore_grade": "e",
            "ecoscore_grade": "d",
            "categories": "Pâtes à tartiner",
            "ingredients_text": "sucre, huile de palme...",
            "nutriments": {
                "energy-kcal_100g": 539,
                "fat_100g": 30.9,
                "proteins_100g": 6.3,
            }
        },
        {
            "code": "5000112637939",
            "product_name": "Coca-Cola",
            "brands": "Coca-Cola",
            "countries": "France, Belgique",
            "packaging": "Aluminium",
            "nutriscore_grade": "e",
            "ecoscore_grade": "d",
            "categories": "Boissons gazeuses",
            "ingredients_text": "eau, sucre...",
            "nutriments": {
                "energy-kcal_100g": 42,
                "fat_100g": 0,
            }
        },
    ]
}


# ══════════════════════════════════════════════════════════════
# TESTS — fetch_products_by_category
# ══════════════════════════════════════════════════════════════

class TestFetchProductsByCategory:

    def test_retourne_les_produits(self):
        """
        Cas nominal : l'API répond 200 avec des produits.

        On mock Session.get pour simuler une réponse HTTP 200
        sans faire de vrai appel réseau.

        Comment fonctionne patch :
        patch("requests.Session.get") remplace temporairement
        la méthode get() de Session par un objet MagicMock.
        À la sortie du bloc with, la vraie méthode est restaurée.
        """
        with patch("requests.Session.get") as mock_get:
            # Configure la réponse simulée
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = FAKE_API_RESPONSE
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            result = fetch_products_by_category("beverages", page=1)

            assert "products" in result
            assert len(result["products"]) == 2
            assert result["products"][0]["product_name"] == "Nutella"

    def test_leve_exception_si_cle_products_absente(self):
        """
        Si la réponse API ne contient pas 'products', on lève ValueError.

        Pourquoi ce test :
        L'API peut changer son format. Si la clé 'products' disparaît,
        on veut une erreur claire immédiatement — pas un KeyError
        mystérieux 50 lignes plus loin.
        """
        with patch("requests.Session.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"error": "bad request"}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            with pytest.raises(ValueError, match="clé 'products' absente"):
                fetch_products_by_category("beverages")

    def test_leve_exception_sur_erreur_http(self):
        """
        Une erreur HTTP (404, 500...) doit lever une exception.

        Sans raise_for_status(), une erreur 500 passerait silencieusement.
        Ce test vérifie que raise_for_status() est bien appelé.
        """
        import requests

        with patch("requests.Session.get") as mock_get:
            mock_response = MagicMock()
            mock_response.raise_for_status.side_effect = (
                requests.HTTPError("500 Server Error")
            )
            mock_get.return_value = mock_response

            with pytest.raises(requests.HTTPError):
                fetch_products_by_category("beverages")

    def test_leve_exception_sur_timeout(self):
        """
        Un timeout doit lever une exception — pas passer silencieusement.
        """
        import requests

        with patch("requests.Session.get") as mock_get:
            mock_get.side_effect = requests.Timeout("Connection timed out")

            with pytest.raises(requests.Timeout):
                fetch_products_by_category("beverages")


# ══════════════════════════════════════════════════════════════
# TESTS — fetch_all_pages
# ══════════════════════════════════════════════════════════════

class TestFetchAllPages:

    def test_recupere_toutes_les_pages(self):
        """
        fetch_all_pages appelle fetch_products_by_category
        autant de fois que nécessaire.

        On mock fetch_products_by_category directement —
        plus simple que de mocker Session.get.
        """
        fake_page_1 = {"products": [{"code": "001", "product_name": "P1"}]}
        fake_page_2 = {"products": [{"code": "002", "product_name": "P2"}]}
        fake_page_empty = {"products": []}

        with patch(
            "src.ingestion.openfoodfacts.fetch_products_by_category"
        ) as mock_fetch:
            # Simule : page 1 → 1 produit, page 2 → 1 produit,
            # page 3 → vide (fin de la pagination)
            mock_fetch.side_effect = [
                fake_page_1,
                fake_page_2,
                fake_page_empty,
            ]

            results = fetch_all_pages("beverages", max_pages=5)

            assert len(results) == 2
            assert results[0]["product_name"] == "P1"
            assert results[1]["product_name"] == "P2"
            # Vérifie qu'on a bien arrêté à la page vide
            assert mock_fetch.call_count == 3

    def test_arrete_a_max_pages(self):
        """
        fetch_all_pages respecte la limite max_pages.

        Même si l'API a plus de pages, on s'arrête à max_pages.
        """
        fake_page = {"products": [{"code": "001", "product_name": "P1"}]}

        with patch(
            "src.ingestion.openfoodfacts.fetch_products_by_category"
        ) as mock_fetch:
            mock_fetch.return_value = fake_page

            results = fetch_all_pages("beverages", max_pages=2)

            # On ne doit pas dépasser 2 pages
            assert mock_fetch.call_count == 2
            assert len(results) == 2

    def test_continue_apres_erreur_reseau(self):
        """
        Une erreur réseau sur une page intermédiaire ne fait pas
        tout planter — on retourne ce qu'on a déjà récupéré.

        Pourquoi ce comportement :
        Si on a récupéré 500 produits sur 5 pages et que la page 6
        plante, mieux vaut charger les 500 que de tout perdre.
        """
        import requests

        fake_page = {"products": [{"code": "001", "product_name": "P1"}]}

        with patch(
            "src.ingestion.openfoodfacts.fetch_products_by_category"
        ) as mock_fetch:
            mock_fetch.side_effect = [
                fake_page,                              # page 1 : OK
                requests.ConnectionError("timeout"),   # page 2 : erreur
            ]

            results = fetch_all_pages("beverages", max_pages=5)

            # On a récupéré la page 1 avant l'erreur
            assert len(results) == 1
            assert results[0]["product_name"] == "P1"