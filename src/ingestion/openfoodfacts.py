# src/ingestion/openfoodfacts.py

import logging
import time
#from typing import Optional
from typing import Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import config

# Logger spécifique à ce module.
# __name__ vaut "src.ingestion.openfoodfacts" — dans les logs
# tu sais exactement d'où vient chaque message.
logger = logging.getLogger(__name__)


def _build_session() -> requests.Session:
    """
    Crée une session HTTP réutilisable avec retry automatique.

    Retry(total=3) = 3 tentatives maximum avant d'abandonner
    backoff_factor=1 = attend 1s après le 1er échec, 2s après
    le 2ème, 4s après le 3ème (progression exponentielle)
    status_forcelist = codes HTTP qui déclenchent un retry :
        429 = trop de requêtes (rate limiting)
        500 = erreur serveur interne
        502 = bad gateway
        503 = service indisponible
        504 = gateway timeout
    """
    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Open Food Facts demande un User-Agent identifiable
    # dans leur politique d'usage — on respecte ça
    session.headers.update({
        "User-Agent": "MaterialFlow-DataPipeline/1.0 (educational project)",
        "Accept": "application/json",
    })

    return session


def fetch_products_by_category(
    category: str,
    page: int = 1,
    page_size: Optional[int] = None,
) -> dict:
    """
    Récupère une page de produits pour une catégorie donnée.

    Args:
        category  : catégorie Open Food Facts (ex: "beverages")
        page      : numéro de page, commence à 1
        page_size : produits par page (défaut : config)

    Returns:
        dict brut retourné par l'API — on ne touche à rien

    Raises:
        requests.Timeout       : l'API n'a pas répondu à temps
        requests.ConnectionError : réseau inaccessible
        requests.HTTPError     : code HTTP >= 400
        ValueError             : réponse API mal formée
    """
    ps = page_size or config.api.page_size

    params = {
        "action": "process",
        "tagtype_0": "categories",
        "tag_contains_0": "contains",
        "tag_0": category,
        "sort_by": "unique_scans_n",
        "page_size": ps,
        "page": page,
        "json": 1,
        "fields": (
            "code,product_name,brands,countries,packaging,"
            "nutriscore_grade,ecoscore_grade,categories,"
            "ingredients_text,nutriments"
        ),
    }

    logger.info(
        f"Appel API — catégorie='{category}' "
        f"page={page} page_size={ps}"
    )

    session = _build_session()

    try:
        response = session.get(
            config.api.base_url,
            params=params,
            timeout=config.api.timeout,
        )

        # Lève HTTPError si code >= 400
        response.raise_for_status()

        data = response.json()

        # Validation minimale — l'API peut changer
        if "products" not in data:
            raise ValueError(
                f"Réponse API inattendue — clé 'products' absente. "
                f"Clés reçues : {list(data.keys())}"
            )

        nb = len(data.get("products", []))
        logger.info(
            f"OK — {nb} produits reçus "
            f"(total dispo : {data.get('count', '?')})"
        )

        return data

    except requests.Timeout:
        logger.error(
            f"Timeout après {config.api.timeout}s — "
            f"catégorie='{category}' page={page}"
        )
        raise

    except requests.ConnectionError as e:
        logger.error(f"Erreur réseau : {e}")
        raise

    except requests.HTTPError as e:
        status = e.response.status_code if e.response else "inconnu"
        logger.error(
            f"Erreur HTTP {status} — "
            f"catégorie='{category}' page={page}"
        )
        raise

    except ValueError as e:
        logger.error(f"Réponse API invalide : {e}")
        raise


def fetch_all_pages(
    category: str,
    max_pages: Optional[int] = None,
    delay_between_pages: float = 1.0,
) -> List[dict]:
    """
    Récupère toutes les pages pour une catégorie.

    Args:
        category             : catégorie à récupérer
        max_pages            : limite de pages (défaut : config)
        delay_between_pages  : pause en secondes entre chaque appel

    Returns:
        liste de tous les produits bruts récupérés

    Pourquoi delay_between_pages :
    Open Food Facts est un projet collaboratif sans gros budget.
    Appeler l'API sans pause = risque de se faire bloquer.
    1 seconde entre les appels est un minimum de courtoisie.
    """
    max_p = max_pages or config.api.max_pages
    all_products = []

    for page in range(1, max_p + 1):
        try:
            data = fetch_products_by_category(
                category=category,
                page=page,
            )
            products = data.get("products", [])

            # Liste vide = plus rien à récupérer
            if not products:
                logger.info(
                    f"Page {page} vide — arrêt. "
                    f"Total : {len(all_products)} produits"
                )
                break

            all_products.extend(products)
            logger.info(
                f"Page {page}/{max_p} — "
                f"cumul : {len(all_products)} produits"
            )

            # Pause entre les pages sauf après la dernière
            if page < max_p and products:
                time.sleep(delay_between_pages)

        except (requests.Timeout, requests.ConnectionError) as e:
            # Erreur réseau sur une page intermédiaire :
            # on garde ce qu'on a déjà plutôt que tout perdre
            logger.warning(
                f"Erreur réseau page {page} — on s'arrête là. "
                f"Récupéré : {len(all_products)} produits. "
                f"Erreur : {e}"
            )
            break

        except Exception as e:
            # Erreur inattendue — on propage pour que
            # le pipeline sache que quelque chose s'est mal passé
            logger.error(f"Erreur inattendue page {page} : {e}")
            raise

    logger.info(
        f"Ingestion terminée — {len(all_products)} produits "
        f"pour '{category}'"
    )
    return all_products
