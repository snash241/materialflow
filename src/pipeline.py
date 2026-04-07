# src/pipeline.py
#
# Orchestrateur du pipeline ELT MaterialFlow.
# Ce fichier assemble les 4 modules dans le bon ordre.
# Il est aussi le point d'entrée pour Airflow (étape 7).

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from src.config import config
from src.ingestion.openfoodfacts import fetch_all_pages
from src.loading.postgres import (
    get_engine,
    load_dataframe,
    log_pipeline_run,
    test_connection,
)
from src.transformation.products import transform_raw_to_staging

# Logger de ce module
logger = logging.getLogger(__name__)


def run_pipeline(
    category: str,
    max_pages: int = None,
) -> dict:
    """
    Exécute le pipeline ELT complet pour une catégorie.

    Ordre d'exécution :
    1. Vérification connexion DB
    2. Extraction API → raw.products
    3. Transformation raw → staging.products
    4. Logging du run dans raw.pipeline_logs

    Args:
        category  : catégorie Open Food Facts ex. "beverages"
        max_pages : limite de pages API (None = valeur config)

    Returns:
        dict avec les statistiques du run :
        {
            "run_id": "run_20240115_143022",
            "status": "success",
            "rows_extracted": 500,
            "rows_loaded_raw": 500,
            "rows_loaded_staging": 487,
            "rows_rejected": 13,
        }

    Raises:
        Exception : si une étape critique échoue
    """

    # ── Initialisation du run ──────────────────────────────────
    # Le run_id identifie de façon unique cette exécution.
    # Format : run_YYYYMMDD_HHMMSS_categorie
    # Exemple : run_20240115_143022_beverages
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{category}"
    started_at = datetime.now()

    # Statistiques du run — mises à jour au fur et à mesure
    stats = {
        "run_id": run_id,
        "status": "running",
        "rows_extracted": 0,
        "rows_loaded_raw": 0,
        "rows_loaded_staging": 0,
        "rows_rejected": 0,
    }

    logger.info(f"{'='*50}")
    logger.info(f"DÉMARRAGE run : {run_id}")
    logger.info(f"Catégorie     : {category}")
    logger.info(f"{'='*50}")

    # ── Étape 0 : vérification connexion ──────────────────────
    # On vérifie AVANT de faire quoi que ce soit.
    # Si la DB est inaccessible, inutile d'appeler l'API.
    logger.info("[0/4] Vérification connexion PostgreSQL...")

    if not test_connection():
        # Erreur critique — on ne peut pas continuer
        raise ConnectionError(
            "Impossible de se connecter à PostgreSQL. "
            "Vérifie que le conteneur Docker est bien démarré : "
            "docker compose ps"
        )

    logger.info("[0/4] Connexion OK")

    try:
        # ── Étape 1 : Extraction API ───────────────────────────
        # On appelle l'API et on récupère les données brutes.
        # Rien n'est transformé ici.
        logger.info(f"[1/4] Extraction API — catégorie '{category}'...")

        products_raw = fetch_all_pages(
            category=category,
            max_pages=max_pages,
        )

        stats["rows_extracted"] = len(products_raw)
        logger.info(
            f"[1/4] Extraction OK — {len(products_raw)} produits récupérés"
        )

        if not products_raw:
            logger.warning(
                f"Aucun produit récupéré pour '{category}'. "
                f"Pipeline terminé sans chargement."
            )
            stats["status"] = "success"
            return stats

        # ── Étape 2 : Chargement RAW ───────────────────────────
        # On transforme la liste de dicts en DataFrame pandas,
        # on ajoute les colonnes techniques, et on charge en base.
        logger.info("[2/4] Chargement dans raw.products...")

        df_raw = pd.DataFrame(products_raw)

        # Renomme 'code' en 'barcode' — l'API appelle ça 'code'
        # mais notre table s'attend à 'barcode'
        if "code" in df_raw.columns:
            df_raw = df_raw.rename(columns={"code": "barcode"})

        # Sérialise le champ nutriments (dict) en JSON string
        # PostgreSQL stocke TEXT — pas un dict Python
        if "nutriments" in df_raw.columns:
            import json
            df_raw["nutriments_json"] = df_raw["nutriments"].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else None
            )
            df_raw = df_raw.drop(columns=["nutriments"])

        # Ajoute les colonnes techniques
        df_raw["pipeline_run_id"] = run_id
        df_raw["source"] = "openfoodfacts"

        # Garde uniquement les colonnes qui existent dans raw.products
        colonnes_raw = [
            "barcode", "product_name", "brands", "countries",
            "packaging", "nutriscore_grade", "ecoscore_grade",
            "categories", "ingredients_text", "nutriments_json",
            "pipeline_run_id", "source",
        ]
        # Garde seulement les colonnes qui existent dans le DataFrame
        # (certaines peuvent être absentes si l'API ne les renvoie pas)
        colonnes_presentes = [c for c in colonnes_raw if c in df_raw.columns]
        df_raw = df_raw[colonnes_presentes]

        nb_raw = load_dataframe(df_raw, "products", "raw")
        stats["rows_loaded_raw"] = nb_raw
        logger.info(f"[2/4] RAW OK — {nb_raw} lignes chargées")

        # ── Étape 3 : Transformation + chargement STAGING ─────
        # On relit depuis raw.products pour ce run_id,
        # on transforme, et on charge dans staging.products.
        #
        # Pourquoi relire depuis la base et pas utiliser df_raw :
        # - La base est la source de vérité
        # - On simule exactement ce que ferait Airflow en prod
        # - Si raw.products a des contraintes ou triggers,
        #   ils ont déjà été appliqués
        logger.info("[3/4] Transformation raw → staging...")

        engine = get_engine()
        with engine.connect() as conn:
            df_raw_from_db = pd.read_sql(
                text(
                    "SELECT * FROM raw.products "
                    "WHERE pipeline_run_id = :run_id"
                ),
                conn,
                params={"run_id": run_id},
            )

        logger.info(
            f"[3/4] {len(df_raw_from_db)} lignes lues depuis raw "
            f"pour ce run"
        )

        df_staging = transform_raw_to_staging(df_raw_from_db)

        rows_rejected = len(df_raw_from_db) - len(df_staging)
        stats["rows_rejected"] = rows_rejected

        nb_staging = load_dataframe(df_staging, "products", "staging")
        stats["rows_loaded_staging"] = nb_staging
        logger.info(f"[3/4] STAGING OK — {nb_staging} lignes chargées")

        # ── Étape 4 : Log du run ───────────────────────────────
        stats["status"] = "success"
        logger.info(f"[4/4] Logging du run...")

        log_pipeline_run(
            run_id=run_id,
            pipeline_name=f"materialflow_{category}",
            started_at=started_at,
            ended_at=datetime.now(),
            status="success",
            rows_extracted=stats["rows_extracted"],
            rows_loaded=nb_staging,
            rows_rejected=rows_rejected,
            run_params=f"category={category}, max_pages={max_pages}",
        )

        logger.info(f"{'='*50}")
        logger.info(f"RUN TERMINÉ — {run_id}")
        logger.info(f"Extraits  : {stats['rows_extracted']}")
        logger.info(f"Raw       : {stats['rows_loaded_raw']}")
        logger.info(f"Staging   : {stats['rows_loaded_staging']}")
        logger.info(f"Rejetés   : {stats['rows_rejected']}")
        logger.info(f"{'='*50}")

        return stats

    except Exception as e:
        # ── Gestion d'erreur globale ───────────────────────────
        # Quelle que soit l'erreur, on la logue en base AVANT
        # de la propager. Comme ça, même si le pipeline plante,
        # raw.pipeline_logs contient une trace de l'échec.
        stats["status"] = "failed"

        logger.error(f"PIPELINE ÉCHOUÉ : {e}")

        log_pipeline_run(
            run_id=run_id,
            pipeline_name=f"materialflow_{category}",
            started_at=started_at,
            ended_at=datetime.now(),
            status="failed",
            rows_extracted=stats["rows_extracted"],
            rows_loaded=stats["rows_loaded_staging"],
            rows_rejected=stats["rows_rejected"],
            error_message=str(e),
            run_params=f"category={category}, max_pages={max_pages}",
        )

        # On propage l'exception pour qu'Airflow la détecte
        # et marque la tâche comme failed
        raise
    