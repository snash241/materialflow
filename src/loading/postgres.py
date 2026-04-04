# src/loading/postgres.py

import logging
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import config

logger = logging.getLogger(__name__)


def get_engine() -> Engine:
    """
    Crée et retourne un moteur SQLAlchemy.

    Le moteur gère un pool de connexions — il ne se connecte
    pas immédiatement, seulement quand tu exécutes une requête.

    pool_pre_ping=True : avant chaque requête, vérifie que
    la connexion est toujours active. Si le conteneur PostgreSQL
    a redémarré, SQLAlchemy se reconnecte automatiquement
    au lieu de planter avec "connection closed".
    """
    return create_engine(
        config.db.connection_string,
        pool_pre_ping=True,
    )


@contextmanager
def get_connection():
    """
    Context manager pour une connexion PostgreSQL.

    Utilisation :
        with get_connection() as conn:
            conn.execute(text("SELECT 1"))

    La connexion est fermée automatiquement à la sortie
    du bloc with — même si une exception est levée.
    """
    engine = get_engine()
    with engine.connect() as conn:
        yield conn


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    schema: str,
    if_exists: str = "append",
    chunk_size: int = 1000,
) -> int:
    """
    Charge un DataFrame dans une table PostgreSQL.

    Args:
        df         : données à charger
        table_name : nom de la table cible (sans schéma)
        schema     : schéma PostgreSQL ("raw", "staging")
        if_exists  : "append" ajoute, "replace" écrase
        chunk_size : nb de lignes par batch d'insertion

    Returns:
        nombre de lignes chargées

    Pourquoi chunk_size=1000 :
    Insérer 10 000 lignes en un seul INSERT peut dépasser
    les limites mémoire de PostgreSQL. Par lots de 1000,
    c'est stable même sur de gros volumes.
    """
    if df.empty:
        logger.warning(
            f"DataFrame vide — rien à charger dans "
            f"{schema}.{table_name}"
        )
        return 0

    nb_rows = len(df)
    logger.info(
        f"Chargement de {nb_rows} lignes dans "
        f"{schema}.{table_name}..."
    )

    try:
        engine = get_engine()

        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,       # ne pas écrire l'index pandas comme colonne
            method="multi",    # insère plusieurs lignes par requête
            chunksize=chunk_size,
        )

        logger.info(
            f"OK — {nb_rows} lignes chargées dans "
            f"{schema}.{table_name}"
        )
        return nb_rows

    except Exception as e:
        logger.error(
            f"Erreur chargement {schema}.{table_name} : {e}"
        )
        raise


def test_connection() -> bool:
    """
    Vérifie que la connexion à PostgreSQL fonctionne.

    Returns:
        True si la connexion est OK, False sinon.

    Utilisé au démarrage du pipeline pour détecter
    immédiatement un problème de connexion avant
    de lancer des traitements longs.
    """
    try:
        with get_connection() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        logger.info("Connexion PostgreSQL OK")
        return True

    except Exception as e:
        logger.error(f"Connexion PostgreSQL échouée : {e}")
        return False


def log_pipeline_run(
    run_id: str,
    pipeline_name: str,
    started_at,
    ended_at,
    status: str,
    rows_extracted: int = 0,
    rows_loaded: int = 0,
    rows_rejected: int = 0,
    error_message: str = None,
    run_params: str = None,
) -> None:
    """
    Enregistre le résultat d'un run dans raw.pipeline_logs.

    Cette fonction est appelée à la fin de chaque run —
    succès ou échec. C'est le journal de bord du pipeline.
    Sans ça, impossible de savoir ce qui s'est passé hier.
    """
    record = pd.DataFrame([{
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "started_at": started_at,
        "ended_at": ended_at,
        "status": status,
        "rows_extracted": rows_extracted,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "error_message": error_message,
        "run_params": run_params,
    }])

    try:
        load_dataframe(
            df=record,
            table_name="pipeline_logs",
            schema="raw",
        )
        logger.info(f"Run {run_id} loggué — status: {status}")

    except Exception as e:
        # On loggue l'erreur mais on ne propage pas —
        # une erreur de logging ne doit pas faire planter
        # le pipeline principal
        logger.error(f"Impossible de logger le run {run_id} : {e}")
    