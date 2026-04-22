# dags/materialflow_dag.py
#
# DAG Airflow pour le pipeline MaterialFlow.
#
# Ce fichier est lu par le scheduler Airflow toutes les 30 secondes.
# Il ne doit PAS contenir de code qui s'exécute au chargement
# (pas de connexions DB, pas d'appels API au niveau module).
# Seulement des définitions de fonctions et de DAG.
#
# Pourquoi cette règle :
# Airflow importe tous les fichiers dags/ en permanence pour détecter
# les changements. Si tu fais une connexion DB au niveau module,
# elle est ouverte des centaines de fois par heure.

import logging
import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow exécute les DAGs dans son propre environnement.
# Le dossier src/ est monté dans /opt/airflow/src (voir docker-compose.yml).
# On ajoute ce chemin pour que Python trouve nos modules.
sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)


# ── Fonctions des tasks ────────────────────────────────────────
# Chaque fonction correspond à une task du DAG.
# Elles reçoivent **context — le contexte Airflow qui contient
# la date d'exécution, l'identifiant du run, etc.

def task_check_connexion(**context):
    """
    Task 1 : vérifie que PostgreSQL est accessible.

    Si cette task échoue, toutes les tasks suivantes
    sont annulées automatiquement par Airflow.

    **context contient entre autres :
    - context["ds"] : date d'exécution au format YYYY-MM-DD
    - context["ti"] : TaskInstance, pour XCom
    - context["run_id"] : identifiant du DAG run
    """
    from src.loading.postgres import test_connection

    ds = context["ds"]
    logger.info(f"Vérification connexion DB pour le run du {ds}")

    if not test_connection():
        raise ConnectionError(
            "PostgreSQL inaccessible — "
            "vérifie que le conteneur Docker est démarré"
        )

    logger.info("Connexion PostgreSQL OK")


def _make_task_run_category(category: str):
    """
    Fabrique une fonction de task pour une catégorie donnée.

    Pourquoi une factory function et pas une fonction par catégorie :
    Si on a 10 catégories, on ne va pas écrire 10 fonctions identiques.
    Cette fonction "fabrique" une fonction de task configurée pour
    une catégorie spécifique.

    Args:
        category : catégorie Open Food Facts ex. "beverages"

    Returns:
        fonction de task Airflow
    """
    def task_run_pipeline(**context):
        from src.pipeline import run_pipeline

        ds = context["ds"]
        logger.info(
            f"Lancement pipeline — catégorie='{category}' date='{ds}'"
        )

        # max_pages=2 en prod pour avoir suffisamment de données
        # sans surcharger l'API
        stats = run_pipeline(
            category=category,
            max_pages=2,
        )

        # Pousse les stats dans XCom pour que rapport_final
        # puisse les récupérer
        context["ti"].xcom_push(
            key=f"stats_{category}",
            value=stats,
        )

        logger.info(
            f"Pipeline '{category}' terminé — "
            f"status={stats['status']}, "
            f"staging={stats['rows_loaded_staging']}"
        )

        # Si le statut est "partial" (checks qualité échoués),
        # on lève une exception pour qu'Airflow marque la task
        # en warning (pas en failed — les données sont quand même là)
        if stats["status"] == "partial":
            raise ValueError(
                f"Data quality checks échoués pour '{category}'. "
                f"Les données sont en base mais suspectes. "
                f"Consulte raw.pipeline_logs pour le détail."
            )

        return stats

    # Renomme la fonction pour qu'Airflow affiche le bon nom
    # dans l'interface web
    task_run_pipeline.__name__ = f"run_{category}"
    return task_run_pipeline


def task_rapport_final(**context):
    """
    Task finale : agrège les résultats de toutes les catégories
    et loggue un rapport global.

    Récupère les stats de chaque catégorie via XCom.
    """
    categories = ["beverages", "dairy", "snacks"]
    ti = context["ti"]

    total_extraits = 0
    total_staging = 0
    total_rejetes = 0
    statuts = []

    logger.info("=== RAPPORT GLOBAL DU RUN ===")

    for category in categories:
        stats = ti.xcom_pull(
            task_ids=f"run_{category}",
            key=f"stats_{category}",
        )

        if stats:
            total_extraits += stats.get("rows_extracted", 0)
            total_staging += stats.get("rows_loaded_staging", 0)
            total_rejetes += stats.get("rows_rejected", 0)
            statuts.append(stats.get("status", "unknown"))

            logger.info(
                f"  {category:<15} : "
                f"extraits={stats['rows_extracted']}, "
                f"staging={stats['rows_loaded_staging']}, "
                f"status={stats['status']}"
            )
        else:
            logger.warning(f"  {category:<15} : pas de stats disponibles")

    logger.info(f"  {'─'*45}")
    logger.info(f"  TOTAL : extraits={total_extraits}, staging={total_staging}, rejetés={total_rejetes}")

    # Si au moins une catégorie est en partial ou failed,
    # le rapport final le signale
    if "failed" in statuts:
        raise ValueError(
            f"Au moins une catégorie a échoué : {statuts}. "
            f"Consulte les logs pour le détail."
        )


# ── Définition du DAG ──────────────────────────────────────────

# Arguments par défaut appliqués à toutes les tasks
# sauf si elles les surchargent individuellement
default_args = {
    "owner": "data-team",

    # Si un run précédent a échoué, ne pas bloquer le run suivant.
    # En prod on met True pour forcer la résolution des erreurs.
    "depends_on_past": False,

    # Nombre de tentatives en cas d'échec
    # 2 retries = 3 tentatives au total
    "retries": 2,

    # Délai entre les tentatives
    # 5 minutes laisse le temps à l'API de se remettre
    "retry_delay": timedelta(minutes=5),

    # Ne pas envoyer d'email (on n'a pas configuré de serveur SMTP)
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    # Identifiant unique du DAG dans Airflow
    dag_id="materialflow_daily",

    default_args=default_args,

    description="Pipeline ELT quotidien — Open Food Facts matériaux d'emballage",

    # Tous les jours à 6h du matin
    # En développement, on peut mettre None pour déclencher manuellement
    schedule="0 6 * * *",

    # Date à partir de laquelle Airflow commence à planifier
    start_date=datetime(2026, 1, 1),

    # catchup=False = ne pas rejouer les runs manqués passés.
    # Si le pipeline est arrêté 7 jours, Airflow ne lance pas
    # 7 runs en rattrapage au redémarrage.
    # En prod c'est souvent True pour les pipelines financiers
    # où chaque jour compte, False pour les pipelines de monitoring.
    catchup=False,

    # Tags visibles dans l'interface Airflow pour filtrer les DAGs
    tags=["materialflow", "elt", "daily", "openfoodfacts"],

) as dag:

    # ── Définition des tasks ───────────────────────────────────

    # Task 0 : vérification connexion
    check_connexion = PythonOperator(
        task_id="check_connexion_db",
        python_callable=task_check_connexion,
    )

    # Tasks de pipeline par catégorie
    # On utilise la factory function pour créer une task par catégorie
    run_beverages = PythonOperator(
        task_id="run_beverages",
        python_callable=_make_task_run_category("beverages"),
    )

    run_dairy = PythonOperator(
        task_id="run_dairy",
        python_callable=_make_task_run_category("dairy"),
    )

    run_snacks = PythonOperator(
        task_id="run_snacks",
        python_callable=_make_task_run_category("snacks"),
    )

    # Task finale : rapport global
    rapport = PythonOperator(
        task_id="rapport_final",
        python_callable=task_rapport_final,
    )

    # ── Dépendances entre tasks ────────────────────────────────
    # La syntaxe >> signifie "doit précéder"
    # A >> B = A doit finir avant que B commence

    # check_connexion doit réussir avant tout le reste
    check_connexion >> [run_beverages, run_dairy]

    # beverages et dairy tournent en parallèle
    # snacks attend que les deux finissent
    [run_beverages, run_dairy] >> run_snacks

    # rapport_final attend que snacks finisse
    run_snacks >> rapport