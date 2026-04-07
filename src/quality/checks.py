# src/quality/checks.py
#
# Checks de qualité de données pour MaterialFlow.
#
# Chaque check :
# - reçoit un DataFrame ou une connexion DB
# - retourne un dict {"check": nom, "passed": bool, "message": str}
# - ne plante jamais — une exception dans un check est catchée
#   et retournée comme un check failed
#
# Pourquoi cette interface standardisée :
# Le pipeline appelle run_all_checks() et reçoit une liste de résultats.
# Il peut décider quoi faire (warning, erreur, continuer) sans connaître
# le détail de chaque check.

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text

from src.loading.postgres import get_engine

logger = logging.getLogger(__name__)


def _make_result(
    check_name: str,
    passed: bool,
    message: str,
    value=None,
) -> dict:
    """
    Construit un résultat de check standardisé.

    Pourquoi une fonction helper et pas un dict inline :
    Si on change la structure du résultat (ajouter un champ "severity"),
    on le change ici une seule fois — pas dans chaque check.

    Args:
        check_name : nom du check ex. "completude_barcode"
        passed     : True = check OK, False = check échoué
        message    : explication lisible par un humain
        value      : valeur mesurée (pour le logging et le rapport)
    """
    status = "PASS" if passed else "FAIL"
    logger.info(f"[{status}] {check_name} — {message}")
    return {
        "check": check_name,
        "passed": passed,
        "message": message,
        "value": value,
    }


# ── CHECKS DE COMPLÉTUDE ──────────────────────────────────────
# Vérifient que les colonnes obligatoires sont remplies.

def check_completude_barcode(df: pd.DataFrame) -> dict:
    """
    Vérifie qu'aucun barcode n'est NULL dans le DataFrame.

    Pourquoi c'est critique :
    Le barcode est la clé d'identification d'un produit.
    Sans barcode, on ne peut pas faire de jointure, pas de
    dédoublonnage, pas de mise à jour incrémentale.
    """
    null_count = df["barcode"].isna().sum()
    total = len(df)

    # On accepte 0 NULL absolu — c'est une colonne critique
    passed = null_count == 0

    return _make_result(
        check_name="completude_barcode",
        passed=passed,
        message=(
            f"{null_count}/{total} barcodes NULL"
            if not passed
            else f"Tous les {total} barcodes sont renseignés"
        ),
        value=null_count,
    )


def check_completude_product_name(df: pd.DataFrame) -> dict:
    """
    Vérifie le taux de noms de produits renseignés.

    Pourquoi un seuil à 80% et pas 100% :
    Open Food Facts est une base collaborative — certains produits
    sont ajoutés sans nom complet. On tolère jusqu'à 20% d'absence,
    au-delà c'est suspect (problème API ou transformation).
    """
    total = len(df)
    if total == 0:
        return _make_result(
            "completude_product_name", False,
            "DataFrame vide", 0
        )

    null_count = df["product_name"].isna().sum()
    taux_rempli = (total - null_count) / total * 100

    # Seuil : au moins 80% des noms doivent être renseignés
    seuil = 80.0
    passed = taux_rempli >= seuil

    return _make_result(
        check_name="completude_product_name",
        passed=passed,
        message=(
            f"Taux noms renseignés : {taux_rempli:.1f}% "
            f"(seuil : {seuil}%)"
        ),
        value=round(taux_rempli, 1),
    )


# ── CHECKS DE VALIDITÉ ────────────────────────────────────────
# Vérifient que les valeurs sont dans les bornes attendues.

def check_validite_nutriscore(df: pd.DataFrame) -> dict:
    """
    Vérifie que les valeurs de nutriscore_grade sont valides.

    Valeurs valides : a, b, c, d, e, ou NULL (non renseigné).
    Toute autre valeur = problème dans la transformation.

    Pourquoi NULL est accepté :
    Tous les produits n'ont pas de Nutri-Score calculé.
    NULL = "on ne sait pas", ce n'est pas une erreur.
    "not-applicable" ou "unknown" EN BASE = erreur de transformation.
    """
    if "nutriscore_grade" not in df.columns:
        return _make_result(
            "validite_nutriscore", False,
            "Colonne nutriscore_grade absente", None
        )

    valeurs_valides = {"a", "b", "c", "d", "e"}

    # On ne vérifie que les valeurs non-NULL
    non_null = df["nutriscore_grade"].dropna()
    invalides = non_null[~non_null.isin(valeurs_valides)]

    passed = len(invalides) == 0

    return _make_result(
        check_name="validite_nutriscore",
        passed=passed,
        message=(
            f"{len(invalides)} valeurs invalides : "
            f"{invalides.unique().tolist()}"
            if not passed
            else "Toutes les valeurs Nutri-Score sont valides"
        ),
        value=len(invalides),
    )


def check_validite_energy(df: pd.DataFrame) -> dict:
    """
    Vérifie que les valeurs d'énergie sont dans des bornes physiques.

    Bornes : 0 kcal (eau pure) à 9000 kcal/100g (huile pure ~ 900,
    on prend une marge large de 9000 pour les erreurs de saisie).

    Pourquoi vérifier les bornes physiques :
    Une valeur de 99999 kcal/100g est une erreur de saisie humaine
    sur Open Food Facts. Sans ce check, elle fausserait toutes les
    moyennes nutritionnelles.
    """
    if "energy_kcal" not in df.columns:
        return _make_result(
            "validite_energy", False,
            "Colonne energy_kcal absente", None
        )

    non_null = df["energy_kcal"].dropna()

    if len(non_null) == 0:
        return _make_result(
            "validite_energy", True,
            "Aucune valeur energy_kcal à vérifier", 0
        )

    hors_bornes = non_null[(non_null < 0) | (non_null > 9000)]
    passed = len(hors_bornes) == 0

    return _make_result(
        check_name="validite_energy",
        passed=passed,
        message=(
            f"{len(hors_bornes)} valeurs hors bornes [0-9000] : "
            f"min={non_null.min():.0f}, max={non_null.max():.0f}"
            if not passed
            else f"Valeurs energy OK — min={non_null.min():.0f}, "
                 f"max={non_null.max():.0f} kcal/100g"
        ),
        value=len(hors_bornes),
    )


# ── CHECKS DE VOLUME ──────────────────────────────────────────
# Vérifient que le nombre de lignes est cohérent.

def check_volume_staging(
    run_id: str,
    min_rows: int = 10,
) -> dict:
    """
    Vérifie que le run a chargé suffisamment de lignes dans staging.

    Pourquoi ce check est important :
    Un pipeline peut terminer avec status "success" même si staging
    contient 0 lignes — si toutes les lignes ont été rejetées par
    la transformation. Ce check détecte ce cas silencieux.

    Args:
        run_id   : identifiant du run à vérifier
        min_rows : seuil minimum de lignes attendues
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT COUNT(*) FROM staging.products "
                    "WHERE pipeline_run_id = :run_id"
                ),
                {"run_id": run_id},
            )
            count = result.scalar()

        passed = count >= min_rows

        return _make_result(
            check_name="volume_staging",
            passed=passed,
            message=(
                f"{count} lignes dans staging pour ce run "
                f"(minimum attendu : {min_rows})"
            ),
            value=count,
        )

    except Exception as e:
        return _make_result(
            "volume_staging", False,
            f"Erreur lors du check : {e}", None
        )


# ── CHECKS MÉTIER ─────────────────────────────────────────────
# Vérifient que les ratios métier sont cohérents.

def check_taux_nutriscore(
    df: pd.DataFrame,
    min_taux: float = 20.0,
) -> dict:
    """
    Vérifie que le taux de produits avec Nutri-Score est suffisant.

    Pourquoi 20% minimum :
    Open Food Facts ne calcule le Nutri-Score que pour certaines
    catégories. Sur des boissons (beverages), on s'attend à avoir
    au moins 20% des produits avec un score calculé.
    Si on tombe en dessous, soit l'API a changé, soit la catégorie
    choisie n'a pas de Nutri-Score (ex: eau minérale).
    """
    total = len(df)
    if total == 0:
        return _make_result(
            "taux_nutriscore", False, "DataFrame vide", 0
        )

    avec_nutriscore = df["nutriscore_grade"].notna().sum()
    taux = avec_nutriscore / total * 100

    passed = taux >= min_taux

    return _make_result(
        check_name="taux_nutriscore",
        passed=passed,
        message=(
            f"Taux Nutri-Score renseigné : {taux:.1f}% "
            f"(seuil : {min_taux}%)"
        ),
        value=round(taux, 1),
    )


def check_taux_packaging(
    df: pd.DataFrame,
    min_taux: float = 10.0,
) -> dict:
    """
    Vérifie que le taux de produits avec emballage détecté est suffisant.

    Un produit a un emballage détecté si au moins un des booléens
    has_plastic, has_cardboard, has_glass, has_metal est True.

    Pourquoi 10% minimum :
    Le champ packaging est souvent mal renseigné sur Open Food Facts.
    10% est un seuil bas mais réaliste.
    Si on tombe en dessous, la détection d'emballage ne fonctionne plus.
    """
    total = len(df)
    if total == 0:
        return _make_result(
            "taux_packaging", False, "DataFrame vide", 0
        )

    colonnes_packaging = [
        "has_plastic", "has_cardboard", "has_glass", "has_metal"
    ]

    # Vérifie que les colonnes existent
    colonnes_presentes = [
        c for c in colonnes_packaging if c in df.columns
    ]
    if not colonnes_presentes:
        return _make_result(
            "taux_packaging", False,
            "Colonnes has_* absentes du DataFrame", 0
        )

    # Un produit a un emballage si au moins un booléen est True
    avec_packaging = df[colonnes_presentes].any(axis=1).sum()
    taux = avec_packaging / total * 100

    passed = taux >= min_taux

    return _make_result(
        check_name="taux_packaging",
        passed=passed,
        message=(
            f"Taux produits avec emballage détecté : {taux:.1f}% "
            f"(seuil : {min_taux}%)"
        ),
        value=round(taux, 1),
    )


# ── ORCHESTRATEUR ─────────────────────────────────────────────

def run_all_checks(
    df_staging: pd.DataFrame,
    run_id: str,
) -> dict:
    """
    Lance tous les checks et retourne un rapport complet.

    Args:
        df_staging : DataFrame de staging.products du run
        run_id     : identifiant du run

    Returns:
        {
            "run_id": str,
            "total_checks": int,
            "passed": int,
            "failed": int,
            "results": [liste des résultats individuels],
            "all_passed": bool,
        }

    Pourquoi retourner all_passed et pas lever une exception :
    Le pipeline décide quoi faire du résultat.
    Certains checks échoués = warning (on continue).
    D'autres = bloquant (on arrête).
    Cette décision appartient au pipeline, pas au check.
    """
    logger.info(f"Lancement des checks qualité pour run : {run_id}")

    results = []

    # Lance chaque check — l'ordre n'a pas d'importance
    checks_dataframe = [
        check_completude_barcode,
        check_completude_product_name,
        check_validite_nutriscore,
        check_validite_energy,
        check_taux_nutriscore,
        check_taux_packaging,
    ]

    for check_fn in checks_dataframe:
        try:
            result = check_fn(df_staging)
            results.append(result)
        except Exception as e:
            # Un check qui plante ne doit pas bloquer les autres
            results.append(_make_result(
                check_fn.__name__, False,
                f"Exception inattendue : {e}", None
            ))

    # Check de volume — lit directement en base
    try:
        results.append(check_volume_staging(run_id))
    except Exception as e:
        results.append(_make_result(
            "volume_staging", False,
            f"Exception inattendue : {e}", None
        ))

    # Calcul du résumé
    passed_count = sum(1 for r in results if r["passed"])
    failed_count = len(results) - passed_count

    rapport = {
        "run_id": run_id,
        "total_checks": len(results),
        "passed": passed_count,
        "failed": failed_count,
        "all_passed": failed_count == 0,
        "results": results,
    }

    logger.info(
        f"Checks terminés — "
        f"{passed_count}/{len(results)} réussis"
    )

    return rapport