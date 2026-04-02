-- sql/01_create_raw.sql
-- Ce fichier est exécuté automatiquement au premier démarrage de PostgreSQL
-- (grâce au volume ./sql:/docker-entrypoint-initdb.d dans docker-compose.yml)

-- Crée les schémas séparément pour isoler les couches de données
-- Un schéma = un namespace, comme un dossier dans la base
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Table raw.products : données brutes de l'API, on ne touche à RIEN
-- On stocke exactement ce que l'API renvoie, en TEXT pour tout
-- Pourquoi TEXT ? Parce qu'on ne sait pas encore si les données sont propres.
-- Un champ "nutriments" peut valoir "45.3" ou "N/A" ou "" ou NULL selon l'API.
-- Si on met NUMERIC directement, une valeur "N/A" ferait planter l'insertion.
CREATE TABLE IF NOT EXISTS raw.products (
    -- Clé primaire technique auto-incrémentée
    id                  SERIAL PRIMARY KEY,

    -- Identifiant unique du produit dans Open Food Facts
    -- TEXT car il peut contenir des lettres et des tirets
    barcode             TEXT,

    -- Nom du produit tel quel, sans nettoyage
    product_name        TEXT,

    -- Marque du produit
    brands              TEXT,

    -- Pays où le produit est vendu (liste séparée par virgules dans l'API)
    countries           TEXT,

    -- Matériaux d'emballage — c'est l'angle "matériaux" du projet
    -- Exemple : "Plastique, Carton, Verre"
    packaging           TEXT,

    -- Score nutritionnel Nutri-Score (A, B, C, D, E)
    nutriscore_grade    TEXT,

    -- Score environnemental Eco-Score
    ecoscore_grade      TEXT,

    -- Catégories du produit (liste de catégories hiérarchiques)
    categories          TEXT,

    -- Ingrédients en texte libre
    ingredients_text    TEXT,

    -- Valeurs nutritionnelles (JSON stocké en TEXT brut)
    -- On verra plus tard comment le parser en staging
    nutriments_json     TEXT,

    -- Métadonnées du pipeline — cruciales pour le debugging
    -- Quand cette ligne a été insérée dans la base
    ingested_at         TIMESTAMP DEFAULT NOW(),

    -- Identifiant du run pipeline qui a inséré cette ligne
    -- Permet de retracer quelle exécution a produit quoi
    pipeline_run_id     TEXT,

    -- Source de la donnée (toujours "openfoodfacts" ici, mais extensible)
    source              TEXT DEFAULT 'openfoodfacts'
);

-- Index sur barcode pour les jointures futures
-- Sans index, une recherche sur 100 000 produits = scan complet de la table
-- Avec index = recherche en millisecondes
CREATE INDEX IF NOT EXISTS idx_raw_products_barcode
    ON raw.products(barcode);

-- Index sur pipeline_run_id pour retrouver facilement un run spécifique
CREATE INDEX IF NOT EXISTS idx_raw_products_run_id
    ON raw.products(pipeline_run_id);

-- Table raw.pipeline_logs : journal de chaque exécution du pipeline
-- Un DE sénior loggue TOUT. Sans ça, impossible de débugger en production.
CREATE TABLE IF NOT EXISTS raw.pipeline_logs (
    id              SERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    pipeline_name   TEXT NOT NULL,
    started_at      TIMESTAMP NOT NULL,
    ended_at        TIMESTAMP,

    -- 'running', 'success', 'failed', 'partial'
    status          TEXT,

    rows_extracted  INTEGER DEFAULT 0,
    rows_loaded     INTEGER DEFAULT 0,
    rows_rejected   INTEGER DEFAULT 0,

    -- Message d'erreur si status = 'failed'
    error_message   TEXT,

    -- Paramètres du run (catégorie demandée, nb de pages, etc.)
    run_params      TEXT
);
