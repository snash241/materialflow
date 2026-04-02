-- sql/02_create_staging.sql
-- La couche staging contient les données nettoyées et typées correctement

CREATE TABLE IF NOT EXISTS staging.products (
    id                  SERIAL PRIMARY KEY,

    -- On reprend le barcode mais maintenant on peut contraindre
    barcode             TEXT NOT NULL,

    product_name        TEXT NOT NULL,
    brand               TEXT,  -- nettoyé : on prend la première marque seulement
    country             TEXT,  -- nettoyé : on prend le premier pays

    -- Matériaux d'emballage nettoyés et normalisés
    -- Séparés en colonnes distinctes pour l'analyse
    has_plastic         BOOLEAN DEFAULT FALSE,
    has_cardboard       BOOLEAN DEFAULT FALSE,
    has_glass           BOOLEAN DEFAULT FALSE,
    has_metal           BOOLEAN DEFAULT FALSE,
    packaging_raw       TEXT,  -- valeur originale conservée pour audit

    -- Scores avec types corrects
    -- CHECK garantit que seules les valeurs valides entrent
    nutriscore_grade    CHAR(1) CHECK (nutriscore_grade IN ('a','b','c','d','e')),
    ecoscore_grade      TEXT,

    -- Valeurs nutritionnelles typées en NUMERIC (décimal précis)
    -- NUMERIC(7,2) = jusqu'à 7 chiffres dont 2 décimales
    energy_kcal         NUMERIC(7,2),
    fat_g               NUMERIC(7,2),
    saturated_fat_g     NUMERIC(7,2),
    carbohydrates_g     NUMERIC(7,2),
    sugars_g            NUMERIC(7,2),
    proteins_g          NUMERIC(7,2),
    salt_g              NUMERIC(7,2),

    -- Catégorie principale (première de la liste)
    main_category       TEXT,

    -- Traçabilité
    raw_product_id      INTEGER REFERENCES raw.products(id),
    processed_at        TIMESTAMP DEFAULT NOW(),
    pipeline_run_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_products_barcode
    ON staging.products(barcode);
CREATE INDEX IF NOT EXISTS idx_staging_products_nutriscore
    ON staging.products(nutriscore_grade);
CREATE INDEX IF NOT EXISTS idx_staging_products_packaging
    ON staging.products(has_plastic, has_cardboard, has_glass);
