-- sql/03_create_analytical.sql
-- Vues analytiques pour Metabase — pas besoin de tables, les vues suffisent
-- Une vue est une requête nommée, recalculée à chaque appel

CREATE OR REPLACE VIEW analytics.packaging_distribution AS
-- Distribution des types d'emballage
SELECT
    COUNT(*) FILTER (WHERE has_plastic)   AS products_with_plastic,
    COUNT(*) FILTER (WHERE has_cardboard) AS products_with_cardboard,
    COUNT(*) FILTER (WHERE has_glass)     AS products_with_glass,
    COUNT(*) FILTER (WHERE has_metal)     AS products_with_metal,
    COUNT(*) AS total_products
FROM staging.products;

CREATE OR REPLACE VIEW analytics.nutriscore_by_category AS
-- Distribution Nutri-Score par catégorie
SELECT
    main_category,
    nutriscore_grade,
    COUNT(*) AS product_count,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY main_category),
        1
    ) AS pct_in_category
FROM staging.products
WHERE nutriscore_grade IS NOT NULL
  AND main_category IS NOT NULL
GROUP BY main_category, nutriscore_grade
ORDER BY main_category, nutriscore_grade;

CREATE OR REPLACE VIEW analytics.pipeline_health AS
-- Vue de santé du pipeline pour le monitoring
SELECT
    DATE(started_at) AS run_date,
    COUNT(*) AS total_runs,
    COUNT(*) FILTER (WHERE status = 'success') AS successful_runs,
    COUNT(*) FILTER (WHERE status = 'failed')  AS failed_runs,
    AVG(rows_loaded) AS avg_rows_loaded
FROM raw.pipeline_logs
GROUP BY DATE(started_at)
ORDER BY run_date DESC;
