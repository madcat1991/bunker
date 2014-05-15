DROP TABLE IF EXISTS catalogue_table;
CREATE TABLE catalogue_table AS
WITH content_posters_temp AS (
    SELECT p.content_id, array_agg(s.host || '/contents/' || f.path) AS posters
    FROM posters p
        JOIN files f ON f.id = ANY(p.files)
        JOIN servers s ON s.id = f.server_id
    GROUP BY p.content_id
),
compilation_posters_temp AS (
    SELECT cp.compilation_id, array_agg(s.host || '/contents/' || f.path) AS posters
    FROM compilation_poster cp
        JOIN files f ON f.id = ANY(cp.files)
        JOIN servers s ON s.id = f.server_id
    GROUP BY cp.compilation_id
)
SELECT
    hcv.id * 10 + 0 AS qid,
    c.title,
    mcv.content_paid_types,
    mcv.is_active,
    mcv.subsite_ids,
    cavp.versions_with_allowed_formats,
    NULL AS hru,
    COALESCE(cpt.posters, copt.posters, '{}') AS posters
FROM hydra_content_view hcv
    JOIN content c ON c.id = hcv.id
    LEFT JOIN content_app_version_mapi cavp ON hcv.id = cavp.content_id
    LEFT JOIN mobile_content_view mcv ON hcv.id = mcv.id
    --поиск постеров у контента
    LEFT JOIN content_posters_temp cpt ON cpt.content_id = hcv.id
    --поиск постеров у сборников
    LEFT JOIN compilation_posters_temp copt ON copt.compilation_id = hcv.compilation_id
WHERE hcv.compilation_id IS NULL


UNION ALL


SELECT
    hcv.compilation_id * 10 + 1 AS qid,
    c.title,
    mcv.content_paid_types,
    mcv.is_active,
    mcv.subsite_ids,
    cavp.versions_with_allowed_formats,
    c.hru,
    cpt.posters
FROM hydra_content_view hcv
    JOIN compilation c ON hcv.compilation_id = c.id
    LEFT JOIN compilation_app_version_mapi cavp ON hcv.compilation_id = cavp.compilation_id
    LEFT JOIN mobile_compilation_view mcv ON hcv.compilation_id = mcv.id
    LEFT JOIN compilation_posters_temp cpt ON cpt.compilation_id = hcv.compilation_id
WHERE hcv.compilation_id IS NOT NULL
GROUP BY
    hcv.compilation_id,
    c.id,
    mcv.content_paid_types,
    mcv.is_active,
    mcv.subsite_ids,
    cavp.versions_with_allowed_formats,
    cpt.posters
;
