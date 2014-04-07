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
),
thumbs_by_content AS (
    SELECT ct.id, array_agg(ts.host || '/contents/' || tf.path) AS thumbs
    FROM content_thumb_pack ct
    JOIN files tf
        ON (tf.id = ANY(ct.files)) AND tf.format = 7
    JOIN servers ts ON ts.id = tf.server_id
    GROUP BY 1
)
SELECT
    mcv.id * 10 + 0 AS qid,
    mcv.title,
    mcv.description,
    mcv.duration,
    mcv.country,
    mcv.restrict,
    mcv.genres,
    mcv.main_genre,
    mcv.lang,
    mcv.categories,
    mcv.seasons,
    mcv.episodes,
    mcv.release_date,
    mcv.ivi_pseudo_release_date,
    mcv.date_insert,
    mcv.date_start,
    mcv.date_end,
    mcv.year_from,
    mcv.year_to,
    ARRAY[mcv.year_from]::int[] AS years,
    mcv.content_paid_types,
    mcv.kinopoisk_id,
    mcv.kinopoisk_rating,
    mcv.imdb_rating,
    mcv.world_box_office,
    mcv.usa_box_office,
    mcv.budget,
    NULL AS content_ids,
    mcv.is_active,
    mcv.subsite_ids,
    mcv.versions_with_allowed_formats,
    NULL AS hru,
    ARRAY(
        SELECT p.name
        FROM content_person cp
        JOIN person p
            ON (cp.person_id = p.id)
        WHERE (cp.content_id = mcv.id AND cp.person_type_id = 6)
        ORDER BY cp.id
    ) AS artists,
    COALESCE(cpt.posters, copt.posters, '{}') AS posters,
    thumbs_by_content.thumbs
FROM mobile_content_view mcv
    --поиск постеров у контента
    LEFT JOIN content_posters_temp cpt ON cpt.content_id = mcv.id
    --поиск постеров у сборников
    LEFT JOIN compilation_posters_temp copt ON copt.compilation_id = mcv.compilation_id
    LEFT JOIN thumbs_by_content ON thumbs_by_content.id = mcv.preview_id
WHERE mcv.compilation_id IS NULL


UNION ALL


SELECT
    mcv.id * 10 + 1 AS qid,
    mcv.title,
    mcv.description,
    NULL AS duration,
    mcv.country,
    mcv.restrict,
    mcv.genres,
    NULL AS main_genre,
    mcv.lang,
    mcv.categories,
    mcv.seasons,
    mcv.episodes,
    mcv.release_date,
    mcv.ivi_pseudo_release_date,
    mcv.date_start,
    mcv.date_end,
    mcv.date_insert,
    mcv.year_from,
    mcv.year_to,
    mcv.years,
    mcv.content_paid_types,
    mcv.kinopoisk_id,
    mcv.kinopoisk_rating,
    mcv.imdb_rating,
    mcv.world_box_office,
    mcv.usa_box_office,
    mcv.budget,
    mcv.content_ids,
    mcv.is_active,
    mcv.subsite_ids,
    mcv.versions_with_allowed_formats,
    mcv.hru,
    ARRAY(
        SELECT p.name
        FROM compilation_person cp
        JOIN person p
            ON (cp.person_id = p.id)
        WHERE (cp.compilation_id = mcv.id AND cp.person_type_id = 6)
        ORDER BY cp.id
    ) AS artists,
    compilation_posters_temp.posters,
    NULL AS thumbs
FROM mobile_compilation_view mcv
LEFT JOIN compilation_posters_temp
    ON compilation_posters_temp.compilation_id = mcv.id
;
