--
-- Таблица с атрибутами записей каталога
--
DROP TABLE catalogue_vector;
CREATE TABLE catalogue_vector AS
WITH top_persons AS (
    -- ищем для каждого фильма|сборника
    -- двух режиссеров, двух композиторов, 15 ведущих актеров (в порядке Кинопоиска)
    -- (если выдать всех личностей, загнемся)
    SELECT
        catalogue_id, array_agg(id) AS persons
    FROM (
        SELECT
            catalogue_id,
            person.id,
            person.name,
            person_type.title,
            row_number() OVER (PARTITION BY catalogue_id, person_type_id ORDER BY m2m.id) AS priority,
            CASE WHEN person_type.title IN ('Режиссер', 'Композитор') THEN 7.0
            ELSE 1.0 END AS person_type_weight
        FROM (
            SELECT id, content_id * 10 + 0 AS catalogue_id,
            person_id,
            person_type_id
            FROM content_person
            UNION ALL
            SELECT id, compilation_id * 10 + 1 AS catalogue_id,
            person_id,
            person_type_id
            FROM compilation_person
        ) AS m2m
        JOIN person ON m2m.person_id = person.id
        JOIN person_type ON m2m.person_type_id = person_type.id
        WHERE person_type.title IN ('Актер', 'Исполнитель', 'Режиссер', 'Композитор')
    ) q
    WHERE person_type_weight * priority <= 15
    GROUP BY catalogue_id
)
SELECT
    compilation.id * 10 + 1 AS catalogue_id,
    compilation.orig_country,
    compilation.title,
    extract(year from MAX(content.release_date)) AS release_year,
    (intarray_union(
            array_agg(DISTINCT COALESCE(comp_prop.property_value_id, 0)),
            array_agg(DISTINCT COALESCE(cont_prop.property_value_id, 0))
    ) - 0) AS property_values,
    (array_agg(DISTINCT COALESCE(genre.category_id, 0)) - 0) AS categories,
    -- вместо id жанров берем хеш от названия, чтобы одинаково рекомендовать
    -- сборники и единичный контент
    (array_agg(DISTINCT COALESCE(hashtext(genre.title), 0)) - 0) AS genres,
    intarray_aggregate_union(top_persons.persons) AS persons
FROM compilation
LEFT JOIN content ON content.compilation_id = compilation.id
LEFT JOIN compilation_property AS comp_prop ON comp_prop.compilation_id = compilation.id
LEFT JOIN content_property AS cont_prop ON cont_prop.content_id = content.id
LEFT JOIN m2m_compilation_content_genre AS comp_genre ON comp_genre.compilation_id = compilation.id
LEFT JOIN content_genre AS genre ON genre.id = comp_genre.content_genre_id
LEFT JOIN top_persons ON top_persons.catalogue_id = compilation.id * 10 + 1
GROUP BY 1, 2, 3
UNION ALL
SELECT
    content.id * 10 + 0 AS catalogue_id,
    content.origin_country,
    content.title,
    extract(year from MAX(content.release_date)) AS release_year,
    (array_agg(DISTINCT COALESCE(cont_prop.property_value_id, 0)) - 0) AS property_values,
    (array_agg(DISTINCT COALESCE(genre.category_id, 0)) - 0) AS categories,
    (array_agg(DISTINCT COALESCE(hashtext(genre.title), 0)) - 0) AS genres,
    intarray_aggregate_union(top_persons.persons) AS persons
FROM content
LEFT JOIN content_property AS cont_prop ON cont_prop.content_id = content.id
LEFT JOIN m2m_content_content_genre AS cont_genre ON cont_genre.content_id = content.id
LEFT JOIN content_genre AS genre ON genre.id = cont_genre.content_genre_id
LEFT JOIN top_persons ON top_persons.catalogue_id = content.id * 10 + 0
WHERE compilation_id IS NULL
GROUP BY 1, 2, 3;
