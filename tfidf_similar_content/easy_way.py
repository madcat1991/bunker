#coding: utf-8

"""
Упрощенный вариант
"""
import psycopg2
from finder import prepare_attrs

sql = """
    WITH gcc AS (
        SELECT
            m2m.content_id content_id,
            ARRAY_AGG(DISTINCT 'g' || g.id) genres,
            ARRAY_AGG(DISTINCT 'c' || g.category_id) categories
        FROM m2m_content_content_genre m2m
            JOIN content_genre g ON m2m.content_genre_id = g.id
        GROUP BY content_id
    ),
    pc AS (
        SELECT
            cp.content_id content_id,
            ARRAY_AGG(DISTINCT 'p' || cp.person_id) persons
        FROM content_person cp
        GROUP BY content_id
    ),
    ac AS (
        SELECT
            ca.content_id content_id,
            ARRAY_AGG(DISTINCT 'a' || ca.award_id) awards
        FROM content_award ca
        GROUP BY content_id
    ),
    cpc AS (
        SELECT
            cp.content_id content_id,
            ARRAY_AGG(DISTINCT 'p' || cp.property_id || '-' || cp.property_value_id) properties
        FROM content_property cp
        GROUP BY content_id
    ),
    tc AS (
        SELECT m2m.content_id content_id, ARRAY_AGG(DISTINCT 't' || m2m.tags_id) tags
        FROM m2m_content_tags m2m
        GROUP BY content_id
    )
    SELECT
        c.id,
        c.title,
        gcc.genres, gcc.categories, pc.persons, ac.awards, cpc.properties, tc.tags
    FROM content c
        LEFT JOIN gcc ON gcc.content_id = c.id
        LEFT JOIN pc ON pc.content_id = c.id
        LEFT JOIN ac ON ac.content_id = c.id
        LEFT JOIN cpc ON cpc.content_id = c.id
        LEFT JOIN tc ON tc.content_id = c.id
    WHERE c.compilation_id IS NULL
"""


class Content(object):
    def __init__(self, content_id, title):
        self.id = content_id
        self.title = title
        self.attrs = set()

    def set_attr(self, attrs):
        self.attrs.update(attrs)

    def __str__(self):
        return "%s. %s" % (self.id, self.title)


def collect_data():
    data = {}
    conn = psycopg2.connect(database="da_prod", user="da", host="localhost", port="5432")
    cursor = conn.cursor()
    cursor.execute(sql)
    for c_id, c_title, genres, categories, persons, awards, properties, tags in cursor:
        attrs = prepare_attrs(genres, categories, persons, awards, properties, tags)
        if attrs:
            content = Content(c_id, c_title)
            content.set_attr(attrs)
            data[c_id] = content
    conn.close()
    return data


def get_similarity(content1, content2):
    up = len(content1.attrs.intersection(content2.attrs))
    if up > 0:
        down = len(content1.attrs.union(content2.attrs))
        return float(up) / down
    return 0


def find_similar_content(data, similar_to=64252, top=20):
    result = {}
    main_content = data.pop(similar_to)
    for content_id, other_content in data.iteritems():
        result[other_content] = get_similarity(main_content, other_content)

    result = sorted(result.iteritems(), key=lambda x: x[1], reverse=True)
    for k, v in result[:top]:
        print k, "-", v

if __name__ == "__main__":
    data = collect_data()
    print "DATA COLLECTED"
    find_similar_content(data, similar_to=64238)
