#coding: utf-8

"""
Скрипт для вычисления схожести контента,
путем сравнения его с отальными
"""
import psycopg2
from redis import Redis
from common import attr_key, content_key

docs_count = None


class Content(object):
    def __init__(self, content_id, title):
        self.id = content_id
        self.title = title
        self.terms = {}

    def set_terms(self, rd, attrs):
        """Вычисление tf-idf для всех атрибутов контента
        """
        try:
            terms_in_doc = float(rd.get(content_key(self.id)))
        except TypeError:
            print self.id, u"Нет в редисе"
        else:
            for attr in attrs:
                try:
                    tf = float(rd.hget(attr_key(attr), self.id)) / terms_in_doc
                    idf = float(rd.hlen(attr_key(attr))) / docs_count
                except ValueError:
                    pass
                else:
                    self.terms[attr] = tf / idf


def get_similarity(content1, content2):
    """Поиск схожести двух единиц контента
    """
    if not (docs_count > 0 and len(content1.terms) > 0 and len(content2.terms) > 0):
        return 0

    up = sum(content1.terms.get(attr, 0) * content2.terms.get(attr, 0) for attr in content1.terms)
    if up > 0:
        down = sum(content1.terms.get(attr, 0) * content1.terms.get(attr, 0) for attr in content1.terms) * \
            sum(content2.terms.get(attr, 0) * content2.terms.get(attr, 0) for attr in content2.terms)
        return up / down if down > 0.0 else 0.0
    return 0


def prepare_attrs(*args):
    """Преобразование свойств в атрибуты
    """
    result = []
    for item in args:
        result += item if item is not None else []
    return result


def find_similar_for_content(content_id, cursor, rd, top=20):
    """Поиск топ 20 единиц контента схожих с указанным content_id
    """
    global docs_count
    docs_count = len(redis.keys(content_key('*')))

    sql = """
        WITH gcc AS (
            SELECT m2m.content_id content_id, ARRAY_AGG(g.title) genres, ARRAY_AGG(c.title) categories
            FROM m2m_content_content_genre m2m
                JOIN content_genre g ON m2m.content_genre_id = g.id
                JOIN content_category c ON g.category_id = c.id
            GROUP BY content_id
        ),
        pc AS (
            SELECT cp.content_id content_id, ARRAY_AGG(p.name) persons
            FROM content_person cp
                JOIN person p ON cp.person_id = p.id
            GROUP BY content_id
        ),
        ac AS (
            SELECT ca.content_id content_id, ARRAY_AGG(a.title) awards
            FROM content_award ca
                JOIN award a ON ca.award_id = a.id
            GROUP BY content_id
        ),
        cpc AS (
            SELECT cp.content_id content_id, ARRAY_AGG(p.title || '-' || pv.title) properties
            FROM content_property cp
                JOIN property p ON cp.property_id = p.id
                JOIN property_value pv ON cp.property_value_id = pv.id
            GROUP BY content_id
        ),
        tc AS (
            SELECT m2m.content_id content_id, ARRAY_AGG(t.title) tags
            FROM m2m_content_tags m2m
                JOIN tags t ON m2m.tags_id = t.id
            GROUP BY content_id
        )
        SELECT c.id, c.title, gcc.genres, gcc.categories, pc.persons, ac.awards, cpc.properties, tc.tags
        FROM content c
            LEFT JOIN gcc ON gcc.content_id = c.id
            LEFT JOIN pc ON pc.content_id = c.id
            LEFT JOIN ac ON ac.content_id = c.id
            LEFT JOIN cpc ON cpc.content_id = c.id
            LEFT JOIN tc ON tc.content_id = c.id
        %s
    """

    main_content_sql = sql % "WHERE c.id = %s"
    cursor.execute(main_content_sql, (content_id,))

    main_content = None
    for c_id, title, genres, categories, persons, awards, properties, tags in cursor:
        main_content = Content(c_id, title)
        attrs = prepare_attrs(genres, categories, persons, awards, properties, tags)
        main_content.set_terms(rd, attrs)
        break

    if main_content:
        other_contents_sql = sql % "WHERE c.id != %s AND c.compilation_id IS NULL ORDER BY c.id"
        cursor.execute(other_contents_sql, (content_id,))
        result = {}
        for c_id, title, genres, categories, persons, awards, properties, tags in cursor:
            other_content = Content(c_id, title)
            attrs = prepare_attrs(genres, categories, persons, awards, properties, tags)
            other_content.set_terms(rd, attrs)

            sim = get_similarity(main_content, other_content)
            if sim > 0:
                result[title] = sim

        result = sorted(result.iteritems(), key=lambda x: x[1], reverse=True)[:top]
        for k, v in result:
            print k, v


if __name__ == "__main__":
    conn = psycopg2.connect(database="da", user="da", host="localhost", port="5432")
    cursor = conn.cursor()
    redis = Redis()

    find_similar_for_content(64252, cursor, redis)

    conn.close()
