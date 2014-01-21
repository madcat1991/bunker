#coding: utf-8

"""
Версия в которой берутся только атрибуты, которые встречаются не менее чем у n
контентов
"""
import argparse
import logging
import psycopg2
import sys

logger = logging.getLogger("easy_way_good_attrs")


class Content(object):
    def __init__(self, content_id, title):
        self.id = content_id
        self.title = title
        self.tags = set()
        self.categories = set()
        self.genres = set()
        self.properties = set()
        self.persons = set()
        self.awards = set()

    def __str__(self):
        return "%s. %s" % (self.id, self.title)


category_sql = """
    SELECT g.category_id, ARRAY_AGG(DISTINCT m2m.content_id) as contents
    FROM m2m_content_content_genre m2m
        JOIN content_genre g ON m2m.content_genre_id = g.id
    GROUP BY g.category_id
"""

genre_sql = """
    SELECT content_genre_id, ARRAY_AGG(DISTINCT content_id) as contents
    FROM m2m_content_content_genre
    GROUP BY content_genre_id
"""

award_sql = """
    SELECT award_id, ARRAY_AGG(DISTINCT content_id) as contents
    FROM content_award
    GROUP BY award_id
"""

person_sql = """
    SELECT person_id, ARRAY_AGG(DISTINCT content_id) as contents
    FROM content_person
    GROUP BY person_id
"""

tag_sql = """
    SELECT tags_id, ARRAY_AGG(DISTINCT content_id) as contents
    FROM m2m_content_tags
    GROUP BY tags_id
"""

property_sql = """
    SELECT property_id || '-' || property_value_id as property, ARRAY_AGG(DISTINCT content_id) as contents
    FROM content_property
    GROUP BY property
"""

content_sql = "SELECT id, title FROM content"


def collect_data(args):
    data = {}
    conn = psycopg2.connect(
        database=args.db_name,
        user=args.db_user,
        host=args.db_host,
        port=args.db_port
    )
    cursor = conn.cursor()

    cursor.execute(content_sql)
    for c_id, c_title in cursor:
        content = Content(c_id, c_title)
        data[c_id] = content
    logger.info("CONTENT DATA COLLECTED")

    cursor.execute(category_sql)
    for attr_id, content_ids in cursor:
        if content_ids and len(content_ids) >= args.limit_n:
            for c_id in content_ids:
                data[c_id].categories.add(attr_id)
    logger.info("CATEGORIES COLLECTED")

    cursor.execute(genre_sql)
    for attr_id, content_ids in cursor:
        if content_ids and len(content_ids) >= args.limit_n:
            for c_id in content_ids:
                data[c_id].genres.add(attr_id)
    logger.info("GENRES COLLECTED")

    cursor.execute(award_sql)
    for attr_id, content_ids in cursor:
        if content_ids and len(content_ids) >= args.limit_n:
            for c_id in content_ids:
                data[c_id].awards.add(attr_id)
    logger.info("AWARDS COLLECTED")

    cursor.execute(tag_sql)
    for attr_id, content_ids in cursor:
        if content_ids and len(content_ids) >= args.limit_n:
            for c_id in content_ids:
                data[c_id].tags.add(attr_id)
    logger.info("TAGS COLLECTED")

    cursor.execute(person_sql)
    for attr_id, content_ids in cursor:
        if content_ids and len(content_ids) >= args.limit_n:
            for c_id in content_ids:
                data[c_id].persons.add(attr_id)
    logger.info("PERSONS COLLECTED")

    cursor.execute(property_sql)
    for attr_id, content_ids in cursor:
        if content_ids and len(content_ids) >= args.limit_n:
            for c_id in content_ids:
                data[c_id].properties.add(c_id)
    logger.info("PROPERTIES COLLECTED")
    conn.close()

    def check(dict_entity):
        c = dict_entity[1]
        return any([c.categories, c.genres, c.awards, c.tags, c.persons, c.properties])
    return dict(filter(check, data.iteritems()))


def get_similarity(content1, content2):
    up = len(content1.categories.intersection(content2.categories))
    up += len(content1.genres.intersection(content2.genres))
    up += len(content1.tags.intersection(content2.tags))
    up += len(content1.awards.intersection(content2.awards))
    up += len(content1.persons.intersection(content2.persons))
    up += len(content1.properties.intersection(content2.properties))
    if up > 0:
        down = len(content1.categories.union(content2.categories))
        down += len(content1.genres.union(content2.genres))
        down += len(content1.tags.union(content2.tags))
        down += len(content1.awards.union(content2.awards))
        down += len(content1.persons.union(content2.persons))
        down += len(content1.properties.union(content2.properties))
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
    parser = argparse.ArgumentParser(description=u'Скрипт для поиска похожего контента')
    parser.add_argument('--db_host', default="localhost", type=str, dest="db_host",
                        help=u'Хост БД, по умолчанию: localhost')
    parser.add_argument('--db_port', default="5432", type=str, dest="db_port",
                        help=u'Порт БД, по умолчанию: 5432')
    parser.add_argument('--db_name', default="da_prod", type=str, dest="db_name",
                        help=u'Имя БД, по умолчанию: da')
    parser.add_argument('--db_user', default="da", type=str, dest="db_user",
                        help=u'Пользователь БД, по умолчанию: da')
    parser.add_argument('-c', default=64252, type=int, dest="similar_to",
                        help=u'Идентификатор контента для которого будут искаться похожие. '
                             u'По умолчанию: 64252 (Темный рыцарь)')
    parser.add_argument('-n', default=2, type=int, dest="limit_n",
                        help=u'Количество единиц контента, которые должны встречаться с '
                             u'атрибутом. По умолчанию: 2')
    parser.add_argument('-t', default=20, type=int, dest="top",
                        help=u'Размер топа результатов. '
                             u'По умолчанию находятся первые 20 похожих единиц контента')

    args = parser.parse_args()
    logging.basicConfig(stream=sys.stdout, level="INFO", format="%(asctime)s :: %(message)s")
    data = collect_data(args)
    logger.info("DATA COLLECTED")
    find_similar_content(data, similar_to=args.similar_to, top=args.top)
