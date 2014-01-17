#coding: utf-8

"""
Скрипт для сбора данных об атрибутах

Создадим индекс.
Для каждого атрибута храним ключ атрибута, контент в котором встречается атрибут и количества раз, которые атрибут встретился с этим контентом. Все атрибуты - текстовые поля.

Атрибуты это:
* жанры и категории
* персоны
* награды
* свойства
* тэги

* слова из текста для эмбеда (кроме стопслов) - будет в будущем

Дополнительно считаем количество атрибутов у контента
"""
from multiprocessing import Process
from redis import Redis
import psycopg2

from common import content_key, attr_key


class DBToRedisBaseProcess(Process):
    def __init__(self, name):
        super(DBToRedisBaseProcess, self).__init__(name=name)
        self.conn = psycopg2.connect(database="da", user="da", host="localhost", port="5432")
        self.redis = Redis()


class GenresAndCategoriesToAttributes(DBToRedisBaseProcess):
    def run(self):
        cursor = self.conn.cursor()
        sql = """
            SELECT m2m.content_id, g.title, c.title
            FROM m2m_content_content_genre m2m
                JOIN content_genre g ON m2m.content_genre_id = g.id
                JOIN content_category c ON g.category_id = c.id
        """
        cursor.execute(sql)

        categories = set()
        for content_id, genre, category in cursor:
            self.redis.hincrby(attr_key(genre), content_id, 1)
            self.redis.incr(content_key(content_id), 1) 

            if category not in categories:
                self.redis.hincrby(attr_key(category), content_id, 1)
                self.redis.incr(content_key(content_id), 1)
                categories.add(category)
        self.conn.close()


class PersonsToAttributes(DBToRedisBaseProcess):
    def run(self):
        cursor = self.conn.cursor()
        #На данном этапе разработки мы не различаем типа персоны
        sql = """
            SELECT cp.content_id, p.name
            FROM content_person cp
                JOIN person p ON cp.person_id = p.id
        """
        cursor.execute(sql)

        for content_id, person in cursor:
            self.redis.hincrby(attr_key(person), content_id, 1)
            self.redis.incr(content_key(content_id), 1)
        self.conn.close()


class AwardsToAttributes(DBToRedisBaseProcess):
    def run(self):
        cursor = self.conn.cursor()
        sql = """
            SELECT ca.content_id, a.title
            FROM content_award ca
                JOIN award a ON ca.award_id = a.id
        """
        cursor.execute(sql)

        for content_id, award in cursor:
            self.redis.hincrby(attr_key(award), content_id, 1)
            self.redis.incr(content_key(content_id), 1)
        self.conn.close()


class PropertiesToAttributes(DBToRedisBaseProcess):
    def run(self):
        cursor = self.conn.cursor()
        sql = """
            SELECT cp.content_id, p.title, pv.title
            FROM content_property cp
                JOIN property p ON cp.property_id = p.id
                JOIN property_value pv ON cp.property_value_id = pv.id
        """
        cursor.execute(sql)

        for content_id, prop, value in cursor:
            self.redis.hincrby(attr_key(prop + "-" + value), content_id, 1)
            self.redis.incr(content_key(content_id), 1)
        self.conn.close()


class TagsToAttributes(DBToRedisBaseProcess):
    def run(self):
        cursor = self.conn.cursor()
        sql = """
            SELECT m2m.content_id, t.title
            FROM m2m_content_tags m2m
                JOIN tags t ON m2m.tags_id = t.id
        """
        cursor.execute(sql)

        for content_id, tag in cursor:
            self.redis.hincrby(attr_key(tag), content_id, 1)
            self.redis.incr(content_key(content_id), 1)
        self.conn.close()


def main():
    collectors = [
        GenresAndCategoriesToAttributes("genres_and_categories"),
        PersonsToAttributes("persons"),
        AwardsToAttributes("awards"),
        PropertiesToAttributes("properties"),
        TagsToAttributes("tags"),
    ]

    # чистим БД
    collectors[0].redis.flushdb()

    for p in collectors:
        p.start()

    for p in collectors:
        print "Waiting for: %s" % p.name
        p.join()
        print "%s finished" % p.name

    # контрольное сохранение
    collectors[0].redis.save()

if __name__ == "__main__":
    main()
