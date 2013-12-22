# coding: utf-8
"""
    Определение похожего контента.

    Везде используется обычная схема нумерации записей каталога:
        единичный контент id * 10 + 0
        сборник id * 10 + 1

    Что умеет и как с этим работать:

    >>> cat = similiarity_computer.Catalogue()
    >>> cat.load_catalogue()

    1. Ищем контент, похожий на другой контент

    >>> _id = 78851
    >>> sims = cat.find_similar_for_catalogue_item(_id)
    >>> cat.print_similar_results(_id, sims)

    Similiar content for 78851 Что сказал покойник
    0.712250712251 83931 Провинциальные страсти 1.0 * 1.00 + 1.0 * 1.00 + 3.0 * 2.00 + 0.5 * 0.00 + 0.3 * 0.00 + 1.0 * 0.33
    0.712250712251 84861 Евлампия Романова. Следствие ведет дилетант 1.0 * 1.00 + 1.0 * 1.00 + 3.0 * 2.00 + 0.5 * 0.00 + 0.3 * 0.00 + 1.0 * 0.33
    0.71078869795 71321 Иван Подушкин. Джентльмен сыска 1.0 * 1.00 + 1.0 * 1.00 + 3.0 * 2.00 + 0.5 * 0.00 + 0.3 * 0.00 + 1.0 * 0.32

    2. Ищем контент, похожий на нечто среднее из истории просмотров

    >>> my_history = [610940, 199090, 78931, 960850]
    >>> sims = cat.find_similar_for_watch_history(my_history, limit=5)
    >>> cat.print_similar_results(my_history, sims)

    Similiar content for 610940 Зорро
    Similiar content for 199090 Особенности национальной охоты
    Similiar content for 78931 Шерлок
    Similiar content for 960850 Хоббит: Нежданное путешествие
    0.338994250046 983350 Джекпот 1.0 * 0.00 + 1.0 * 1.33 + 3.0 * 3.41 + 0.5 * 25.10 + 0.3 * 0.00 + 1.0 * 0.00
    0.338994250046 960990 007: Координаты «Скайфолл» 1.0 * 0.00 + 1.0 * 1.33 + 3.0 * 3.41 + 0.5 * 25.10 + 0.3 * 0.00 + 1.0 * 0.00
    0.327775512786 984430 Доспехи Бога 3: Миссия Зодиак 1.0 * 0.00 + 1.0 * 1.33 + 3.0 * 3.52 + 0.5 * 22.85 + 0.3 * 0.00 + 1.0 * 0.00
    0.32760680997 1023120 Черный тюльпан 1.0 * 0.00 + 1.0 * 1.33 + 3.0 * 3.52 + 0.5 * 21.26 + 0.3 * 2.60 + 1.0 * 0.00
    0.326847647299 985330 Добро пожаловать в капкан 1.0 * 0.00 + 1.0 * 1.33 + 3.0 * 3.41 + 0.5 * 23.38 + 0.3 * 0.00 + 1.0 * 0.00

    3. Ищем контент, похожий на отдельные записи из истории просмотров
    (с объяснениями, на какую запись похоже)

    >>> sims = cat.find_similar_for_watch_history_separate(my_history, shuffle=True, limit=5)
    >>> cat.print_similar_results(my_history, sims)
    Similiar content for 610940 Зорро  
    Similiar content for 199090 Особенности национальной охоты  
    Similiar content for 78931 Шерлок  
    Similiar content for 960850 Хоббит: Нежданное путешествие  
    0.558703708627 834270 Разборки в стиле кунг-фу Зорро 
    0.467904653051 978110 Неудержимый Шерлок 
    0.558448312568 25640 Агент 117: Шпионское гнездо Зорро 
    0.454241228029 452370 Викинги Хоббит: Нежданное путешествие 
    0.519451044804 91571 Улица потрошителя Шерлок
"""

import math
import random
from contextlib import contextmanager
from collections import defaultdict
import time
import logging
import sys

import psycopg2


logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s :: %(message)s")


ATTRIBUTE_KINDS = ['genres', 'property_values', 'persons', 'categories']


@contextmanager
def timing(title):
    t0 = time.time()
    yield
    t1 = time.time()
    logging.info(u'%s: %.3f s', title, t1 - t0)

def timed(fn):
    def __inner(*args, **kwargs):
        t0 = time.time()
        r = fn(*args, **kwargs)
        t1 = time.time()
        logging.info(u'%s: %.3f s', str(fn), t1 - t0)
        return r
    return __inner


def sethash(_set):
    n = int()
    m = 61
    for i in _set:
        n = n | (1 << (i % m))
    return n


class Catalogue(object):
    def __init__(self):
        self.id_to_title = {}
        self.ids_by_genre = defaultdict(set)
        self.id_to_info = {}
        self.db = psycopg2.connect(user='da', dbname='da_prod')

    @timed
    def load_catalogue(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT catalogue_id, orig_country, title, release_year, property_values, categories, genres, persons FROM catalogue_vector')

        for row in cursor:
            catalogue_id, orig_country, title, release_year, property_values, categories, genres, persons = row
            self.id_to_title[catalogue_id] = title
            for genre_id in genres:
                self.ids_by_genre[genre_id].add(catalogue_id)

            info = {
                'release_year': release_year,
                'orig_country': orig_country,
                'property_values': set(property_values),
                'categories': set(categories),
                'genres': set(genres),
                'persons': set(persons),
            }

            for k in ATTRIBUTE_KINDS:
                info[k + '_h'] = sethash(info[k])

            self.id_to_info[catalogue_id] = info
        logging.info('Loaded %d catalogue items', len(self.id_to_info))

    def make_weighted_union(self, catalogue_ids):
        """
            По множеству записей каталога получаем два словаря (info, weights_info):
                info --- словарь с объединением всех жанров, свойств, персон
                weights_info --- словарь с весами жанров, свойств, персон,  где вес
                пропорционален числу записей каталога из заданного множества с этим жанром|свойством|персоной
        """
        info = {
            'categories': set(),
            'genres': set(),
            'property_values': set(),
            'persons': set(),
        }
        weights_info = {
            'categories': defaultdict(int),
            'genres': defaultdict(int),
            'property_values': defaultdict(int),
            'persons': defaultdict(int),
        }
        for c1 in catalogue_ids:
            c1_info = self.id_to_info[c1]
            for kind in ATTRIBUTE_KINDS:
                for i in c1_info[kind]:
                    info[kind].add(i)
                    weights_info[kind][i] += 1  # пока записываем кол-во совпавших записей, потом перегоним в вес

        # далее веса расчитываются как base ^ (кол-во записей)
        bases = {
            'categories': 1.1,
            'genres': 1.1,
            'property_values': 1.2,
            'persons': 1.3,
        }

        for kind in ATTRIBUTE_KINDS:
            for i in weights_info[kind]:
                n = weights_info[kind][i]
                weights_info[kind][i] = bases[kind] ** n

        for k in ATTRIBUTE_KINDS:
            info[k + '_h'] = sethash(info[k])

        return info, weights_info

    def lookup_catalogue(self, catalogue_ids):
        rows = []
        for c1 in catalogue_ids:
            rows.append([c1, self.id_to_title[c1]])
        return rows

    def find_similar_for_catalogue_item(self, catalogue_id, limit=15):
        """ Поиск похожих записей каталога для заданной записи каталога """
        c1_info = self.id_to_info[catalogue_id]
        return self.find_similar_for_info(c1_info, exclude_ids=set([catalogue_id]), limit=limit)

    def find_similar_for_watch_history(self, catalogue_ids, limit=15):
        """ Поиск похожих записей каталога для заданной истории просмотров

            Метод первый. Объединяем атрибуты записей истории просмотров в один объект
            с учетом весов (если в истории много комедий, то вес жанра "комедии" будет выше),
            а потом выдаем рекомендации для этого объекта.
        """
        united_info, weights_info = self.make_weighted_union(catalogue_ids)
        return self.find_similar_for_info(united_info, exclude_ids=set(catalogue_ids),
                limit=limit, weights_info=weights_info)

    def find_similar_for_watch_history_separate(self, catalogue_ids, limit=15, shuffle=False):
        """ Поиск похожих записей каталога для заданной истории просмотров

            Метод второй: делаем рекомендации для отдельных пунктов из истории, а потом объединяем.
        """
        united_similiar = []
        already_recommended = set()
        for c1 in catalogue_ids:
            title = self.id_to_title[c1]
            similiars = self.find_similar_for_catalogue_item(c1)
            # в качестве объяснения рекомендации подставляем title пункта из истории
            for similiar_row in similiars:
                similiar_row = list(similiar_row)
                similiar_row[2] = title
                if similiar_row[0] not in already_recommended:
                    already_recommended.add(similiar_row[0])
                    united_similiar.append(similiar_row)

        if shuffle:
            random.shuffle(united_similiar)
        else:
            # выдаем топ-15 общих рекомендаций
            united_similiar.sort(key=lambda similiar_row: similiar_row[0])

        united_similiar = united_similiar[:limit]
        return united_similiar


    def find_similar_for_info(self, c1_info, limit=15, exclude_ids=set(), weights_info={}):
        """ Поиск похожих для словаря c1_info с жанрами, свойствами и персонами

            :param exclude_ids: какие записи исключить из результата
            :param c1_info: словарь с жанрами | свойствами | пр.
            :param weights_info: словарь весов отдельных жанров | свойств:
                {
                    'genres': {1: 1.1, 2: 1.25},
                    'property_values': {66: 1.25},
                }
        """
        with_intersected_genres = set()
        for genre_id in c1_info['genres']:
            with_intersected_genres.update(self.ids_by_genre[genre_id])

        similiar = []  # list of (metric, catalogue_id)

        def calc_intersection_metric(c2_info, kind):
            key_h = kind + '_h'
            if not (c1_info[key_h] & c2_info[key_h]):
                return 0.0
            intersection = c1_info[kind] & c2_info[kind]
            weights = weights_info.get(kind, {})
            if not weights:
                return len(intersection)
            return sum(weights.get(i, 1.0) for i in intersection)

        # веса в итоговой формуле
        w_country = 1.0
        w_category = 1.0
        w_genre = 3.0
        w_property = 0.5
        w_people = 0.3
        w_date = 1.0

        def calc_total_metric(c2_info):
            """ Вычисление общей метрики похожести c1_info на c2_info
            """
            same_country = 0
            if 'orig_country' in c1_info and 'orig_country' in c2_info:
                same_country = 1 if c1_info['orig_country'] == c2_info['orig_country'] else 0

            n_common_categories = calc_intersection_metric(c2_info, 'categories')
            n_common_genres = calc_intersection_metric(c2_info, 'genres')
            n_common_properties = calc_intersection_metric(c2_info, 'property_values')
            n_common_people = calc_intersection_metric(c2_info, 'persons')

            date_similiarity = 0
            if c1_info.get('release_year') and c2_info.get('release_year'):
                date_similiarity = math.sqrt(1/(1.0 + abs(c1_info['release_year'] - c2_info['release_year'])))
            metric = w_country * same_country \
                + w_category * n_common_categories \
                + w_genre * n_common_genres \
                + w_property * n_common_properties \
                + w_people * n_common_people \
                + w_date * date_similiarity

            explain = ' + '.join([
                '%.1f * %.2f' % (w_country, same_country),
                '%.1f * %.2f' % (w_category, n_common_categories),
                '%.1f * %.2f' % (w_genre, n_common_genres),
                '%.1f * %.2f' % (w_property, n_common_properties),
                '%.1f * %.2f' % (w_people, n_common_people),
                '%.1f * %.2f' % (w_date, date_similiarity),
            ])
            return metric, explain

        # максимальная метрика для нормирования
        # считается как похожесть c1 на самого себя
        norm_total, _ = calc_total_metric(c1_info)
        if norm_total == 0:
            return []

        for c2 in with_intersected_genres:
            if c2 in exclude_ids:
                continue
            c2_info = self.id_to_info[c2]

            metric, explain = calc_total_metric(c2_info)

            if metric > 1.0:
                metric = metric / norm_total
                similiar.append((metric, c2, explain))

        # сортировка по метрике. При равенстве метрик выбираем порядок случайно
        similiar.sort(key=lambda k: (k[0], random.random()), reverse=True)
        similiar = similiar[:limit]
        return similiar

    def print_similar_results(self, catalogue_id, similiars):
        rows = self.prepare_similar_results(catalogue_id, similiars)
        for row in rows:
            for part in row:
                print part,
            print ''

    def prepare_similar_results(self, catalogue_id, similiars):
        rows = []

        if isinstance(catalogue_id, list):
            catalogue_ids = catalogue_id
        else:
            catalogue_ids = [catalogue_id]

        for c1 in catalogue_ids:
            rows.append(['Similiar content for', c1, self.id_to_title[c1], ''])
        for metric, c2, explain in similiars:
            rows.append([metric, c2, self.id_to_title[c2], explain])
        return rows

    @timed
    def find_em_all(self):
        out = open('similiar.csv', 'w')
        n = 0
        t0 = time.time()
        for c1 in self.id_to_info:
            similiar = self.find_similar_for_catalogue_item(c1)
            for metric, c2, explain in similiar:
                out.write('%s;%s;%s\n' % (c1, c2, metric))
            n += 1
            if n % 100 == 0:
                t1 = time.time()
                speed = n / float(t1 - t0)
                logging.info('processed %d catalogue items. Speed: %.1f items / s', n, speed)
        logging.info('processed %d catalogue items (finished)', n)


def main():
    cat = Catalogue()
    cat.load_catalogue()

    _id = 70290
    with timing('one_similiar'):
        sims = cat.find_similar_for_catalogue_item(_id, limit=15)

    cat.print_similar_results(_id, sims)

    #cat.find_em_all()

if __name__ == '__main__':
    #import pyximport
    #pyximport.install()
    #similiarity_computer = pyximport.load_module('similiarity_computer', __file__)
    #similiarity_computer.main()
    main()
