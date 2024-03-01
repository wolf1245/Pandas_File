#!/usr/bin/env python
# coding: utf-8

# # Чтение и запись данных. Часть 2
# 
# ## 1. Работа с базой данных из pandas
# 

# ### 1.1 
# 
# Подготовка данных:

# In[3]:


import psycopg2
import pandas as pd
import sqlite3

pg_connection = {
    "host": "dsstudents.skillbox.ru",
    "port": 5432,
    "dbname": "db_ds_students",
    "user": "readonly",
    "password": "6hajV34RTQfmxhS"
}
conn = psycopg2.connect(**pg_connection)

cursor = conn.cursor()

# получаем имена таблиц из базы
sql_str = "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
cursor.execute(sql_str)
tables_data = [a for a in cursor.fetchall()]
conn.commit()

print("Какие таблицы содержатся в Postgres: %s" % tables_data)

df = pd.read_sql_query("select * from public.ratings limit 5;", conn)
df.head()


# **Задание простого уровня** <br>Посчитайте количество записей в каждой из таблиц **keywords, links, ratings** и выведите в формате имя *таблицы:количество строк*
# 
# *Результат работы:*
# <pre>
# table keywords:92838
# table links:91686
# table ratings:1555552
# </pre>

# In[5]:


import psycopg2

pg_connection = {
    "host": "dsstudents.skillbox.ru",
    "port": 5432,
    "dbname": "db_ds_students",
    "user": "readonly",
    "password": "6hajV34RTQfmxhS"
}
conn = psycopg2.connect(**pg_connection)
cursor = conn.cursor()

table_names = [i[0] for i in tables_data]
for table in table_names:
        # -- ВАШ КОД ТУТ --
        sql_str = "SELECT 1 FROM public.{} LIMIT 1;".format(table)
        cursor.execute(sql_str)
        row_count = [a for a in cursor.fetchall()][0][0]
        print("table {}:{}".format(table, row_count))

cursor.close()


# ### 1.2
# 
# **Задание среднего уровня** <br>Напечатайте колонки таблицы *ratings*. Подсказка: поисследуйте объект **cursor** с помощью встроенной в python команды *dir()*

# In[1]:


import psycopg2

pg_connection = {
    "host": "dsstudents.skillbox.ru",
    "port": 5432,
    "dbname": "db_ds_students",
    "user": "readonly",
    "password": "6hajV34RTQfmxhS"
}
conn = psycopg2.connect(**pg_connection)
cursor = conn.cursor()

sql_str = "SELECT * FROM ratings LIMIT 10;"
cursor.execute(sql_str)
conn.commit()



# -- ВАШ КОД ТУТ Готово--
table_rating_fields = [dir(cursor)]
print("Поля таблицы ratings: %s" % table_rating_fields)

cursor.close()


# ### 1.3
# 
# **Задание высокого уровня**
# * Посмотрите на столбцы таблицы **links** и столбцы таблицы **ratings** по какому полю можно заджойнить эти две таблицы
# * Посчитайте количество фильмов из links, у который нет оценок в ratings.

# In[4]:


import psycopg2

pg_connection = {
    "host": "dsstudents.skillbox.ru",
    "port": 5432,
    "dbname": "db_ds_students",
    "user": "readonly",
    "password": "6hajV34RTQfmxhS"
}
conn = psycopg2.connect(**pg_connection)
cursor = conn.cursor()


# -- ВАШ КОД ТУТ - Готово --
sql_str = """
    SELECT COUNT(DISTINCT links.movieId)
    FROM links AS ls
    LEFT JOIN ratings AS rs
    USING(movieId);
"""

cursor.execute(sql_str)
row_count = [a for a in cursor.fetchall()][0][0]
conn.commit()

print("Количество фильмов без рейтингов: %s" % row_count)

cursor.close()


# ## 2. Работа с БД: MongoDB
# 
# ### 2.1
# 
# Подготовка данных

# In[11]:


from pymongo import MongoClient

mongo_connection = {
    "host": "dsstudents.skillbox.ru",
    "port": 27017,
    "user": "students",
    "password": "X63673t47Gl03Sq",
    "authSource": "movies"
}

mongo = MongoClient('mongodb://%s:%s@%s:%s/?authSource=%s' % (
    mongo_connection['user'], mongo_connection['password'],
    mongo_connection['host'], mongo_connection['port'], mongo_connection['authSource'])
)
db = mongo["movies"]

print("Коллекции, доступные в MongoDB: %s" % db.list_collection_names())

collection = db['tags']
print("Число документов в коллекции %s" % collection.estimated_document_count())


# **Задание простого уровня** <br>Посчитайте количество контента, у которого встречается тэг "toy"
# 
# * сделайте выборку такого контента (а точнее - поля `id`) в питоновский `list`
# * подсчитайте количество элементов в `list`

# In[14]:


selector = {'name': 'toy'}
exclude_id = {'_id': False}

mongo_cursor = collection.find(projection=exclude_id, filter={'name': 'toy'})

# -- ВАШ КОД ТУТ --

cursor_items = set([])
collection = db['list']
print("Число документов в коллекции %s" % collection.estimated_document_count())

print("Количество контента с тэгом 'toy': %s\n" % len(cursor_items))


# ### 2.2
# 
# **Задание среднего уровня** <br> Выведите **только названия** 10 самых непопулярных тэгов <br>
# 
# Подсказка: посмотрите в уроке, как MongoDB позволяет также выполнять сложные агрегирующие запросы средствами СУБД
# 
# *Результат работы*
# <pre>
# Счётчик тэгов ['ancient tablet', 'remains', 'robespierre', 'social scandal', 'brain implant', 'adam west', 'arm cast', 'acab', 'gas explosion', 'female psychologist']
# </pre>

# In[15]:


# -- ВАШ КОД ТУТ --


# ### 2.3
# 
# **Задание высокого уровня**
# 
# 1. Подключитесь к Postgres. Найдите top-10 самого популярного контента
# 1. Сохраните в файл `content_popularity.json` в виде
# <pre>
# {'movieId': 931, 'popularity': 999}
# {'movieId': 429, 'popularity': 111}
# </pre>
# 
# Этот формат называется single-line json и его можно загрузить в MongoDB с помощью команды
# <pre>
# mongoimport --host $APP_MONGO_HOST --port $APP_MONGO_PORT --db movies --collection tags --file content_popularity.json
# </pre>
# 
# **ВНИМАНИЕ!** Это просто пример как можно использовать single-line JSON, заливать его никуда не нужно - в задаче требуется только подготовить файл в правильном виде
# 
# *Подсказка* воспользуйтесь функцией `json.dumps`
# 
# *Результат работы* - файл `content_popularity.json`:
# 
# <pre>
# {"movieId": 318, "popularity": 5626}
# {"movieId": 356, "popularity": 5464}
# {"movieId": 296, "popularity": 5242}
# {"movieId": 593, "popularity": 5048}
# {"movieId": 2571, "popularity": 4718}
# {"movieId": 260, "popularity": 4556}
# {"movieId": 480, "popularity": 4540}
# {"movieId": 527, "popularity": 4120}
# {"movieId": 1, "popularity": 4072}
# {"movieId": 110, "popularity": 3928}
# </pre>

# In[ ]:


# -- ВАШ КОД ТУТ --

