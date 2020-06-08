## Python обертка для запросов в БД [Clickhouse](https://clickhouse.yandex/)

Обертка сделана вокруг [clickhouse-driver](https://clickhouse-driver.readthedocs.io)

Написано на версии python 3.5

## Установка
```
pip install clickhousepy
или
pip install clickhousepy[pandas]  (для установки pandas)
```


## Получение данных из Clickhouse в формате Pandas Dataframe
```python
from clickhousepy import Client
import datetime as dt

TEST_DB = "__chpytest12345"
TEST_TABLE = "__chpytest12345"


client.create_db(TEST_DB)
client.create_table_mergetree(
    TEST_DB, TEST_TABLE,
    columns=["s String"],
    orders=["s"],
)
client.insert(
    TEST_DB, TEST_TABLE,
    [{"s": "1"}],
    columns=["s"],  # columns необязательный параметр
) 
query = "SELECT s FROM {}.{}".format(TEST_DB, TEST_TABLE)
r = client.get_df(query, columns_names=["Col S"])
print("данные в формате dataframe:\n", r)
```

## Краткая документация по некоторым методам
```python
from clickhousepy import Client
import datetime as dt


TEST_DB = "__chpytest12345"
TEST_TABLE = "__chpytest12345"

client = Client(host="", user="", password="")

r = client.show_databases()
print("список баз данных:", r)

client.create_db(TEST_DB)
client.create_table_mergetree(
    TEST_DB, TEST_TABLE,
    columns=["s String"],
    orders=["s"],
)
client.insert(
    TEST_DB, TEST_TABLE,
    [{"s": "1"}],
    columns=["s"],  # columns необязательный параметр
) 
# Здесь подробно о формате, вставляемых данных.
# https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#inserting-data

r = client.get_count_rows(TEST_DB, TEST_TABLE)
print("кол-во строк:", r)

r = client.execute("SELECT * FROM {}.{}".format(TEST_DB, TEST_TABLE))
print("данные запроса:", r)

r = client.get_df("SELECT * FROM {}.{}".format(TEST_DB, TEST_TABLE), columns_names=["Col S"])
print("данные в формате dataframe:", type(r), r)


## Можно инициализировать класс DB и Table и получить лучше читаемый код.

db = client.DB(TEST_DB)
r = db.show_tables()
print("список таблиц базы данных {}:".format(TEST_DB), r)

table = client.Table(TEST_DB, TEST_TABLE)
r = table.get_count_rows()
print("кол-во строк:", r)

print("удаление таблиц")
table.drop_table()

print("удаление базы данных")
db.drop_db()


## Некоторые методы возвращают объект DB и Table.

db = client.create_db(TEST_DB)

table = db.create_table_mergetree(
    TEST_TABLE,
    columns=["s String", "t String", "d Date"],
    orders=["d"],
    partition=["s", "d"],
)

r = table.show_create_table()
print("описание создания таблицы", r)

r = table.describe()
print("столбцы таблицы", r)

print("вставка данных")
table.insert(
    [
        {"s": "1", "t": "1", "d": dt.datetime(2000, 1, 1)},
        {"s": "2", "t": "2", "d": dt.datetime(2000, 1, 2)},
        {"s": "3", "t": "3", "d": dt.datetime(2000, 1, 3)},
        {"s": "4", "t": "4", "d": dt.datetime(2000, 1, 4)},
    ],
    columns=["s", "t", "d"],
)

# в инициализрованном классе Table
# при запросах данных через query или get_df,
# {db} и {table} заменяться внутри метода
r = table.query("SELECT * FROM {db}.{table}")
print("данные запроса:", r)

r = table.get_df("SELECT * FROM {db}.{table}")
print("данные запроса в формате dataframe:", type(r), r)

r = table.get_df("SELECT * FROM {db}.{table}", columns_names=["S", "T", "D"])
print("данные запроса в формате dataframe c определением названия столбцов:", type(r), r)

r = table.get_count_rows()
print("кол-во строк:", r)

r = table.get_min_date(date_column_name="d")
print("минимальная дата:", r)

r = table.get_max_date(date_column_name="d")
print("максимальная дата:", r)

print("удаление партиций")
table.drop_partitions([["3", "2000-01-03"], ["4", "2000-01-04"]])

r = table.get_count_rows()
print("кол-во строк после удаления партиций:", r)

print("мутация обновления строки")
table.update(update="t = '20' ", where="t = '2' ")

print("мутация удаления строки")
table.delete(where="t = '20'")
time.sleep(1)
r = table.get_count_rows()
print("кол-во строк после мутации удаления строки:", r)

print("очистка таблицы")
table.truncate()
r = table.get_count_rows()
print("кол-во строк после очистки таблицы:", r)

new_table_name = TEST_TABLE + "_new"
print("переименование таблицы {} в {}".format(TEST_TABLE, new_table_name))
new_table = table.rename(TEST_DB, new_table_name)

r = table.exists()
print("существует таблица {}?".format(TEST_TABLE), r)

r = new_table.exists()
print("существует таблица {}?".format(new_table_name), r)

print("удаление таблиц")
table.drop_table()
new_table.drop_table()

print("удаление базы данных")
db.drop_db()
```


## Зависимости
- [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver/)
- [pandas](https://github.com/pandas-dev/pandas) (Опционально)

## Автор
Павел Максимов

Связаться со мной можно в 
[Телеграм](https://teleg.run/pavel_maksimow) 
и в 
[Facebook](https://www.facebook.com/pavel.maksimow)

Удачи тебе, друг! Поставь звездочку ;)
