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
    columns=["i UInt32"],
    orders=["i"],
)
client.insert(
    TEST_DB, TEST_TABLE,
    [{"i": 1}, {"i": 2}],
) 
query = "SELECT i FROM {}.{}".format(TEST_DB, TEST_TABLE)
r = client.get_df(query, columns_names=["Col Integer"])
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

# Создание базы данных.
client.create_db(TEST_DB)
# Создание таблицы.
client.create_table_mergetree(
    TEST_DB, TEST_TABLE,
    columns=["s String"],
    orders=["s"],
)
# Вставка данных.
# Подробнее об этом тут
# https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#inserting-data
client.insert(
    TEST_DB, TEST_TABLE,
    [{"s": "1"}],
) 

r = client.exists(TEST_DB, TEST_TABLE)
print("таблица существует? ", r)

r = client.get_count_rows(TEST_DB, TEST_TABLE)
print("кол-во строк:", r)

# Любой запрос.
r = client.execute("SELECT * FROM {}.{}".format(TEST_DB, TEST_TABLE))
print("данные запроса:", r)
```

### Сущность DB 
```python
db = client.DB(TEST_DB)
r = db.show_tables()
print("список таблиц базы данных {}:".format(TEST_DB), r)

table = client.Table(TEST_DB, TEST_TABLE)
r = table.get_count_rows()
print("кол-во строк:", r)

# Очистка таблицы.
table.truncate()
# Удаление таблицы.
table.drop_table()
# Удаление базы данных.
db.drop_db()
```

### Сущность Table 
```python
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

data = table.select()
print("Первые 10 строк таблицы", data)

data = table.select(limit=1, columns=["s"], where="s = 2")
print("Выборка с фильтрацией", data)

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
table.rename(TEST_DB, new_table_name)

r = client.exists(TEST_DB, TEST_TABLE)
print("существует таблица {}?".format(TEST_TABLE), r)

print("удаление таблиц")
table.drop_table()

print("удаление базы данных")
db.drop_db()
```


### Метод копирования данных из одной таблицы в другую с проверкой кол-ва строк после копирования
```python
client.drop_db(TEST_DB)
db = client.create_db(TEST_DB)
table = db.create_table_mergetree(
    TEST_TABLE,
    columns=["string String", "integer UInt32", "dt DateTime"],
    orders=["string"],
    partition=["string"],
)
table.insert(
    [
        {"string": "a", "integer": 1, "dt": dt.datetime(2000, 1, 1)},
        {"string": "b", "integer": 2, "dt": dt.datetime(2000, 1, 2)},
        {"string": "c", "integer": 3, "dt": dt.datetime(2000, 1, 3)},
        {"string": "c", "integer": 3, "dt": dt.datetime(2000, 1, 3)},
    ],
)

table_name_2 = TEST_TABLE + "_copy"
table2 = table.copy_table(TEST_DB, table_name_2, return_new_table=True)
is_identic = table2.copy_data_from(
    TEST_DB, TEST_TABLE,
    where="string != 'c' ",
    columns=["string"]
)
# Функция вернет bool значение, кол-во строк сопадает или нет, после копирования.
assert is_identic
```

### Метод копирования данных из одной таблицы в другую с удалением дублирующихся строк.
```python
client.drop_db(TEST_DB)
db = client.create_db(TEST_DB)
table = db.create_table_mergetree(
    TEST_TABLE,
    columns=["string String", "integer UInt32", "dt DateTime"],
    orders=["string"],
    partition=["string"],
)
table.insert(
    [
        {"string": "a", "integer": 1, "dt": dt.datetime(2000, 1, 1)},
        {"string": "b", "integer": 2, "dt": dt.datetime(2000, 1, 2)},
        {"string": "c", "integer": 3, "dt": dt.datetime(2000, 1, 3)},
        {"string": "c", "integer": 3, "dt": dt.datetime(2000, 1, 3)},
    ],
)

table_name_2 = TEST_TABLE + "_copy"
table2 = table.copy_table(TEST_DB, table_name_2, return_new_table=True)
# При удалении дублирующихся строк (distinct=True), 
# проверки на кол-во строк после копирования не будет.
table2.copy_data_from(
    TEST_DB, TEST_TABLE,
    columns=["string"],
    distinct=True
)
assert 3 == table2.get_count_rows()
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
