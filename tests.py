import datetime as dt
import time
from importlib.util import find_spec
from pprint import pprint

import yaml

from clickhousepy import Client

with open("config.yml", "r") as stream:
    data_loaded = yaml.safe_load(stream)

client = Client(
    host=data_loaded["host"], user=data_loaded["user"], password=data_loaded["password"]
)

TEST_DB = "__chpytest12345"
TEST_TABLE = "__chpytest12345"


def _decorator_function(func):
    # Перед запуском функции создает базу и таблицу с данными,
    # а при выходе из функции удаляет.
    def wrapper():
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
        func(db, table)
        db.drop_db(TEST_DB)

    return wrapper


@_decorator_function
def test_decorator_function(db, table):
    pass


@_decorator_function
def test_get_methods(db, table):
    print(table.get_count_rows())
    print(table.get_min_date(date_column_name="dt"))
    print(table.get_max_date(date_column_name="dt"))
    print(table.get_count_run_mutations())


@_decorator_function
def test_copy_data(db, table):
    table_name_2 = TEST_TABLE + "_copy"
    client.drop_table(TEST_DB, table_name_2)
    table2 = table.copy_table(TEST_DB, table_name_2, return_new_table=True)
    is_identic = table2.copy_data_from(
        TEST_DB, TEST_TABLE,
        where="string != 'c' ",
        columns=["string"]
    )
    # Проверка идентичности копии данных.
    print(
        table2.select(columns=["string"])
    )
    assert is_identic
    # Проверка удаления дублирующихся строк при копировании.
    table2.truncate()
    table2.copy_data_from(
        TEST_DB, TEST_TABLE,
        columns=["string"],
        distinct=True
    )
    assert 3 == table2.get_count_rows()


@_decorator_function
def test_drop_partitions_str(db, table):
    table.drop_partitions([["b"], ["c"]])
    time.sleep(1)
    assert table.get_count_rows() == 1
    table.truncate()
    assert table.get_count_rows() == 0


def test_drop_partitions_list():
    client.drop_db(TEST_DB)
    client.create_db(TEST_DB)
    client.create_table_mergetree(
        TEST_DB,
        TEST_TABLE,
        columns=["s String", "d Date"],
        orders=["s"],
        partition=["s", "d"],
    )
    t = client.Table(TEST_DB, TEST_TABLE)
    t.insert(
        [
            {"s": "S1", "d": dt.datetime(2000, 1, 1)},
            {"s": "S2", "d": dt.datetime(2000, 1, 2)},
            {"s": "S3", "d": dt.datetime(2000, 1, 3)},
        ],
        columns=["s", "d"],
    )
    t.drop_partitions([["S2", "2000-01-02"], ["S3", "2000-01-03"]])
    time.sleep(1)
    assert t.get_count_rows() == 1
    t.truncate()
    assert t.get_count_rows() == 0
    client.drop_db(TEST_DB)


def test_show():
    client.drop_db(TEST_DB)
    client.create_db(TEST_DB)
    client.create_table_log(TEST_DB, TEST_TABLE, columns=["s String", "d DateTime"])
    print(client.show_process())
    print(client.show_databases())
    print(client.show_tables(TEST_DB))
    print(client.show_create_table(TEST_DB, TEST_TABLE))
    print(client.describe(TEST_DB, TEST_TABLE))
    client.drop_db(TEST_DB)


@_decorator_function
def test_mutation_update(db, table):
    table.update(
        update="""  integer = 10   """,
        where="""  integer = 1   """,
        prevent_parallel_processes=True,
        sleep=1,
    )
    assert [(10,)] == table.select(columns=["integer"], where="integer = 10")


@_decorator_function
def test_mutation_delete(db, table):
    table.delete(where="integer = 3", prevent_parallel_processes=True, sleep=1)
    assert table.get_count_rows() == 2


@_decorator_function
def test_insert_transform_from_table(db, table):
    table_2 = TEST_TABLE + "2"
    table_2 = db.create_table_mergetree(
        table_2,
        columns=["integer String"],
        orders=["integer"]
    )
    table_2.insert_transform_from_table(table.db, table.table)
    data = table_2.select()
    print(data)
    tdata = [i[0] for i in data]
    assert ['1', '2', '3', '3'] == tdata



@_decorator_function
def test_insert_select(db, table):
    # client.insert_select()
    table.insert_select(query="SELECT * FROM {}.{}".format(table.db, table.table))
    table.insert_select(
        query="SELECT string FROM {}.{}".format(table.db, table.table),
        columns=["string"]
    )
    print(
        table.select_df()
    )
    assert table.get_count_rows() == 16



@_decorator_function
def test_select(db, table):
    data = table.select()
    data = table.select(limit=1)
    data = table.select(limit=1, offset=1)
    data = table.select(limit=1, offset=1, columns=["integer"])
    data = table.select(limit=1, offset=1, columns=["integer"], where="string='c'")
    assert data == [(3,)]


@_decorator_function
def test_get_df(db, table):
    if find_spec("pandas"):
        query = "SELECT arrayJoin({0}) as a, arrayJoin({0}) as ba".format(
            list(range(3))
        )
        r = client.get_df(query)
        print(r)

        r = table.select_df()
        print(r)

        r = table.select_df(limit=1)
        r = table.select_df(limit=1, offset=1)
        r = table.select_df(limit=1, offset=1, columns=["string"])
        print(r)


def test_get_empty_df():
    if find_spec("pandas"):
        r = client.get_df("SELECT 1 as a WHERE a > 1")
        print(r)


def test_readme_df():
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
    client.drop_db(TEST_DB)


def test_readme():
    client.drop_db(TEST_DB)
    client.drop_table(TEST_DB, TEST_TABLE)
    client.drop_table(TEST_DB, TEST_TABLE + "_new")
    r = client.test_connection()
    print(r)
    ##
    ##
    ##
    r = client.show_databases()
    print("список баз данных:", r)

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

    ### Сущность DB
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

    ### Сущность Table
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

    data = table.select(limit=1, columns=["s"], where="s = '2'")
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
