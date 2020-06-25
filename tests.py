import datetime as dt
import time
from importlib.util import find_spec

import yaml

from clickhousepy import Client

with open("config.yml", "r") as stream:
    data_loaded = yaml.safe_load(stream)

client = Client(
    host=data_loaded["host"], user=data_loaded["user"], password=data_loaded["password"]
)

TEST_DB = "default"
TEST_TABLE = "__test"


def test_get_methods():
    client.drop_table(TEST_DB, TEST_TABLE)
    client.create_table_mergetree(
        TEST_DB,
        TEST_TABLE,
        columns=["s String", "d DateTime"],
        orders=["s"],
        partition=["s"],
    )
    t = client.Table(TEST_DB, TEST_TABLE)
    t.insert(
        [
            {"s": "1", "d": dt.datetime(2000, 1, 1)},
            {"s": "2", "d": dt.datetime(2000, 1, 2)},
            {"s": "3", "d": dt.datetime(2000, 1, 3)},
        ],
        columns=["s", "d"],
    )
    print(t.get_count_rows())
    print(t.get_min_date(date_column_name="d"))
    print(t.get_max_date(date_column_name="d"))
    t.drop_table()
    assert t.exists() == False


def test_DB():
    TEST_DB = "__clickhousepy_test_db"
    db = client.create_db(TEST_DB)
    t = db.create_table_mergetree(
        TEST_TABLE, columns=["s String", "d DateTime"], orders=["s"], partition=["s"]
    )
    db.drop_table(TEST_TABLE)
    db.drop_db()
    assert t.exists() == False


def test_Table():
    client.drop_table(TEST_DB, TEST_TABLE)
    t = client.create_table_mergetree(
        TEST_DB,
        TEST_TABLE,
        columns=["s String", "d DateTime"],
        orders=["s"],
        partition=["s"],
    )
    t.insert(
        [
            {"s": "1", "d": dt.datetime(2000, 1, 1)},
            {"s": "2", "d": dt.datetime(2000, 1, 2)},
            {"s": "3", "d": dt.datetime(2000, 1, 3)},
        ],
        columns=["s", "d"],
    )
    print("\n", t.execute("select 1"))
    print("\n", t.query("select * from {db}.{table}"))
    t.drop_table()
    assert t.exists() == False


def test_copy_data():
    test_table2 = TEST_TABLE + "_copy"
    client.drop_table(TEST_DB, TEST_TABLE)
    client.drop_table(TEST_DB, test_table2)
    t = client.create_table_mergetree(
        TEST_DB,
        TEST_TABLE,
        columns=["s String", "d DateTime"],
        orders=["s"],
        partition=["s"],
    )
    t.insert(
        [
            {"s": "S1", "d": dt.datetime(2000, 1, 1)},
            {"s": "S2", "d": dt.datetime(2000, 1, 2)},
            {"s": "S3", "d": dt.datetime(2000, 1, 3)},
            {"s": "S3", "d": dt.datetime(2000, 1, 3)},
        ],
        columns=["s", "d"],
    )
    t2 = t.copy_table(TEST_DB, test_table2, return_new_table=True)
    is_identic = t2.copy_data_from(
        TEST_DB, TEST_TABLE, where="s != 'S3' ", columns=("s",)
    )
    print(t2.select(columns=("s",)))
    assert True == is_identic
    t2.truncate()
    # Проверка удаления дублирующихся строк.
    t2.copy_data_from(
        TEST_DB, TEST_TABLE, columns=("s",), distinct=True
    )
    assert 3 == t2.get_count_rows()

    t.drop_table()
    t2.drop_table()


def test_drop_partitions_str():
    client.drop_table(TEST_DB, TEST_TABLE)
    client.create_table_mergetree(
        TEST_DB,
        TEST_TABLE,
        columns=["s String", "d Date"],
        orders=["s"],
        partition=["s"],
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
    t.drop_partitions([["S2"], ["S3"]])
    time.sleep(1)
    assert t.get_count_rows() == 1
    t.truncate()
    assert t.get_count_rows() == 0
    t.drop_table()


def test_drop_partitions_list():
    client.drop_table(TEST_DB, TEST_TABLE)
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
    t.drop_table()


def test_show():
    client.create_table_log(TEST_DB, TEST_TABLE, columns=["s String", "d DateTime"])
    print(client.show_process())
    print(client.show_databases())
    print(client.show_tables(TEST_DB))
    print(client.show_create_table(TEST_DB, TEST_TABLE))
    print(client.describe(TEST_DB, TEST_TABLE))
    client.drop_table(TEST_DB, TEST_TABLE)


def test_mutation():
    t = client.Table(TEST_DB, TEST_TABLE)
    t.drop_table()
    client.create_table_mergetree(
        TEST_DB, TEST_TABLE, columns=["s String", "n String"], orders=["s"]
    )
    t.insert(data=[{"s": "--", "n": "6"}, {"s": "111", "n": "0"}])
    mutation_id = t.update(
        update="""  n = '1000'   """,
        where="""  n = '6'   """,
        prevent_parallel_processes=True,
        sleep=1,
    )
    print("start mutation")
    is_mutation_done = client.is_mutation_done(mutation_id)
    print("is mutation?", is_mutation_done)
    print(client.execute("SELECT * FROM {}.{} ".format(TEST_DB, TEST_TABLE)))
    t.delete(where="n = '1000'", prevent_parallel_processes=True, sleep=1)
    print(client.execute("SELECT * FROM {}.{} ".format(TEST_DB, TEST_TABLE)))
    t.drop_table()


def test_insert_transform_from_table():
    test_insert_table = TEST_TABLE + "2"
    client.drop_table(TEST_DB, test_insert_table)
    client.drop_table(TEST_DB, TEST_TABLE)
    t = client.create_table_mergetree(
        TEST_DB, TEST_TABLE, columns=["s String"], orders=["s"]
    )
    t2 = client.create_table_mergetree(
        TEST_DB, test_insert_table, columns=["s Int32"], orders=["s"]
    )
    t.insert([{"s": "--"}, {"s": "111"}])
    t2.insert_transform_from_table(TEST_DB, test_insert_table)
    print(str(t))
    t2.insert_select(
        query=""" SELECT toInt32OrZero(ifNull(toString(s), '')) FROM {}.{} """.format(
            t.db, t.table
        )
    )
    t2.insert_select(query="SELECT s FROM {}.{}".format(t.db, t.table))
    t2.insert_select(query="SELECT s FROM {}.{}".format(t.db, t.table), columns="s")
    t.drop_table()
    t2.drop_table()


def test_get_df():
    if find_spec("pandas"):
        query = "SELECT arrayJoin({0}) as a, arrayJoin({0}) as ba".format(
            list(range(3))
        )
        r = client.get_df(query)
        print(r)

        r = client.get_df(query, columns_names=["col1", "col2"])
        print(r)


def test_get_empty_df():
    if find_spec("pandas"):
        r = client.get_df("SELECT 1 as a WHERE a > 1")
        print(r)
