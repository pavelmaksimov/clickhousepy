import datetime as dt
import time

import yaml

from clickhousepy import Client

with open("config.yml", "r") as stream:
    data_loaded = yaml.safe_load(stream)

client = Client(
    host=data_loaded["host"],
    user=data_loaded["user"],
    password=data_loaded["password"]
)

TEST_DB = 'default'
TEST_TABLE = '__test'


def test():
    client.drop_table(TEST_DB, TEST_TABLE)
    client.create_table_mergetree(
        TEST_DB, TEST_TABLE,
        columns=['s String', 'd DateTime'],
        orders=['s'],
        partition=["s"]
    )
    t = client.Table(TEST_DB, TEST_TABLE)
    print(t.describe())
    t.insert([{"s": "1", "d": dt.datetime(2000, 1, 1)},
              {"s": "2", "d": dt.datetime(2000, 1, 2)},
              {"s": "3", "d": dt.datetime(2000, 1, 3)}],
             columns=["s", "d"])
    print(t.get_count_rows())
    print(t.get_min_date(date_column_name="d"))
    print(t.get_max_date(date_column_name="d"))
    print(t.get("SELECT * FROM {db}.{table}"))
    print(t.get_df("SELECT * FROM {db}.{table}"))
    t.drop_partitions("1")
    t.drop_partitions([["2"], ["3"]])
    time.sleep(1)
    assert t.get_count_rows() == 0
    t.truncate()
    assert t.get_count_rows() == 0
    t.drop_table()
    assert t.exists() == False


def test_optimize_table():
    r = client.create_table_log(TEST_DB, TEST_TABLE, columns=['s String', 'd DateTime'])
    r = client.show_process()
    print(r)
    r = client.show_databases()
    print(r)
    r = client.show_tables(TEST_DB)
    print(r)
    r = client.show_create_table(TEST_DB, TEST_TABLE)
    print(r)
    r = client.drop_table(TEST_DB, TEST_TABLE)


def test_mutation():
    t = client.Table(TEST_DB, TEST_TABLE)
    t.drop_table()
    client.create_table_mergetree(TEST_DB, TEST_TABLE, columns=['s String', 'n String'], orders=['s'])
    t.insert(data=[{"s": "--", "n": "6"}, {"s": "111", "n": "0"}])
    mutation_id = t.update(update='''  n = '1000'   ''', where='''  n = '6'   ''', prevent_parallel_processes=True,
                           sleep=1)
    print("start mutation")
    is_mutation_done = client.is_mutation_done(mutation_id)
    print('is mutation?', is_mutation_done)
    print(client.execute('SELECT * FROM {}.{} '.format(TEST_DB, TEST_TABLE)))
    t.delete(where="n = '1000'", prevent_parallel_processes=True, sleep=1)
    print(client.execute('SELECT * FROM {}.{} '.format(TEST_DB, TEST_TABLE)))
    t.drop_table()


def test_insert_transform_from_table():
    test_insert_table = TEST_TABLE + '2'
    client.drop_table(TEST_DB, test_insert_table)
    client.drop_table(TEST_DB, TEST_TABLE)
    t = client.create_table_mergetree(TEST_DB, TEST_TABLE, columns=['s String'], orders=['s'])
    t2 = client.create_table_mergetree(TEST_DB, test_insert_table, columns=['s Int32'], orders=['s'])
    t.insert([{"s": "--"}, {"s": "111"}])
    t2.insert_transform_from_table(TEST_DB, test_insert_table)
    print(str(t))
    t2.insert_select(query=''' SELECT toInt32OrZero(ifNull(toString(s), '')) FROM {}.{} '''.format(t.db, t.table))
    t2.insert_select(query='SELECT s FROM {}.{}'.format(t.db, t.table))
    t2.insert_select(query='SELECT s FROM {}.{}'.format(t.db, t.table), columns='s')
    t.drop_table()
    t2.drop_table()


def test_get_df():
    r = client.get_df("SELECT arrayJoin({0}) as a, arrayJoin({0}) as ba".format(list(range(10))))
    print(r.columns)
    print(r)


def test_get_df2():
    r = client.get_df("SELECT 1 as a WHERE a > 1")
    print(r)
