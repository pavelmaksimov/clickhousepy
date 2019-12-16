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
    client.create_table_mergetree(
        TEST_DB, TEST_TABLE,
        columns=['s String', 'd DateTime'],
        orders=['s']
    )
    t = client.Table(TEST_DB, TEST_TABLE)
    t.insert([{"s": "--"}, {"s": "111"}], columns=["s"])
    print(t.count_rows())
    print(t.min_date(date_column_name="d"))
    t.truncate()
    print(t.count_rows())
    t.drop()
    print(t.exists())


def test_create_table_mergetree():
    r = client.create_table_mergetree(TEST_DB, TEST_TABLE, columns=['s String', 'd DateTime'], orders=['s'],
                                      partition=["s"], sample=["s"], primary_key=["s"], ttl="d", if_not_exists=False)
    r = client.drop_table(TEST_DB, TEST_TABLE)
    r = client.create_table_mergetree(TEST_DB, TEST_TABLE, columns=[("s", "String")], orders=["s"], partition=["s"],
                                      sample=["s"])
    r = client.drop_table(TEST_DB, TEST_TABLE)


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


def test_describe():
    r = client.describe('direct', 'bids')
    print(r)


def test_get_min_date():
    r = client.get_min_date('direct', 'bids')
    print(r)


def test_get_max_date():
    r = client.get_max_date('direct', 'bids')
    print(r)


def test_count_rows():
    r = client.get_count_rows(TEST_DB, TEST_TABLE)
    print(f"\n{r}\n")


def test_drop_table():
    r = client.drop_table(TEST_DB, TEST_TABLE)
    print(r)


def test_mutation():
    client.drop_table(TEST_DB, TEST_TABLE)
    client.create_table_mergetree(TEST_DB, TEST_TABLE, columns=['s String', 'n String'], orders=['s'])
    client.execute('INSERT INTO {}.{} VALUES'.format(TEST_DB, TEST_TABLE),
                   [{"s": "--", "n": "6"}, {"s": "111", "n": "0"}])
    mutation_id = client.update(TEST_DB, TEST_TABLE, update='''  n = '1000'   ''', where='''  n = '6'   ''')
    print("start mutation")
    is_mutation_done = client.is_mutation_done(mutation_id)
    print('is mutation?', is_mutation_done)
    print(client.execute('SELECT * FROM {}.{} '.format(TEST_DB, TEST_TABLE)))
    client.drop_table(TEST_DB, TEST_TABLE)


def test_insert_transform_from_table():
    test_insert_table = TEST_TABLE + '2'
    client.drop_table(TEST_DB, test_insert_table)
    client.drop_table(TEST_DB, TEST_TABLE)
    client.create_table_mergetree(TEST_DB, TEST_TABLE, columns=['s String'], orders=['s'])
    client.create_table_mergetree(TEST_DB, test_insert_table, columns=['s Int32'], orders=['s'])
    client.execute('INSERT INTO {}.{} VALUES'.format(TEST_DB, TEST_TABLE),
                   [{"s": "--"}, {"s": "111"}])
    client.insert_transform_from_table(TEST_DB, TEST_TABLE, TEST_DB, test_insert_table)
    client.insert_select(TEST_DB, test_insert_table,
                         query=''' SELECT toInt32OrZero(ifNull(toString(s), '')) FROM to_delete.__test ''')
    client.insert_select(TEST_DB, test_insert_table, query='SELECT s FROM to_delete.__test')
    client.insert_select(TEST_DB, test_insert_table, query='SELECT s FROM to_delete.__test', columns='s')
    client.drop_table(TEST_DB, test_insert_table)
    client.drop_table(TEST_DB, TEST_TABLE)


def test_get_df():
    r = client.get_df("SELECT arrayJoin({0}) as a, arrayJoin({0}) as ba".format(list(range(10))))
    print(r.columns)
    print(r)


def test_get_df2():
    r = client.get_df("SELECT 1 as a WHERE a > 1")
    print(r)
