## Python wrapper for database queries [Clickhouse](https://clickhouse.yandex/)

The wrapper is done around [clickhouse-driver](https://clickhouse-driver.readthedocs.io)

Written in python version 3.5

## Installation
```
pip install clickhousepy
or
pip install clickhousepy[pandas]  (for installation pandas)
```


## Getting Data from Clickhouse in Pandas Dataframe Format
```python
from clickhousepy import Client
import datetime as dt

TEST_DB = "__chpytest12345"
TEST_TABLE = "__chpytest12345"


client.create_db(TEST_DB)
client.create_table_mergetree(
    TEST_DB, TEST_TABLE,
    columns=[("i", "UInt32")], # or ["i UInt32"]
    orders=["i"],
)
client.insert(
    TEST_DB, TEST_TABLE,
    [{"i": 1}, {"i": 2}],
) 
query = "SELECT i FROM {}.{}".format(TEST_DB, TEST_TABLE)
r = client.get_df(query, columns_names=["Col Integer"])
print(r)
```

## Brief documentation of some methods
```python
from clickhousepy import Client
import datetime as dt


TEST_DB = "__chpytest12345"
TEST_TABLE = "__chpytest12345"

client = Client(host="", user="", password="")

r = client.show_databases()
print("list of databases:", r)

client.create_db(TEST_DB)

client.create_table_mergetree(
    TEST_DB, TEST_TABLE,
    columns=[("s", "String")],
    orders=["s"],
)
# Inserting data.
# Read more about it here
# https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#inserting-data
client.insert(
    TEST_DB, TEST_TABLE,
    [{"s": "1"}],
) 

r = client.exists(TEST_DB, TEST_TABLE)
print("does the table exist?", r)

r = client.get_count_rows(TEST_DB, TEST_TABLE)
print("number of lines:", r)

# Any request.
r = client.execute("SELECT * FROM {}.{}".format(TEST_DB, TEST_TABLE))
print(r)
```

### Class DB
```python
db = client.DB(TEST_DB)
r = db.show_tables()
print("list of database tables {}:".format(TEST_DB), r)

db.drop_db()
```

### Class Table 
```python
db = client.create_db(TEST_DB)

table = db.create_table_mergetree(
    TEST_TABLE,
    columns=[("s", "String"), ("t", "String"), ("d", "Date")],
    orders=["d"],
    partition=["s", "d"],
)
# Initialization of an existing table.
# table = client.Table(TEST_DB, TEST_TABLE)

r = table.show_create_table()
print("table creation description", r)

r = table.describe()
print("table columns", r)

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
print("First 10 rows of the table", data)

data = table.select(limit=1, columns=["s"], where="s = 2")
print("Filtered sampling", data)

r = table.get_count_rows()
print("number of lines:", r)

r = table.get_min_date(date_column_name="d")
print("minimum date:", r)

r = table.get_max_date(date_column_name="d")
print("maximum date:", r)

print("deleting partitions")
table.drop_partitions([["3", "2000-01-03"], ["4", "2000-01-04"]])

r = table.get_count_rows()
print("number of lines after deleting partitions:", r)

print("row update mutation")
table.update(update="t = '20' ", where="t = '2' ")

print("row deletion mutation")
table.delete(where="t = '20'")
time.sleep(1)
r = table.get_count_rows()
print("number of lines after mutation of line deletion:", r)

print("clear table")
table.truncate()
r = table.get_count_rows()
print("number of rows after clearing the table:", r)

new_table_name = TEST_TABLE + "_new"
print("rename table {} в {}".format(TEST_TABLE, new_table_name))
table.rename(TEST_DB, new_table_name)

r = client.exists(TEST_DB, TEST_TABLE)
print("does table {} exist?".format(TEST_TABLE), r)

print("drop tables")
table.drop_table()

print("deleting a database")
db.drop_db()
```


### Method of copying data from one table to another with checking the number of rows after copying
```python
client.drop_db(TEST_DB)
db = client.create_db(TEST_DB)
table = db.create_table_mergetree(
    TEST_TABLE,
    columns=[("string", "String"), ("integer", "UInt32"), ("dt", "DateTime")],
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
# The function will return a bool value, whether the number of lines matches or not, after copying.
assert is_identic
```

### A method of copying data from one table to another while removing duplicate rows.
```python
client.drop_db(TEST_DB)
db = client.create_db(TEST_DB)
table = db.create_table_mergetree(
    TEST_TABLE,
    columns=[("string", "String"), ("integer", "UInt32"), ("dt", "DateTime")],
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
# When removing duplicate rows (distinct = True), 
# there will be no check for the number of rows after copying.
table2.copy_data_from(
    TEST_DB, TEST_TABLE,
    columns=["string"],
    distinct=True
)
assert 3 == table2.get_count_rows()
```

## Dependencies
- [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver/)
- [pandas](https://github.com/pandas-dev/pandas) (Optional)

## Author
Pavel Maksimov

You can contact me at
[Telegram](https://teleg.run/pavel_maksimow),
[Facebook](https://www.facebook.com/pavel.maksimow)

Удачи тебе, друг! Поставь звездочку ;)
