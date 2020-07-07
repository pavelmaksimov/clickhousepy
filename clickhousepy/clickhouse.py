# -*- coding: utf-8 -*-
import logging
import time

from clickhouse_driver import Client as ChClient

logging.basicConfig(level=logging.INFO)


class Client(ChClient):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        super().__init__(*args, **kwargs)

    def DB(self, db):
        return DB(self, db, *self.args, **self.kwargs)

    def Table(self, db, table):
        return Table(self, db, table, *self.args, **self.kwargs)

    def test_connection(self, **kwargs):
        r = bool(self.execute("SELECT 1", **kwargs)[0][0])
        if r:
            logging.info("База данных доступна")
        else:
            logging.error("База данных НЕ доступна")
        return r

    def truncate(self, db, table, **kwargs):
        return self.execute("TRUNCATE TABLE {}.{}".format(db, table), **kwargs)

    def exists(self, db, table, **kwargs):
        r = self.execute("EXISTS TABLE {}.{}".format(db, table), **kwargs)
        return bool(r[0][0])

    def describe(self, db, table, **kwargs):
        return self.execute("DESCRIBE TABLE {}.{}".format(db, table), **kwargs)

    def rename(self, db, table, new_db, new_table, **kwargs):
        self.execute(
            """RENAME TABLE {}.{} TO {}.{}""".format(db, table, new_db, new_table),
            **kwargs,
        )
        return self.Table(new_db, new_table)

    def create_db(self, db, if_not_exists=True, **kwargs):
        exists = "IF NOT EXISTS" if if_not_exists else ""
        self.execute("CREATE DATABASE {} {}".format(exists, db), **kwargs)
        return DB(self, db, *self.args, **self.kwargs)

    def create_table_mergetree(
        self,
        db,
        table,
        columns,
        orders,
        partition=None,
        sample=None,
        primary_key=None,
        ttl=None,
        if_not_exists=True,
        extra_before_settings="",
        engine="MergeTree",
        settings=None,
        **kwargs,
    ):
        """

        :param db: str
        :param table: str
        :param columns: list, list(list) : [...,'Name String ...'] or [...,("Name", "String",...)]
        :param orders: list
        :param partition: list
        :param sample: list
        :param primary_key: list
        :param ttl: str
        :param if_not_exists: bool
        :param extra_before_settings: str : будет вставлено перед SETTINGS
        :param engine: str
        :param settings: str
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: Table
        """
        if primary_key is not None:
            primary_key = ", ".join(primary_key)
            primary_key = "PRIMARY KEY ({})\n".format(primary_key)
        else:
            primary_key = ""

        if partition is not None:
            partition = ", ".join(partition)
            partition = "PARTITION BY ({})\n".format(partition)
        else:
            partition = ""

        if sample is not None:
            sample = ", ".join(map(str, sample))
            sample = "SAMPLE BY ({})\n".format(sample)
        else:
            sample = ""

        if not columns:
            raise AttributeError("Отсутствуют значения в переменной columns")
        if isinstance(columns[0], (list, tuple)):
            columns = [" ".join(i) for i in columns]

        columns = ",\n\t".join(columns)
        orders = ", ".join(orders)
        settings = "SETTINGS {}\n".format(settings) if settings is not None else ""
        ttl = "TTL {}\n".format(ttl) if ttl is not None else ""
        exists = "IF NOT EXISTS" if if_not_exists else ""

        query = (
            "CREATE TABLE {exists} {db}.{table} "
            "(\n\t{columns}\n)\n"
            "ENGINE = {engine}\n"
            "ORDER BY ({orders})\n"
            "{partition}"
            "{primary_key}"
            "{sample}"
            "{ttl}"
            "{extra} {settings}"
        )
        query = query.format(
            exists=exists,
            columns=columns,
            partition=partition,
            orders=orders,
            sample=sample,
            db=db,
            table=table,
            primary_key=primary_key,
            ttl=ttl,
            settings=settings,
            extra=extra_before_settings,
            engine=engine,
        )
        self.execute(query, **kwargs)
        return self.Table(db, table)

    def create_table_log(
        self,
        db,
        table,
        columns,
        if_not_exists=True,
        temporary=False,
        engine="StripeLog",
        type_log_table=None,
        **kwargs,
    ):
        """

        :param db: str
        :param table: str
        :param columns: list, list(list) : [...,'Name String ...'] or [...,("Name", "String",...)]
        :param if_not_exists: bool
        :param temporary: bool
        :param engine: str
        :param type_log_table: : параметр более не поддерживается. Но остался для совместимости.
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: Table
        """
        if not columns:
            raise AttributeError("Отсутствуют значения в переменной columns")
        if isinstance(columns[0], (list, tuple)):
            columns = [" ".join(i) for i in columns]

        columns = ",\n\t".join(columns)
        exists = "IF NOT EXISTS" if if_not_exists else ""
        temporary = "TEMPORARY" if temporary else ""

        query = (
            "CREATE {temporary} TABLE {exists} {db}.{table} "
            "(\n\t{columns}\n)\n"
            "ENGINE = {engine}"
        )
        query = query.format(
            temporary=temporary,
            exists=exists,
            columns=columns,
            db=db,
            table=table,
            engine=type_log_table or engine,
        )
        self.execute(query, **kwargs)
        return self.Table(db, table)

    def copy_table(self, db, table, new_db, new_table, if_not_exists=True, **kwargs):
        exists = "IF NOT EXISTS" if if_not_exists else ""
        query = "CREATE TABLE {} {}.{} as {}.{}"
        query = query.format(exists, new_db, new_table, db, table)
        self.execute(query, **kwargs)
        return self.Table(new_db, new_table)

    def copy_data(
        self,
        from_db,
        from_table,
        to_db,
        to_table,
        where=None,
        columns=None,
        distinct=False,
        **kwargs,
    ):
        """
        Копирование данных. Целевая таблица создается автоматически, если отсутствует.
        После копирования проверяется, кол-во строк, если не включен параметр distinct,
        который удаляет дубликаты строк.

        :param from_db: str
        :param from_table: str
        :param to_db: str
        :param to_table: str
        :param where: str
        :param columns: list
        :param distinct: bool : Будет удалять дублирующиеся строки при копировании
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: True, False и None при distinct=True
        """
        if not self.exists(to_db, to_table, **kwargs):
            self.copy_table(from_db, from_table, to_db, to_table, **kwargs)
        where_ = "WHERE {}".format(where) if where else ""
        if columns and isinstance(columns, (list, tuple)):
            columns_ = ",\n\t".join(columns)
            if distinct:
                from_columns = "DISTINCT {}".format(columns_)
            else:
                from_columns = columns_
            columns = "(\n\t{}\n)\n".format(columns_)
        elif columns is None:
            columns = ""
            if distinct:
                from_columns = "DISTINCT *"
            else:
                from_columns = "*"
        else:
            raise TypeError("параметр columns принимается только, как list и tuple")

        self.execute(
            "INSERT INTO {}.{} {} SELECT {} FROM {}.{} {}".format(
                to_db, to_table, columns, from_columns, from_db, from_table, where_
            ),
            **kwargs,
        )
        count_rows1 = self.get_count_rows(from_db, from_table, where=where)
        count_rows2 = self.get_count_rows(to_db, to_table, where=where)
        if not distinct:
            is_identic = count_rows1 == count_rows2
            if not is_identic:
                logging.warning(
                    "Кол-во строк, после копирования данных НЕ СОВПАДАЮТ. "
                    "Строк в таблице источнике: {}, скопировано строк {}.".format(
                        count_rows1, count_rows2
                    )
                )
            else:
                logging.info("Скопировано строк: {}".format(count_rows1))
            return is_identic
        else:
            logging.info(
                "Кол-во строк в таблице источнике: {}. "
                "Кол-во скопированных строк без дубликатов: {}.".format(
                    count_rows1, count_rows2
                )
            )
            return None

    def drop_db(self, db, if_exists=True, **kwargs):
        exists = "IF EXISTS" if if_exists else ""
        return self.execute("DROP DATABASE {} {}".format(exists, db), **kwargs)

    def drop_table(self, db, table, if_exists=True, **kwargs):
        exists = "IF EXISTS" if if_exists else ""
        return self.execute("DROP TABLE {} {}.{}".format(exists, db, table), **kwargs)

    def drop_partitions(self, db, table, partitions, **kwargs):
        """
        :param db: str
        :param table: str
        :param partitions: str or int or list(list)
             Если ключ партиции состоит из одного столбца,
             то можно передать, как str или int, а иначе, как list(list).
             Примеры:  '2018-01-01' или 123 или [...,[12345, '2018-01-01']]
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: None
        """

        def _drop_partition(partition_key):
            # Преобразование списка в строку.
            partition_key = [
                i if isinstance(i, int) else "'{}'".format(i) for i in partition_key
            ]
            partition_key = ", ".join(map(str, partition_key))
            query = "ALTER TABLE {}.{} DROP PARTITION ({})".format(
                db, table, partition_key
            )
            self.execute(query, **kwargs)

        if not isinstance(partitions, list):
            partitions = [[str(partitions)]]

        list(map(_drop_partition, partitions))

    def is_mutation_done(self, mutation_id, **kwargs):
        query = "SELECT is_done FROM system.mutations WHERE mutation_id='{}' "
        query = query.format(mutation_id)
        r = self.execute(query, **kwargs)
        return r[0][0] if r else None

    def _get_last_mutation_id(self, type_mutation, db, table, command, **kwargs):
        command = command.replace("'", "\\'")
        command = command[command.upper().find(type_mutation) :]
        query = (
            "SELECT mutation_id "
            "FROM system.mutations "
            "WHERE database='{}' AND table='{}' AND command='{}' "
            "ORDER BY create_time DESC"
        ).format(db, table, command)
        r = self.execute(query, **kwargs)
        return r[0][0] if r else None

    def get_mutations(
        self,
        limit=10,
        offset=0,
        columns=None,
        where=None,
        order_by="create_time DESC",
        **kwargs,
    ):
        """
        Выводит строки таблицы мутаций.

        :param limit: int
        :param offset: int
        :param columns: list, tuple, None
        :param where: str
        :param order_by: str
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: list
        """
        return self.select(
            "system", "mutations", limit, offset, columns, where, order_by, **kwargs
        )

    def get_mutations_df(
        self,
        limit=10,
        offset=0,
        columns=None,
        where=None,
        order_by="create_time DESC",
        **kwargs,
    ):
        """
        Выводит строки таблицы мутаций в формате DataFrame.

        :param limit: int
        :param offset: int
        :param columns: list, tuple, None
        :param where: str
        :param order_by: str
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: DataFrame
        """
        return self.select_df(
            "system", "mutations", limit, offset, columns, where, order_by, **kwargs
        )

    def delete(
        self, db, table, where, prevent_parallel_processes=False, sleep=1, **kwargs
    ):
        """

        :param db: str
        :param table: str
        :param where: str
        :param prevent_parallel_processes: Запрос будет сделан, когда завершатся все мутации таблицы.
        :param sleep: Интервал проверки завершения всех мутаций таблицы.
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: None
        """
        query = "ALTER TABLE {}.{} DELETE WHERE {}".format(db, table, where)

        if prevent_parallel_processes:
            while True:
                r = self.get_count_run_mutations(db, table)
                if r == 0:
                    self.execute(query, **kwargs)
                    return self._get_last_mutation_id("DELETE", db, table, query)
                else:
                    time.sleep(sleep)
        else:
            self.execute(query, **kwargs)
            return self._get_last_mutation_id("DELETE", db, table, query)

    def update(
        self,
        db,
        table,
        update,
        where,
        prevent_parallel_processes=False,
        sleep=1,
        **kwargs,
    ):
        """

        :param db: str
        :param table: str
        :param update: str
        :param where: str
        :param prevent_parallel_processes: Запрос будет сделан, когда завершатся все мутации таблицы.
        :param sleep: Интервал проверки завершения всех мутаций таблицы.
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: None
        """
        query = """ALTER TABLE {db}.{t} UPDATE {update} WHERE {where}"""
        query = query.format(db=db, t=table, update=update, where=where)

        if prevent_parallel_processes:
            while True:
                r = self.get_count_run_mutations(db, table)
                if r == 0:
                    self.execute(query, **kwargs)
                    return self._get_last_mutation_id("UPDATE", db, table, query)
                else:
                    time.sleep(sleep)
        else:
            self.execute(query, **kwargs)
            return self._get_last_mutation_id("UPDATE", db, table, query)

    def get_count_run_mutations(self, db, table, **kwargs):
        query = (
            "SELECT count() "
            "FROM system.mutations "
            "WHERE database='{}' AND table='{}' AND is_done=0"
        ).format(db, table)
        r = self.execute(query, **kwargs)
        return r[0][0]

    def get_min_date(self, db, table, where=None, date_column_name="Date", **kwargs):
        where = "WHERE " + where if where else ""
        query = "SELECT min({}) FROM {}.{} {}".format(
            date_column_name, db, table, where
        )
        return self.execute(query, **kwargs)[0][0]

    def get_max_date(self, db, table, where=None, date_column_name="Date", **kwargs):
        where = "WHERE " + where if where else ""
        query = "SELECT max({}) FROM {}.{} {}".format(
            date_column_name, db, table, where
        )
        return self.execute(query, **kwargs)[0][0]

    def get_count_rows(self, db, table, where=None, **kwargs):
        where = "WHERE " + where if where else ""
        query = "SELECT count() FROM {}.{} {}".format(db, table, where)
        return self.execute(query, **kwargs)[0][0]

    def optimize_table(self, db, table, **kwargs):
        query = "OPTIMIZE TABLE {}.{}".format(db, table)
        return self.execute(query, **kwargs)

    def reload_dictionary(self, dictionary_name, **kwargs):
        query = "RELOAD DICTIONARY {}".format(dictionary_name)
        return self.execute(query, **kwargs)

    def reload_dictionaries(self, **kwargs):
        return self.execute("SYSTEM RELOAD DICTIONARIES", **kwargs)

    def check_table(self, db, table, **kwargs):
        query = "CHECK TABLE {}.{}".format(db, table)
        return self.execute(query, **kwargs)[0][0]

    def attach(self, db, table, if_exists=True, cluster=None, **kwargs):
        exists = "IF EXISTS" if if_exists else ""
        cluster = "ON CLUSTER {}".format(cluster) if cluster else ""
        query = "ATTACH TABLE {} {}.{} {}".format(exists, db, table, cluster)
        return self.execute(query, **kwargs)

    def detach(self, db, table, if_exists=True, cluster=None, **kwargs):
        exists = "IF EXISTS" if if_exists else ""
        cluster = "ON CLUSTER {}".format(cluster) if cluster else ""
        query = "DETACH TABLE {} {}.{} {}".format(exists, db, table, cluster)
        return self.execute(query, **kwargs)

    def show_databases(self, **kwargs):
        return [i[0] for i in self.execute("SHOW DATABASES", **kwargs)]

    def show_tables(self, db=None, like=None, **kwargs):
        db = "FROM {}".format(db) if db else ""
        like = "LIKE '{}'" if like else ""
        r = self.execute("SHOW TABLES {} {}".format(db, like), **kwargs)
        return [i[0] for i in r]

    def show_process(self, **kwargs):
        return self.execute("SHOW PROCESSLIST", **kwargs)

    def show_create_table(self, db, table, **kwargs):
        query = "SHOW CREATE TABLE {}.{}".format(db, table)
        return self.execute(query, **kwargs)[0][0]

    @staticmethod
    def _transform_data_type_sql(name, data_type):
        if data_type.find("Array") > -1:
            return name
        elif data_type.find("Int") > -1 or data_type.find("Float") > -1:
            if data_type.find("Nullable") > -1:
                return "to{}OrNull(toString({}))".format(data_type, name)
            else:
                return "to{}OrZero(ifNull(toString({}), ''))".format(data_type, name)
        elif data_type == "String":
            return "toString({})".format(name)
        else:
            return name

    def insert_transform_from_table(
        self, from_db, from_table, to_db, to_table, **kwargs
    ):
        """
        Перенос из одной таблицы в другую идентичную таблицу
        с принудительным приведением типов столбцов
        по типам столбцов целевой таблицы.

        :param from_db: str
        :param from_table: str
        :param to_db: str
        :param to_table: str
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: None
        """
        column_data = self.describe(to_db, to_table)
        columns_list = []
        for i in column_data:
            if i[2] not in ("ALIAS", "MATERIALIZED"):
                column_name, column_type = i[0], i[1]
                c = self._transform_data_type_sql(column_name, column_type)
                columns_list.append(c)
        columns_str = ",\n".join(columns_list)
        sql = "INSERT INTO {}.{} SELECT {} FROM {}.{}"
        sql = sql.format(to_db, to_table, columns_str, from_db, from_table)
        return self.execute(sql, **kwargs)

    def insert(self, db, table, data, columns=None, **kwargs):
        columns_str = "({})".format(",".join(columns)) if columns else ""
        query = "INSERT INTO {}.{} {} VALUES".format(db, table, columns_str)
        return self.execute(query, data, **kwargs)

    def insert_select(self, db, table, query, columns=None, **kwargs):
        if columns:
            columns_str = ",".join(columns)
            columns_str = "({})".format(columns_str)
        else:
            columns_str = ""
        query = "INSERT INTO {}.{} {} {}".format(db, table, columns_str, query)
        return self.execute(query, **kwargs)

    def get_df(self, query, columns_names=None, dtype=None, **kwargs):
        """

        :param query: str
        :param columns_names: list, tuple : названия столбцов для DataFrame
        :param dtype: object type : параметр передается при создании dataframe для определения типа столбцов
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: DataFrame
        """
        import pandas as pd  # pylint: disable=import-error

        result = self.execute(query, **kwargs) or [[]]
        return pd.DataFrame(data=result, columns=columns_names, dtype=dtype)

    def _generate_select(
        self, db, table, limit=10, offset=0, columns=None, where=None, order_by=None
    ):
        """Формирование запроса."""
        where = "WHERE {}\n".format(where) if where else ""
        order_by = "ORDER BY {}\n".format(order_by) if order_by else ""
        if columns and isinstance(columns, (tuple, list)):
            columns_ = ",\n\t".join(columns)
            columns_ = "\n\t{}\n".format(columns_)
        elif columns is None:
            columns_ = "*"
        else:
            raise TypeError("параметр columns принимается только, как list и tuple")

        query = "SELECT {}\nFROM {}.{}\n{}{}LIMIT {} OFFSET {}".format(
            columns_, db, table, where, order_by, limit, offset
        )
        return query

    def select(
        self,
        db,
        table,
        limit=10,
        offset=0,
        columns=None,
        where=None,
        order_by=None,
        **kwargs,
    ):
        """
        Выводит строки таблицы.

        :param db: str
        :param table: str
        :param limit: int
        :param offset: int
        :param columns: list, tuple, None
        :param where: str
        :param order_by: str
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: list
        """
        query = self._generate_select(
            db, table, limit, offset, columns, where, order_by
        )
        return self.execute(query, **kwargs)

    def select_df(
        self,
        db,
        table,
        limit=10,
        offset=0,
        columns=None,
        where=None,
        order_by=None,
        dtype=None,
        **kwargs,
    ):
        """
        Выводит строки таблицы.

        :param db: str
        :param table: str
        :param limit: int
        :param offset: int
        :param columns: list, tuple, None
        :param where: str
        :param order_by: str
        :param dtype: object type : параметр передается при создании dataframe для определения типа столбцов датафрейма
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: DataFrame
        """
        query = self._generate_select(
            db, table, limit, offset, columns, where, order_by
        )
        if columns is None:
            # Если названия столбцов не переданы, возьмет их из описания таблицы.
            columns_data = self.describe(db, table, **kwargs)
            columns = [i[0] for i in columns_data]
        return self.get_df(query, columns_names=columns, dtype=dtype, **kwargs)


class DB(ChClient):
    def __init__(self, client, db, *args, **kwargs):
        self._client = client or Client
        self.db = db
        super().__init__(*args, **kwargs)

    def show_tables(self, like=None, **kwargs):
        return self._client.show_tables(self.db, like=like, **kwargs)

    def drop_db(self, if_exists=True, **kwargs):
        return self._client.drop_db(self.db, if_exists=if_exists, **kwargs)

    def drop_table(self, table, if_exists=True, **kwargs):
        return self._client.drop_table(self.db, table, if_exists, **kwargs)

    def create_table_mergetree(
        self,
        table,
        columns,
        orders,
        partition=None,
        sample=None,
        primary_key=None,
        ttl=None,
        if_not_exists=True,
        extra_before_settings="",
        engine="MergeTree",
        settings=None,
        **kwargs,
    ):
        """

        :param table: str
        :param columns: list, list(list) : [...,'Name String ...'] or [...,("Name", "String",...)]
        :param orders: list
        :param partition: list
        :param sample: list
        :param primary_key: list
        :param ttl: str
        :param if_not_exists: bool
        :param extra_before_settings: str : будет вставлено перед SETTINGS
        :param engine: str
        :param settings: str
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: Table
        """
        return self._client.create_table_mergetree(
            self.db,
            table,
            columns,
            orders,
            partition=partition,
            sample=sample,
            primary_key=primary_key,
            ttl=ttl,
            if_not_exists=if_not_exists,
            extra_before_settings=extra_before_settings,
            engine=engine,
            settings=settings,
            **kwargs,
        )

    def create_table_log(
        self,
        table,
        columns,
        if_not_exists=True,
        temporary=False,
        engine="StripeLog",
        type_log_table=None,
        **kwargs,
    ):
        """

        :param table: str
        :param columns: list, list(list) : [...,'Name String ...'] or [...,("Name", "String",...)]
        :param if_not_exists: bool
        :param temporary: bool
        :param engine: str
        :param type_log_table: : параметр более не поддерживается. Но остался для совместимости.
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: Table
        """
        return self._client.create_table_log(
            self.db,
            table,
            columns,
            if_not_exists=if_not_exists,
            temporary=temporary,
            engine=engine,
            type_log_table=type_log_table,
            **kwargs,
        )


class Table(ChClient):
    def __init__(self, client, db, table, *args, **kwargs):
        self._client = client or Client
        self.db = db
        self.table = table
        super().__init__(*args, **kwargs)

    def select(self, limit=10, offset=0, columns=None, where=None, **kwargs):
        """
        Выводит строки таблицы.

        :param limit: int
        :param offset: int
        :param columns: list, tuple, None
        :param where: str
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: list
        """
        return self._client.select(
            self.db, self.table, limit, offset, columns, where, **kwargs
        )

    def select_df(
        self, limit=10, offset=0, columns=None, where=None, dtype=None, **kwargs
    ):
        """
        Выводит строки таблицы.

        :param limit: int
        :param offset: int
        :param columns: list, tuple, None
        :param where: str
        :param dtype: object type : параметр передается при создании dataframe для определения типа столбцов датафрейма
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: DataFrame
        """
        return self._client.select_df(
            self.db, self.table, limit, offset, columns, where, dtype, **kwargs
        )

    def insert(self, data, columns=None, **kwargs):
        return self._client.insert(self.db, self.table, data, columns, **kwargs)

    def insert_select(self, query, columns=None, **kwargs):
        return self._client.insert_select(self.db, self.table, query, columns, **kwargs)

    def insert_transform_from_table(self, from_db, from_table, **kwargs):
        """
        Перенос из одной таблицы в другую идентичную таблицу
        с принудительным приведением типов столбцов
        по типам столбцов целевой таблицы.
        :param from_db: str
        :param from_table: str
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: None
        """
        return self._client.insert_transform_from_table(
            from_db, from_table, self.db, self.table, **kwargs
        )

    def exists(self, **kwargs):
        return self._client.exists(self.db, self.table, **kwargs)

    def rename(self, new_db, new_table, **kwargs):
        t = self._client.rename(self.db, self.table, new_db, new_table, **kwargs)
        self.db, self.table = new_db, new_table
        return t

    def truncate(self, **kwargs):
        return self._client.truncate(self.db, self.table, **kwargs)

    def describe(self, **kwargs):
        return self._client.describe(self.db, self.table, **kwargs)

    def delete(self, where, prevent_parallel_processes=False, sleep=1, **kwargs):
        """

        :param where: str
        :param prevent_parallel_processes: bool : Запрос будет сделан, когда завершатся все мутации таблицы.
        :param sleep: int : Интервал проверки завершения всех мутаций таблицы.
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: None
        """
        return self._client.delete(
            db=self.db,
            table=self.table,
            where=where,
            prevent_parallel_processes=prevent_parallel_processes,
            sleep=sleep,
            **kwargs,
        )

    def update(
        self, update, where, prevent_parallel_processes=False, sleep=1, **kwargs
    ):
        """

        :param update: str
        :param where: str
        :param prevent_parallel_processes: bool : Запрос будет сделан, когда завершатся все мутации таблицы.
        :param sleep: int : Интервал проверки завершения всех мутаций таблицы.
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: None
        """
        return self._client.update(
            db=self.db,
            table=self.table,
            update=update,
            where=where,
            prevent_parallel_processes=prevent_parallel_processes,
            sleep=sleep,
            **kwargs,
        )

    def copy_data_from(
        self, from_db, from_table, where=None, columns=None, distinct=False, **kwargs
    ):
        """
        Копирование данных. Целевая таблица создается автоматически, если отсутствует.
        После копирования проверяется, кол-во строк, если не включен параметр distinct,
        который удаляет дубликаты строк.

        :param from_db: str
        :param from_table: str
        :param where: str
        :param columns: list
        :param distinct: bool : Будет удалять дублирующиеся строки при копировании
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: True, False и None при distinct=True
        """
        return self._client.copy_data(
            from_db, from_table, self.db, self.table, where, columns, distinct, **kwargs
        )

    def get_count_rows(self, where=None, **kwargs):
        return self._client.get_count_rows(self.db, self.table, where=where, **kwargs)

    def get_min_date(self, where=None, date_column_name="Date", **kwargs):
        return self._client.get_min_date(
            self.db, self.table, where, date_column_name, **kwargs
        )

    def get_max_date(self, where=None, date_column_name="Date", **kwargs):
        return self._client.get_max_date(
            self.db, self.table, where, date_column_name, **kwargs
        )

    def get_count_run_mutations(self, **kwargs):
        return self._client.get_count_run_mutations(self.db, self.table, **kwargs)

    def copy_table(self, new_db, new_table, return_new_table=False, **kwargs):
        """

        :param new_db: str
        :param new_table: str
        :param return_new_table: Возвратит новый класс Table
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: None, Table
        """
        Table = self._client.copy_table(
            self.db, self.table, new_db, new_table, **kwargs
        )
        if return_new_table:
            return Table
        return None

    def optimize_table(self, **kwargs):
        return self._client.optimize_table(self.db, self.table, **kwargs)

    def check_table(self, **kwargs):
        return self._client.check_table(self.db, self.table, **kwargs)

    def drop_table(self, if_exists=True, **kwargs):
        return self._client.drop_table(
            self.db, self.table, if_exists=if_exists, **kwargs
        )

    def drop_partitions(self, partitions, **kwargs):
        """
        :param partitions: str or int or list(list)
             Если ключ партиции состоит из одного столбца,
             то можно передать, как str или int, а иначе, как list(list).
             Примеры:  '2018-01-01' или 123 или [...,[12345, '2018-01-01']]
        :param kwargs: параметры принимаемые библиотекой clickhouse_driver
        :return: None
        """
        self._client.drop_partitions(
            self.db, self.table, partitions=partitions, **kwargs
        )

    def attach(self, if_exists=True, cluster=None, **kwargs):
        return self._client.attach(self.db, self.table, if_exists, cluster, **kwargs)

    def detach(self, if_exists=True, cluster=None, **kwargs):
        return self._client.detach(self.db, self.table, if_exists, cluster, **kwargs)

    def show_create_table(self, **kwargs):
        return self._client.show_create_table(self.db, self.table, **kwargs)
