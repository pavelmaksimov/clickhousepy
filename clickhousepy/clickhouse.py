# -*- coding: utf-8 -*-
import logging
import time
import datetime as dt

from clickhouse_driver import Client as ChClient

logging.basicConfig(level=logging.INFO)


class Client(ChClient):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        super().__init__(*args, **kwargs)

    def DB(self, db):
        return DB(self, db, *self._args, **self._kwargs)

    def Table(self, db, table):
        return Table(self, db, table, *self._args, **self._kwargs)

    def test_connection(self, **kwargs):
        r = bool(self.execute("SELECT 1", **kwargs)[0][0])
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
            **kwargs
        )
        return self.Table(new_db, new_table)

    def create_db(self, db, if_not_exists=True, **kwargs):
        exists = "IF NOT EXISTS" if if_not_exists else ""
        self.execute("CREATE DATABASE {} {}".format(exists, db), **kwargs)
        return DB(self, db, *self._args, **self._kwargs)

    def _normalize_columns(self, columns):
        if not columns:
            raise Exception("Missing value in columns")

        columns_list = []
        for col in columns:
            try:
                if isinstance(col, (list, tuple)):
                    assert len(col) >= 2
                    columns_list.append(" ".join(col))
                else:
                    assert len(col.split(" ")) >= 2
                    columns_list.append(col)

            except AssertionError:
                raise AssertionError("Not valid column schema for '{}'".format(col))

        return ",\n\t".join(columns_list)

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
        **kwargs
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
        :param extra_before_settings: str : will be inserted before SETTINGS
        :param engine: str
        :param settings: str
        :param kwargs: Parameters accepted by the clickhouse_driver library
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

        columns = self._normalize_columns(columns)
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
        **kwargs
    ):
        """

        :param db: str
        :param table: str
        :param columns: list, list(list) : [...,'Name String ...'] or [...,("Name", "String",...)]
        :param if_not_exists: bool
        :param temporary: bool
        :param engine: str
        :param type_log_table: : Parameter is no longer supported. But remained for compatibility.
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return: Table
        """
        columns = self._normalize_columns(columns)
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
        **kwargs
    ):
        """
        Copying data. The target table is created automatically if missing. After copying,
        the number of rows is checked, if the distinct parameter is not included,
        which removes duplicate rows.

        :param from_db: str
        :param from_table: str
        :param to_db: str
        :param to_table: str
        :param where: str
        :param columns: list
        :param distinct: bool : Will remove duplicate lines when copying
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return: True, False and None with distinct=True
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
            raise TypeError("Columns parameter is accepted only as list and tuple")

        number_rows = self.get_count_rows(from_db, from_table, where=where)
        before = self.get_count_rows(to_db, to_table)

        self.execute(
            "INSERT INTO {}.{} {} SELECT {} FROM {}.{} {}".format(
                to_db, to_table, columns, from_columns, from_db, from_table, where_
            ),
            **kwargs
        )
        after = self.get_count_rows(to_db, to_table)

        if not distinct:
            is_identic = after - before == number_rows
            if not is_identic:
                logging.warning(
                    "The number of lines after copying the data DO NOT MATCH. "
                    "Rows in the source table: {}, rows copied {}.".format(
                        number_rows, after - before
                    )
                )
            else:
                logging.info("Copied lines: {}".format(number_rows))

            return is_identic
        else:
            logging.info(
                "Number of rows in the source table: {}. "
                "Number of copied lines without duplicates: {}.".format(
                    number_rows, after - before
                )
            )

            return None

    def deduplicate_data(self, db, table, where, **kwargs):
        """
        Remove duplicate rows by copying the table and backing up with DISTINCT.

        :param db: str
        :param table: str
        :param where: str
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return: True, False
        """
        copy_table_name = table + "copy_table_for_deduplicate"
        self.copy_table(db, table, db, copy_table_name, **kwargs)

        count_rows_before = self.get_count_rows(db, table, where, **kwargs)
        is_identic_data = self.copy_data(
            db, table, db, copy_table_name, where=where, **kwargs
        )

        if is_identic_data:
            self.delete(
                db, table, where=where, prevent_parallel_processes=True, **kwargs
            )
            self.copy_data(db, copy_table_name, db, table, distinct=True, **kwargs)
            self.drop_table(db, copy_table_name, **kwargs)
            count_rows_after = self.get_count_rows(db, table, where, **kwargs)
            diff = count_rows_before - count_rows_after
            logging.info("Removed duplicate lines: {}".format(diff))

            return True
        else:
            logging.error(
                "The data when copying the table is not identical, start again."
            )
            self.drop_table(db, copy_table_name, **kwargs)

            return False

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
            If the partition key consists of one column,
            then it can be passed as str or int, and otherwise as list (list).
            Examples:  '2018-01-01' or 123 or [...,[12345, '2018-01-01']]
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return:
        """
        if not isinstance(partitions, (list, tuple)):
            partitions = [[str(partitions)]]

        for partition_key in partitions:
            partition_key_serialize = []
            for value in partition_key:
                if isinstance(value, int):
                    partition_key_serialize.append(value)
                elif isinstance(value, dt.date):
                    partition_key_serialize.append(
                        "'{}'".format(value.isoformat())
                    )
                elif isinstance(value, dt.datetime):
                    partition_key_serialize.append(
                        "'{}'".format(value.strftime("%Y-%m-%d %H:%M:%S"))
                    )
                else:
                    partition_key_serialize.append("'{}'".format(value))

            partition = ", ".join(map(str, partition_key_serialize))

            query = "ALTER TABLE {}.{} DROP PARTITION ({})".format(db, table, partition)
            self.execute(query, **kwargs)

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
        dataframe=False,
        **kwargs
    ):
        """
        Displays the rows of the mutation table.

        :param limit: int
        :param offset: int
        :param columns: list, tuple, None
        :param where: str
        :param order_by: str
        :param dataframe: bool : return DataFrame
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return: list
        """
        return self.select(
            "system", "mutations", limit, offset, columns, where, order_by, dataframe, **kwargs
        )

    def delete(
        self, db, table, where, prevent_parallel_processes=False, sleep=1, **kwargs
    ):
        """

        :param db: str
        :param table: str
        :param where: str
        :param prevent_parallel_processes: bool : The request will be made when all mutations on the table are complete.
        :param sleep: int : The interval to check the completion of all mutations in the table.
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return:
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
        **kwargs
    ):
        """

        :param db: str
        :param table: str
        :param update: str
        :param where: str
        :param prevent_parallel_processes: bool : The request will be made when all mutations on the table are complete.
        :param sleep: int : The interval to check the completion of all mutations in the table.
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return:
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
        if "Array" in data_type:
            return name
        elif "Int" in data_type or "Float" in data_type:
            if "Nullable" in data_type:
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
        Transfer from one table to another identical table with forced casting
        of column types according to the types of columns of the target table.

        :param from_db: str
        :param from_table: str
        :param to_db: str
        :param to_table: str
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return:
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

    def insert_via_stage_table(
        self,
        db,
        table,
        data,
        columns=None,
        stage_db="default",
        stage_table=None,
        **kwargs
    ):
        rows = len(data)
        is_identic = False
        if stage_table is None:
            stage_table = "{}_".format(table)

        self.drop_table(stage_db, stage_table, **kwargs)

        try:
            self.copy_table(db, table, stage_db, stage_table, **kwargs)
            self.insert(stage_db, stage_table, data, columns, **kwargs)
            assert rows == self.get_count_rows(stage_db, stage_table, **kwargs)

            is_identic = self.copy_data(
                stage_db, stage_table, db, table, columns=columns, **kwargs
            )
            if not is_identic:
                logging.error("The number of lines is not identical")
        finally:
            self.drop_table(stage_db, stage_table, **kwargs)

            return is_identic

    def get_df(self, query, columns_names=None, dtype=None, **kwargs):
        """

        :param query: str
        :param columns_names: list, tuple : column names for the DataFrame
        :param dtype: object type : a parameter is passed when creating a dataframe to determine the type of columns
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return: DataFrame
        """
        import pandas as pd  # pylint: disable=import-error

        result = self.execute(query, **kwargs) or [[]]
        return pd.DataFrame(data=result, columns=columns_names, dtype=dtype)

    def _generate_select(
        self, db, table, limit=10, offset=0, columns=None, where=None, order_by=None
    ):
        """Formation of a select request."""
        where = "WHERE {}\n".format(where) if where else ""
        order_by = "ORDER BY {}\n".format(order_by) if order_by else ""

        if columns and isinstance(columns, (tuple, list)):
            columns_ = ",\n\t".join(columns)
            columns_ = "\n\t{}\n".format(columns_)
        elif columns is None:
            columns_ = "*"
        else:
            raise TypeError("Columns parameter is accepted only as list and tuple.")

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
        dataframe=False,
        **kwargs
    ):
        """

        :param db: str
        :param table: str
        :param limit: int
        :param offset: int
        :param columns: list, tuple, None
        :param where: str
        :param order_by: str
        :param dataframe: bool : return DataFrame
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return: DataFrame
        """
        query = self._generate_select(
            db, table, limit, offset, columns, where, order_by
        )
        if dataframe:
            if columns is None:
                # If no column names are supplied, it will take them from the table description.
                columns_data = self.describe(db, table, **kwargs)
                columns = [i[0] for i in columns_data if i[2] not in ("ALIAS", "MATERIALIZED")]

            return self.get_df(query, columns_names=columns, **kwargs)
        else:
            return self.execute(query, **kwargs)

    def _alter_table_column(
        self,
        db,
        table,
        method,
        name,
        type=None,
        after=None,
        expr=None,
        codec=None,
        ttl=None,
        if_not_exists_or_if_exists=True,
        on_cluster=False,
        extra=None,
        **kwargs
    ):
        """

        :param db: str
        :param table: str
        :param method: str : ADD|DROP|CLEAR|COMMENT|MODIFY
        :param name: str
        :param type: str, None
        :param after: str
        :param expr: str : DEFAULT|MATERIALIZED|ALIAS expr
        :param codec: str
        :param if_not_exists_or_if_exists: bool
        :param on_cluster: bool
        :param extra: str, None
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return:
        """
        type = type if type else ""
        after = "AFTER {}".format(after) if after else ""
        ttl = "TTL {}".format(ttl) if ttl else ""
        expr = expr if expr else ""
        codec = codec if codec else ""
        cluster = "ON CLUSTER {}".format(on_cluster) if on_cluster else ""
        extra = extra if extra else ""
        method = method.upper()

        if method == "ADD":
            exists = "IF NOT EXISTS" if if_not_exists_or_if_exists else ""
        else:
            exists = "IF EXISTS" if if_not_exists_or_if_exists else ""

        query = (
            "ALTER TABLE {db}.{table} {cluster} {method} COLUMN "
            "{exists} {name} {type} {default} {codec} {ttl} {after} {extra}"
        )
        query = query.format(
            db=db,
            table=table,
            cluster=cluster,
            method=method,
            exists=exists,
            name=name,
            type=type,
            default=expr,
            codec=codec,
            ttl=ttl,
            after=after,
            extra=extra,
        )

        return self.execute(query, **kwargs)

    def add_column(
        self,
        db,
        table,
        name,
        type,
        after=None,
        expr=None,
        codec=None,
        ttl=None,
        if_not_exists=True,
        on_cluster=False,
        **kwargs
    ):
        return self._alter_table_column(
            db,
            table,
            "ADD",
            name,
            type,
            after,
            expr,
            codec,
            ttl,
            if_not_exists,
            on_cluster,
            **kwargs
        )

    def drop_column(self, db, table, name, if_exists=True, on_cluster=False, **kwargs):
        return self._alter_table_column(
            db,
            table,
            "DROP",
            name,
            if_not_exists_or_if_exists=if_exists,
            on_cluster=on_cluster,
            **kwargs
        )

    def clear_column(
        self, db, table, name, partition, if_exists=True, on_cluster=False, **kwargs
    ):
        partition_ = "IN PARTITION {}".format(partition)
        return self._alter_table_column(
            db,
            table,
            "CLEAR",
            name,
            if_not_exists_or_if_exists=if_exists,
            on_cluster=on_cluster,
            extra=partition_,
            **kwargs
        )

    def comment_column(
        self, db, table, name, comment, if_exists=True, on_cluster=False, **kwargs
    ):
        comment = "'{}'".format(comment)
        return self._alter_table_column(
            db,
            table,
            "COMMENT",
            name,
            if_not_exists_or_if_exists=if_exists,
            on_cluster=on_cluster,
            extra=comment,
            **kwargs
        )

    def modify_column(
        self,
        db,
        table,
        name,
        type,
        expr=None,
        ttl=None,
        if_exists=True,
        on_cluster=False,
        **kwargs
    ):
        return self._alter_table_column(
            db,
            table,
            "MODIFY",
            name,
            type,
            expr=expr,
            ttl=ttl,
            if_not_exists_or_if_exists=if_exists,
            on_cluster=on_cluster,
            **kwargs
        )


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
        **kwargs
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
        :param extra_before_settings: str : will be inserted before SETTINGS
        :param engine: str
        :param settings: str
        :param kwargs: Parameters accepted by the clickhouse_driver library
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
            **kwargs
        )

    def create_table_log(
        self,
        table,
        columns,
        if_not_exists=True,
        temporary=False,
        engine="StripeLog",
        type_log_table=None,
        **kwargs
    ):
        """

        :param table: str
        :param columns: list, list(list) : [...,'Name String ...'] or [...,("Name", "String",...)]
        :param if_not_exists: bool
        :param temporary: bool
        :param engine: str
        :param type_log_table: : parameter is no longer supported. But remained for compatibility.
        :param kwargs: Parameters accepted by the clickhouse_driver library
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
            **kwargs
        )

    def __repr__(self):
        return str(self.db)

    def __str__(self):
        return str(self.db)


class Table(ChClient):
    def __init__(self, client, db, table, *args, **kwargs):
        self._client = client or Client
        self.db = db
        self.table = table
        super().__init__(*args, **kwargs)

    def select(
        self, limit=10, offset=0, columns=None, where=None, order_by=None, dataframe=False, **kwargs
    ):
        """

        :param limit: int
        :param offset: int
        :param columns: list, tuple, None
        :param where: str
        :param order_by: str
        :param dataframe: bool : return DataFrame
        :param dtype: object type : a parameter is passed when creating a dataframe
            to determine the type of columns of the dataframe
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return: DataFrame
        """
        return self._client.select(
            self.db, self.table, limit, offset, columns, where, order_by, dataframe, **kwargs
        )

    def insert(self, data, columns=None, **kwargs):
        return self._client.insert(self.db, self.table, data, columns, **kwargs)

    def insert_select(self, query, columns=None, **kwargs):
        return self._client.insert_select(self.db, self.table, query, columns, **kwargs)

    def insert_transform_from_table(self, from_db, from_table, **kwargs):
        """
        Transfer from one table to another identical table with forced casting
        of column types according to the types of columns of the target table.

        :param from_db: str
        :param from_table: str
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return:
        """
        return self._client.insert_transform_from_table(
            from_db, from_table, self.db, self.table, **kwargs
        )

    def insert_via_stage_table(
        self, data, columns=None, stage_db="default", stage_table=None, **kwargs
    ):
        return self._client.insert_via_stage_table(
            self.db,
            self.table,
            data,
            columns=columns,
            stage_db=stage_db,
            stage_table=stage_table,
            **kwargs
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
        :param prevent_parallel_processes: bool : The request will be made when all mutations on the table are complete.
        :param sleep: int : The interval to check the completion of all mutations in the table.
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return:
        """
        return self._client.delete(
            db=self.db,
            table=self.table,
            where=where,
            prevent_parallel_processes=prevent_parallel_processes,
            sleep=sleep,
            **kwargs
        )

    def update(
        self, update, where, prevent_parallel_processes=False, sleep=1, **kwargs
    ):
        """

        :param update: str
        :param where: str
        :param prevent_parallel_processes: bool : The request will be made when all mutations on the table are complete.
        :param sleep: int : The interval to check the completion of all mutations in the table.
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return:
        """
        return self._client.update(
            db=self.db,
            table=self.table,
            update=update,
            where=where,
            prevent_parallel_processes=prevent_parallel_processes,
            sleep=sleep,
            **kwargs
        )

    def copy_data_from(
        self, from_db, from_table, where=None, columns=None, distinct=False, **kwargs
    ):
        """
        Copying data. The target table is created automatically if missing.
        After copying, the number of rows is checked,
        if the distinct parameter is not included, which removes duplicate rows.

        :param from_db: str
        :param from_table: str
        :param where: str
        :param columns: list
        :param distinct: bool : Will remove duplicate lines when copying
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return: True, False and None with distinct=True
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
        :param return_new_table: Returns a new Table class
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return: , Table
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
             If the partition key consists of one column,
             then it can be passed as str or int, and otherwise as list (list).
             Examples: '2018-01-01' or 123 or [..., [12345, '2018-01-01']]
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return:
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

    def deduplicate_data(self, where, **kwargs):
        """
        Removes duplicate rows by copying the table and data and inserting backwards through DISTINCT.

        :param where: str
        :param kwargs: Parameters accepted by the clickhouse_driver library
        :return: True, False
        """
        return self._client.deduplicate_data(self.db, self.table, where, **kwargs)

    def add_column(
        self,
        name,
        type,
        after=None,
        expr=None,
        codec=None,
        ttl=None,
        if_not_exists=True,
        on_cluster=False,
        **kwargs
    ):
        return self._client.add_column(
            self.db,
            self.table,
            name,
            type,
            after=after,
            expr=expr,
            codec=codec,
            ttl=ttl,
            if_not_exists=if_not_exists,
            on_cluster=on_cluster,
            **kwargs
        )

    def drop_column(self, name, if_exists=True, on_cluster=False, **kwargs):
        return self._client.drop_column(
            self.db,
            self.table,
            name,
            if_exists=if_exists,
            on_cluster=on_cluster,
            **kwargs
        )

    def clear_column(
        self, name, partition=None, if_exists=True, on_cluster=False, **kwargs
    ):
        return self._client.clear_column(
            self.db,
            self.table,
            name,
            partition=partition,
            if_exists=if_exists,
            on_cluster=on_cluster,
            **kwargs
        )

    def comment_column(self, name, comment, if_exists=True, on_cluster=False, **kwargs):
        return self._client.comment_column(
            self.db,
            self.table,
            name,
            comment=comment,
            if_exists=if_exists,
            on_cluster=on_cluster,
            **kwargs
        )

    def modify_column(
        self,
        name,
        type,
        expr=None,
        if_exists=True,
        on_cluster=False,
        ttl=None,
        **kwargs
    ):
        return self._client.modify_column(
            self.db,
            self.table,
            name,
            type,
            expr=expr,
            ttl=ttl,
            if_exists=if_exists,
            on_cluster=on_cluster,
            **kwargs
        )

    def __repr__(self):
        return "{}.{}".format(self.db, self.table)

    def __str__(self):
        return "{}.{}".format(self.db, self.table)
