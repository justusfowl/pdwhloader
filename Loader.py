import os
from dotenv import load_dotenv
from os.path import join, dirname
import datetime
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import pyodbc
import urllib
import pandas as pd

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

class Loader:

    default_cols = [
        "ts_loaded TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    ]

    def __init__(self, **kwargs):

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        self.run_timestamp = datetime.datetime.utcnow()

        self.logger.info("Loader initiated at %s" % str(self.run_timestamp))

        if "src_schema" not in kwargs:
            print("No src_schema provided")
        else:
            self.src_schema = kwargs["src_schema"]

        if "src_table_list" not in kwargs:
            print("No src_table_list provided")
            return
        else:
            self.src_table_list = kwargs["src_table_list"]

        if "post_processes" in kwargs:
            self.post_processes = kwargs["post_processes"]

        else:
            self.post_processes = {}

        source_driver = "{" + os.environ.get("SOURCE_DRIVER") + "}"
        source_db = os.environ.get("SOURCE_MSSQL_DB")
        source_host = os.environ.get("SOURCE_MSSQL_HOST")
        source_user = os.environ.get("SOURCE_MSSQL_USER")
        source_pass = os.environ.get("SOURCE_MSSQL_PASS")
        source_port = os.environ.get("SOURCE_MSSQL_PORT")


        self.src_conn_string = 'DRIVER={source_driver};SERVER={source_host},{source_port};DATABASE={source_db};UID={source_user};PWD={source_pass}'.format(
            source_driver=source_driver,
            source_db=source_db,
            source_host=source_host,
            source_user=source_user,
            source_pass=source_pass,
            source_port=source_port
        )

        # SOURCE schema
        #@TODO: Hier noch variable konfigurieren
        self.src_conn = pyodbc.connect(self.src_conn_string)

        quoted = urllib.parse.quote_plus(self.src_conn_string)
        self.src_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

        if "target_schema" not in kwargs:
            print("No target_schema provided")
        else:
            self.target_schema = kwargs["target_schema"]

        # TARGET schema
        self.mysql_conn_string = "{drivername}://{user}:{passwd}@{host}:{port}/{db_name}?charset=utf8".format(
            drivername="mysql+pymysql",
            user=os.environ.get("TARGET_MYSQL_USER"),
            passwd=os.environ.get("TARGET_MYSQL_PASS"),
            host=os.environ.get("TARGET_MYSQL_HOST"),
            port=os.environ.get("TARGET_MYSQL_PORT"),
            db_name=os.environ.get("TARGET_MYSQL_DB")
        )

        self.target_engine = self._get_engine()
        self.Session = sessionmaker(bind=self.target_engine)
        self.res_session = self.Session()

    def _get_engine(self):

        curr_engine = create_engine(self.mysql_conn_string)
        return curr_engine

    def _truncate_table(self, table):
        try:
            truncate_qry = "TRUNCATE %s" % table.get("table");

            with self.target_engine.connect() as con:
                con.execute(text(truncate_qry))
            self.logger.info("Truncated table %s" % (table.get("table")))

        except Exception as e:
            self.logger.fatal("Table %s could not be truncated" % table.get("where_clause"), e)


    def _load_table(self, table):

        chunksize = 50000
        current_chunk = chunksize

        read_qry_where = ""
        read_qry_order_by = ""
        offset_qry = ""

        if "where_clause" in table:
            read_qry_where = " WHERE %s " % (table.get("where_clause"))

        if "order_by" in table:
            read_qry_order_by = " ORDER BY %s" % (table.get("order_by"))

        if "offset" in table:
            offset_qry = " OFFSET %s ROWS" % (table.get("offset"))

        self.logger.info("Starting to load table %s..." % table.get("table"))

        read_qry = "SELECT * From %s %s %s %s ;" % (table.get("table"), read_qry_where, read_qry_order_by, offset_qry)

        df = pd.read_sql_query(read_qry, self.src_engine, chunksize=chunksize)

        for d in df:

            try:

                self.logger.info("Processing table %s with chunk: %s" % (table.get("table"), str(current_chunk)))

                # df = df.drop('AOHaupt', 1)

                d["ts_loaded"] = self.run_timestamp

                target_engine = self.target_engine

                d.to_sql(name=table.get("table"), con=target_engine, if_exists='append', index=False)

                current_chunk = current_chunk + chunksize
            except Exception as e:
                self.logger.fatal(e)

    def _table_post_process(self, table):

        self.logger.info("Loading complete, entering post-processing for table %s" % (table.get("table")))

        try:
            post_process_qyries = table.get("post_process");

            with self.target_engine.connect() as con:
                con.execute(text(post_process_qyries))
            self.logger.info("Executing process statements for table %s" % (table.get("table")))

        except Exception as e:
            self.logger.fatal("Could not execute post-process step: %s " % table.get("post_process"), e)

    def _check_table_exists(self, table):

        table_name = table["table"]

        if self.target_engine.dialect.has_table(self.target_engine.connect(), table_name):
            return True
        else:
           return False

    def _drop_table(self, table):
        try:
            table_name = table["table"]

            with self.target_engine.connect() as con:
                drop_qry = 'drop table %s;' % table_name  # % self.target_schema
                con.execute(drop_qry)
        except Exception as e:
            self.logger.error(e, exc_info=True)

    def _create_table(self, table):

        flag_table_exists = False
        flag_table_dropped = False

        if self._check_table_exists(table):
            flag_table_exists = True
            self.logger.info("Table %s already exists in target schema" % table["table"])

            if "drop_if_exists" in table:
                if table["drop_if_exists"]:
                    self._drop_table(table)
                    self.logger.warning("Table %s will be dropped and recreated" % table["table"] )
                    flag_table_dropped = True


        if flag_table_exists and not flag_table_dropped:
            self.logger.fatal("Table %s cannot be created without dropping, as it exists." % table["table"])
        else:

            converted_cols = []

            cursor = self.src_conn.cursor()

            table_name = table["table"]

            cursor.execute(
                ("SELECT * FROM %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = ? ORDER BY ORDINAL_POSITION;" % (self.src_schema)),
                table_name)

            for row in cursor.fetchall():
                converted_cols.append(self._convert_column_from_mssql(row))

            create_statement = self._get_create_statement(table["table"], converted_cols)

            with self.target_engine.connect() as con:
                con.execute(text(create_statement))

    def _get_create_statement(self, table, columns):

        qry_str = "CREATE TABLE `%s`.`%s` (" % (self.target_schema, table)

        for c in self.default_cols:
            columns.append(c)

        for idx, c in enumerate(columns):
            if idx > 0:
                qry_str += ", "
            qry_str += c

        qry_str += ");"

        return qry_str

    def _ensure_max_char_length(self, MAX_LENGTH):
        try:

            if int(MAX_LENGTH) < 0:
                return 2555
            else:
                return MAX_LENGTH
        except:
            self.logger.debug("Error converting MAX_LENGTH %s into INT, 255 used instead" % str(MAX_LENGTH))
            return 2555

    def _convert_column_from_mssql(self, col):

        mssql_types = {
            "int": {
                "type": "INT",
                "format": '"INT"'
            },
            "smallint": {
                "type": "SMALLINT",
                "format": '"SMALLINT(%s)" % (str(getattr(col, "NUMERIC_PRECISION_RADIX")))'
            },
            "datetime": {
                "type": "DATETIME"
            },
            "tinyint": {
                "type": "TINYINT"
            },
            "bit": {
                "type": "TINYINT(1)"
            },
            "decimal": {
                "format": '"DECIMAL(%s,%s)" % (str(getattr(col, "NUMERIC_PRECISION")), str(getattr(col, "NUMERIC_PRECISION_RADIX")))'
            },
            "varchar": {
                "format": '"VARCHAR(%s)" % (str(self._ensure_max_char_length(getattr(col, "CHARACTER_MAXIMUM_LENGTH"))))'
            },
            "nvarchar": {
                "format": '"VARCHAR(%s)" % (str(self._ensure_max_char_length(getattr(col, "CHARACTER_MAXIMUM_LENGTH"))))'
            },
            "money": {
                "type": "DOUBLE"
            }

        }

        target_type = mssql_types.get(getattr(col, "DATA_TYPE"), False)

        if target_type:
            res_col = ""
            if "format" in target_type:
                res_col = eval(target_type["format"])
            else:
                res_col = target_type["type"]

            res_col = ("`%s` " % getattr(col, "COLUMN_NAME")) + res_col

            # TODO: hier noch default werte einfügen

            res_col += " NULL"
            return res_col

        else:
            self.logger.error("No datatype conversion possible for %s (%s)" % (getattr(col, "COLUMN_NAME"), getattr(col, "DATA_TYPE")))
            return False

    def process_tables(self):

        for t in self.src_table_list:

            if "create" in t:
                self._create_table(t)

            if "tructate_first" in t:
                self._truncate_table(t)

            self._load_table(t)

            if "post_process" in t:
                self._table_post_process(t)

    def post_process(self):
        for el in self.post_processes:
            if el["database"] == "target":
                for statement in el["execute"]:
                    try:
                        with self.target_engine.connect() as con:
                            con.execute(text(statement))
                        self.logger.info("Post Processing: %s" % (statement))

                    except Exception as e:
                        self.logger.fatal("Post-Processing failed for:" % statement, e)