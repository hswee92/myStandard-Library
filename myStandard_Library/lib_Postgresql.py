"""
lib_Postgresql.py

Purpose:
    Provide reusable PostgreSQL connection, row, and table utilities.

Changelog:
    - 1.0.0:
        - Added PostgreSQL equivalents of the SQLDatabase table and row helpers.
"""

from collections.abc import Callable, Mapping, Sequence
from typing import TypeVar

import psycopg
from psycopg import sql

from myStandard_Library.lib_ContextLogger import ContextLogger


_Result = TypeVar("_Result")


class PostgreSQLDatabase:
    """Manage one reusable PostgreSQL connection and common table operations."""

    def __init__(
        self,
        host_ip: str,
        user: str,
        password: str,
        db_name: str,
        logger: ContextLogger,
        db_table: str | None = None,
        port: int = 5432,
        connect_timeout: int = 5,
        max_reconnect: int = 5,
        schema: str = "public",
    ) -> None:
        """Initialize the connection settings and default table.

        Args:
            host_ip (str): PostgreSQL server host.
            user (str): Database user.
            password (str): Database password.
            db_name (str): Database name.
            logger (ContextLogger): Logger for connection failures.
            db_table (str | None): Default table name.
            port (int): PostgreSQL server port.
            connect_timeout (int): Connection timeout in seconds.
            max_reconnect (int): Maximum attempts for connection errors.
            schema (str): Schema containing the default table.

        Returns: None

        Raises:
            ValueError: If max_reconnect is less than one.
        """
        if max_reconnect < 1:
            raise ValueError("max_reconnect must be at least 1")
        self._host_ip = host_ip
        self._user = user
        self._password = password
        self._db_name = db_name
        self._db_table = db_table
        self._port = port
        self._connect_timeout = connect_timeout
        self._max_reconnect = max_reconnect
        self._schema = schema
        self._logger = logger
        self._conn: psycopg.Connection | None = None

    def connect(self) -> psycopg.Connection:
        """Return the open connection, creating it when needed.

        Args: None

        Returns:
            psycopg.Connection: Open autocommit connection.
        """
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(
                host=self._host_ip,
                user=self._user,
                password=self._password,
                dbname=self._db_name,
                port=self._port,
                connect_timeout=self._connect_timeout,
                autocommit=True,
            )
        return self._conn

    def close(self) -> None:
        """Close and forget the current connection.

        Args: None

        Returns: None
        """
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def is_connected(self) -> bool:
        """Check whether the database connection responds.

        Args: None

        Returns:
            bool: True when the connection responds.

        Raises:
            ConnectionError: If the connection cannot be established or queried.
        """
        def check(conn: psycopg.Connection) -> bool:
            """Query the active connection.

            Args:
                conn (psycopg.Connection): Active connection.

            Returns:
                bool: True when the query succeeds.
            """
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True

        try:
            return self._run(check)
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            raise ConnectionError("Connection closed and reconnect failed") from exc

    def _run(self, operation: Callable[[psycopg.Connection], _Result]) -> _Result:
        """Run an operation with bounded connection retries.

        Args:
            operation (Callable): Operation receiving an open connection.

        Returns:
            _Result: Result of the operation.
        """
        for attempt in range(1, self._max_reconnect + 1):
            try:
                return operation(self.connect())
            except (psycopg.OperationalError, psycopg.InterfaceError):
                self._logger.warning2(
                    "PostgreSQLDatabase",
                    f"Connection attempt {attempt} of {self._max_reconnect} failed.",
                )
                self.close()
                if attempt == self._max_reconnect:
                    raise
        raise AssertionError("Unreachable retry state")

    def _table_name(self, table_name: str | None) -> str:
        """Resolve an explicit or default table name.

        Args:
            table_name (str | None): Explicit table name.

        Returns:
            str: Resolved table name.

        Raises:
            ValueError: If no table name is available.
        """
        table = table_name or self._db_table
        if not table:
            raise ValueError("table_name is required")
        return table

    def _qualified_table(self, table_name: str) -> sql.Identifier:
        """Quote a table name in the configured schema.

        Args:
            table_name (str): Unqualified table name.

        Returns:
            sql.Identifier: Safely quoted schema and table name.
        """
        return sql.Identifier(self._schema, table_name)

    def validate_db(self) -> bool:
        """Confirm that the connection uses the configured database.

        Args: None

        Returns:
            bool: True when connected to the configured database.
        """
        def check(conn: psycopg.Connection) -> bool:
            """Compare the current database with the configured name.

            Args:
                conn (psycopg.Connection): Active connection.

            Returns:
                bool: Whether database names match.
            """
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                row = cur.fetchone()
                return row is not None and row[0] == self._db_name

        return self._run(check)

    def validate_table(self, table_name: str | None = None) -> bool:
        """Check whether a table exists in the configured schema.

        Args:
            table_name (str | None): Table name or configured default.

        Returns:
            bool: Whether the table exists.
        """
        table = self._table_name(table_name)

        def check(conn: psycopg.Connection) -> bool:
            """Query the table catalog for one table.

            Args:
                conn (psycopg.Connection): Active connection.

            Returns:
                bool: Whether the table exists.
            """
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = %s LIMIT 1",
                    (self._schema, table),
                )
                return cur.fetchone() is not None

        return self._run(check)

    def get_columns(self, table_name: str | None = None) -> list[str]:
        """Get table columns in ordinal order.

        Args:
            table_name (str | None): Table name or configured default.

        Returns:
            list[str]: Column names in table order.
        """
        table = self._table_name(table_name)

        def read(conn: psycopg.Connection) -> list[str]:
            """Read columns from the information schema.

            Args:
                conn (psycopg.Connection): Active connection.

            Returns:
                list[str]: Column names in table order.
            """
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (self._schema, table),
                )
                return [row[0] for row in cur.fetchall()]

        return self._run(read)

    def get_last_value(
        self,
        column_name: str,
        table_name: str | None = None,
        order_by: str | None = None,
    ) -> object | None:
        """Get one value from the row with the greatest ordering value.

        Args:
            column_name (str): Column to return.
            table_name (str | None): Table name or configured default.
            order_by (str | None): Ordering column; defaults to column_name.

        Returns:
            object | None: Selected value, or None for an empty table.
        """
        table = self._table_name(table_name)
        query = sql.SQL(
            "SELECT {value} FROM {table} ORDER BY {order} DESC NULLS LAST LIMIT 1"
        ).format(
            value=sql.Identifier(column_name),
            table=self._qualified_table(table),
            order=sql.Identifier(order_by or column_name),
        )

        def read(conn: psycopg.Connection) -> object | None:
            """Fetch the requested value.

            Args:
                conn (psycopg.Connection): Active connection.

            Returns:
                object | None: Selected value, if present.
            """
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                return None if row is None else row[0]

        return self._run(read)

    def insert_rows(
        self,
        rows: Sequence[Mapping[str, object]],
        table_name: str | None = None,
        on_duplicate: str | None = None,
        key_columns: str | Sequence[str] | None = None,
    ) -> int:
        """Insert rows, optionally skipping or updating unique conflicts.

        Args:
            rows (Sequence[Mapping[str, object]]): Rows with identical column keys.
            table_name (str | None): Table name or configured default.
            on_duplicate (str | None): None, 'skip', or 'update'.
            key_columns (str | Sequence[str] | None): Conflict key for update mode.

        Returns:
            int: Number of supplied rows after successful execution.

        Raises:
            ValueError: If rows or conflict settings are invalid.
        """
        table = self._table_name(table_name)
        if not rows:
            return 0
        columns = list(rows[0])
        if not columns or any(set(row) != set(columns) for row in rows):
            raise ValueError("rows must have the same non-empty column keys")
        values = [tuple(row[col] for col in columns) for row in rows]
        query = sql.SQL("INSERT INTO {table} ({columns}) VALUES ({values})").format(
            table=self._qualified_table(table),
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
            values=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        if on_duplicate is not None:
            mode = on_duplicate.strip().lower()
            if mode == "skip":
                query += sql.SQL(" ON CONFLICT DO NOTHING")
            elif mode == "update":
                keys = [key_columns] if isinstance(key_columns, str) else list(key_columns or [])
                if not keys or any(key not in columns for key in keys):
                    raise ValueError("key_columns must name inserted columns")
                update_columns = [column for column in columns if column not in keys]
                conflict = sql.SQL(", ").join(map(sql.Identifier, keys))
                if update_columns:
                    assignments = sql.SQL(", ").join(
                        sql.SQL("{column} = EXCLUDED.{column}").format(
                            column=sql.Identifier(column)
                        )
                        for column in update_columns
                    )
                    query += sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
                        conflict, assignments
                    )
                else:
                    query += sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(conflict)
            else:
                raise ValueError("on_duplicate must be 'skip', 'update', or None")

        def write(conn: psycopg.Connection) -> int:
            """Insert all rows in one transaction.

            Args:
                conn (psycopg.Connection): Active connection.

            Returns:
                int: Number of supplied rows.
            """
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.executemany(query, values)
            return len(rows)

        return self._run(write)

    def create_table(
        self,
        columns: Mapping[str, str],
        table_name: str | None = None,
        if_not_exists: bool = True,
        primary_key: str | Sequence[str] | None = None,
    ) -> None:
        """Create a table from trusted PostgreSQL column definitions.

        Args:
            columns (Mapping[str, str]): Column names and trusted SQL type clauses.
            table_name (str | None): Table name or configured default.
            if_not_exists (bool): Suppress an error if the table exists.
            primary_key (str | Sequence[str] | None): Primary key column names.

        Returns: None

        Raises:
            ValueError: If columns or primary key names are invalid.
        """
        table = self._table_name(table_name)
        if not columns:
            raise ValueError("columns is required")
        definitions = [
            sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(datatype))
            for name, datatype in columns.items()
        ]
        if primary_key is not None:
            keys = [primary_key] if isinstance(primary_key, str) else list(primary_key)
            if not keys or any(key not in columns for key in keys):
                raise ValueError("primary_key must name columns in columns")
            definitions.append(
                sql.SQL("PRIMARY KEY ({})").format(
                    sql.SQL(", ").join(map(sql.Identifier, keys))
                )
            )
        query = sql.SQL("CREATE TABLE {}{} ({})").format(
            sql.SQL("IF NOT EXISTS ") if if_not_exists else sql.SQL(""),
            self._qualified_table(table),
            sql.SQL(", ").join(definitions),
        )

        def write(conn: psycopg.Connection) -> None:
            """Execute table creation.

            Args:
                conn (psycopg.Connection): Active connection.

            Returns: None
            """
            with conn.cursor() as cur:
                cur.execute(query)

        self._run(write)

    def add_column(
        self,
        column_name: str,
        datatype: str,
        table_name: str | None = None,
        default_value: object | None = None,
    ) -> None:
        """Add a column using a trusted PostgreSQL type clause.

        Args:
            column_name (str): Column to add.
            datatype (str): Trusted SQL type clause.
            table_name (str | None): Table name or configured default.
            default_value (object | None): Optional SQL default value.

        Returns: None
        """
        table = self._table_name(table_name)
        query = sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
            self._qualified_table(table), sql.Identifier(column_name), sql.SQL(datatype)
        )
        if default_value is not None:
            query += sql.SQL(" DEFAULT {}").format(sql.Literal(default_value))

        def write(conn: psycopg.Connection) -> None:
            """Execute column creation.

            Args:
                conn (psycopg.Connection): Active connection.

            Returns: None
            """
            with conn.cursor() as cur:
                cur.execute(query)

        self._run(write)


def create_table_add_column(
    db_handler: PostgreSQLDatabase,
    db_table: str,
    dict_sql_format: Mapping[str, str],
    logger: ContextLogger,
    primary_key: str | Sequence[str] | None = None,
) -> None:
    """Create a missing table or add its missing columns.

    Args:
        db_handler (PostgreSQLDatabase): Database helper to use.
        db_table (str): Table to synchronize.
        dict_sql_format (Mapping[str, str]): Trusted column definitions.
        logger (ContextLogger): Logger for schema changes.
        primary_key (str | Sequence[str] | None): Key for new tables.

    Returns: None
    """
    if not db_handler.validate_table(db_table):
        db_handler.create_table(
            dict_sql_format, table_name=db_table, primary_key=primary_key
        )
        return
    existing = set(db_handler.get_columns(db_table))
    for column, datatype in dict_sql_format.items():
        if column not in existing:
            db_handler.add_column(column, datatype, table_name=db_table)
            logger.info2("PostgreSQLDatabase", f"Added column '{column}' to '{db_table}'.")
