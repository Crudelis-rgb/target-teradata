"""Teradata target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.connectors import SQLConnector
from singer_sdk.helpers._conformers import replace_leading_digit
from singer_sdk.sinks import SQLSink
from singer_sdk.helpers._typing import get_datelike_property_type
from singer_sdk import typing as t
from typing import Any, Dict, Iterable, List, Optional, cast

import json
import sqlalchemy
import sys
import re
from textwrap import dedent


class TeradataConnector(SQLConnector):
    """The connector for Teradata.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = False  # Whether temp tables are supported.

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for Teradata.

        Args:
            config: The configuration for the connector.
        """        
        if config.get("sqlalchemy_url"):
            return config["sqlalchemy_url"]

        connection_url = sqlalchemy.engine.url.URL.create(
            drivername="teradatasql",
            username=config["username"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
        )

        return connection_url


    def create_engine(self) -> Engine:
        """Overriding the create_engine method as the default Engine comes with the
        json_serializer and json_deserializer parameters that are not supported by the
        Teradata dialect engine

        Returns:
            A new SQLAlchemy Engine.
        """
        return sqlalchemy.create_engine(
            self.sqlalchemy_url,
            # echo=True,
            # pool_pre_ping=True,
            poolclass=sqlalchemy.NullPool # Teradata seems to have probles with the default Pool handling
            # json_serializer=self.serialize_json,
            # json_deserializer=self.deserialize_json,
        )

    def get_column_add_ddl(
        self,
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for creating columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                column_type,
            ),
        )
        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s ADD %(create_column_clause)s",
            {
                "table_name": self.quote(table_name),
                "create_column_clause": create_column_clause,
            },
        )

    def get_column_rename_ddl(
        self,
        table_name: str,
        column_name: str,
        new_column_name: str,
    ) -> sqlalchemy.DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for renaming columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Existing column name.
            new_column_name: New column name.

        Returns:
            A sqlalchemy DDL instance.
        """
        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s "
            "RENAME %(column_name)s TO %(new_column_name)s",
            {
                "table_name": self.quote(table_name),
                "column_name": self.quote(column_name),
                "new_column_name": self.quote(new_column_name),
            },
        )

    def get_column_alter_ddl(
        self,
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the alter column DDL statement.

        Override this if your database uses a different syntax for altering columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to alter.
            column_type: New column type string.

        Returns:
            A sqlalchemy DDL instance.
        """
        alter_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                column_type,
            ),
        )

        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s ADD %(alter_column_clause)s",
            {
                "table_name": self.quote(table_name),
                "alter_column_clause": alter_column_clause
            },
        )

    def to_sql_type(  # noqa: PLR0911, C901
        self,
        jsonschema_type: dict,
    ) -> sqlalchemy.types.TypeEngine:
        """Convert JSON Schema type to a SQL type.

        Args:
            jsonschema_type: The JSON Schema object.

        Returns:
            The SQL type.
        """
        
        if t._jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = get_datelike_property_type(jsonschema_type)
            if datelike_type:
                if datelike_type == "date-time":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TIMESTAMP())
                if datelike_type in "time":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TIME())
                if datelike_type == "date":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DATE())

            maxlength = jsonschema_type.get("maxLength",4000)
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.Unicode(maxlength))

        if t._jsonschema_type_check(jsonschema_type, ("integer",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.BIGINT())
        if t._jsonschema_type_check(jsonschema_type, ("number",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.NUMERIC(38,12))
        if t._jsonschema_type_check(jsonschema_type, ("boolean",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR(20))

        if t._jsonschema_type_check(jsonschema_type, ("object",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.Unicode(4000))

        if t._jsonschema_type_check(jsonschema_type, ("array",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.Unicode(4000))

        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.Unicode(4000))

    def create_temp_table_from_table(self, from_table_name, temp_table_name):
        """Temp table from another table."""

        try:
            self.connection.execute(
                sqlalchemy.text(f"""DROP TABLE {temp_table_name}""")
            )
        except Exception as e:
            pass
        
        ddl = sqlalchemy.text(f"""
            CREATE TABLE {temp_table_name} AS {from_table_name} WITH NO DATA
        """)
        self.connection.execute(ddl)


class TeradataSink(SQLSink):
    """Teradata target sink class."""
        
    connector_class = TeradataConnector

    def generate_insert_statement(
            self,
            full_table_name: str,
            schema: dict,
        ) -> str | Executable:
            """Generate an insert statement for the given records.

            Args:
                full_table_name: the target table name.
                schema: the JSON schema for the new table.

            Returns:
                An insert statement.
            """

            property_names = list(self.conform_schema(schema)["properties"].keys())
            # value_names need to be quoted
            value_names = [self.connector.quote(property_name) for property_name in property_names]
            statement = dedent(
                f"""\
                INSERT INTO {self.connector.quote(full_table_name)}
                ({','.join(value_names)})
                VALUES ({", ".join([f":{name}" for name in property_names])})
                """,  # noqa: S608
            )

            return statement.rstrip()

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.
        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.
        Args:
            context: Stream partition or context dictionary.
        """
        conformed_records = (
            [self.conform_record(record) for record in context["records"]]
            if isinstance(context["records"], list)
            else (self.conform_record(record) for record in context["records"])
        )

        join_keys = [self.conform_name(key, "column") for key in self.key_properties]
        schema = self.conform_schema(self.schema)

        if self.key_properties:

            # Create a temp table (Creates from the table above)
            tmp_table_name = self.full_table_name + "_tmp"

            self.logger.info(f"Creating temp table {self.full_table_name}")
            self.connector.create_temp_table_from_table(
                from_table_name=self.full_table_name,
                temp_table_name=tmp_table_name
            )


            # Insert into temp table
            self.bulk_insert_records(
                full_table_name=tmp_table_name,
                schema=schema,
                records=context["records"],
            )
            # Merge data from Temp table to main table
            self.logger.info(f"Merging data from temp table to {self.full_table_name}")
            self.merge_upsert_from_table(
                from_table_name=tmp_table_name,
                to_table_name=self.full_table_name,
                schema=schema,
                join_keys=join_keys,
            )

        else:
            self.bulk_insert_records(
                full_table_name=self.full_table_name,
                schema=schema,
                records=context["records"],
            )

    def merge_upsert_from_table(
        self,
        from_table_name: str,
        to_table_name: str,
        schema: dict,
        join_keys: List[str],
    ) -> Optional[int]:
        """Merge upsert data from one table to another.
        Args:
            from_table_name: The source table name.
            to_table_name: The destination table name.
            join_keys: The merge upsert keys, or `None` to append.
            schema: Singer Schema message.
        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.
        """
        
         # Conform schema and quote join keys
        schema = self.conform_schema(schema)
        quoted_join_keys = [self.connector.quote(self.conform_name(key, "column")) for key in join_keys]

        # Construct join condition
        join_condition = " AND ".join([f"tmp.{key} = trgt.{key}" for key in quoted_join_keys])

        # Construct update statement
        update_columns = [
            key for key in schema["properties"].keys() if self.connector.quote(key) not in quoted_join_keys
        ]
        update_stmt = ", ".join([f"{self.connector.quote(key)} = tmp.{self.connector.quote(key)}" for key in update_columns])

        # Construct insert columns and values
        quoted_columns = [self.connector.quote(key) for key in schema["properties"].keys()]
        insert_columns = ", ".join(quoted_columns)
        insert_values = ", ".join([f"tmp.{col}" for col in quoted_columns])

        # Construct and execute merge SQL statement
        merge_sql = f"""
            MERGE INTO {self.connector.quote(to_table_name)} AS trgt
            USING {self.connector.quote(from_table_name)} AS tmp
            ON ({join_condition})
            WHEN MATCHED THEN
                UPDATE SET {update_stmt}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
        """
        self.logger.info("Merging with SQL: %s", merge_sql)
        self.connection.execute(sqlalchemy.text(merge_sql))


    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: t.Iterable[dict[str, t.Any]],
    ) -> int | None:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        insert_sql = self.generate_insert_statement(
            full_table_name,
            schema,
        )
        if isinstance(insert_sql, str):
            insert_sql = sqlalchemy.text(insert_sql)

        conformed_records = [self.conform_record(record) for record in records]
        property_names = list(self.conform_schema(schema)["properties"].keys())
        
        # Teradata sqlalchemy driver cannot handle objects. Converting every list or dictionary to a string
        # Process each entry in the list to convert second-level elements to strings
        for entry in conformed_records:
            for key, value in entry.items():
                # Check if the value is a list or a dictionary
                if isinstance(value, (list, dict)):
                    # Convert the list or dictionary to a string
                    # Use json.dumps for a JSON-like string representation
                    entry[key] = json.dumps(value)

        # Create new record dicts with missing properties filled in with None
        new_records = [
            {name: record.get(name) for name in property_names}
            for record in conformed_records
        ]

        self.logger.info("Inserting with SQL: %s", insert_sql)
        with self.connector._connect() as conn, conn.begin():
            result = conn.execute(insert_sql, new_records)

        return result.rowcount
