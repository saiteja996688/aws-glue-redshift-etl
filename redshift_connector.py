"""Redshift connection utilities for AWS Glue ETL jobs."""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession


class RedshiftConnector:
    """Handles connection and operations with Amazon Redshift."""

    def __init__(self, spark, redshift_url, db_name, user, password):
        """
        Initialize Redshift connector.

        Args:
            spark: SparkSession object
            redshift_url: Redshift cluster endpoint
            db_name: Database name
            user: Database user
            password: Database password
        """
        self.spark = spark
        self.redshift_url = redshift_url
        self.db_name = db_name
        self.user = user
        self.password = password
        self.jdbc_url = f"jdbc:redshift://{redshift_url}:5439/{db_name}"

    def get_redshift_options(self):
        """Return JDBC connection options for Spark."""
        return {
            "url": self.jdbc_url,
            "dbtable": "",
            "user": self.user,
            "password": self.password,
            "driver": "com.amazon.redshift.jdbc42.Driver",
        }

    def read_table(self, table_name, predicate=None):
        """
        Read data from a Redshift table into a Spark DataFrame.

        Args:
            table_name: Name of the Redshift table
            predicate: Optional SQL WHERE clause

        Returns:
            pyspark.sql.DataFrame
        """
        options = self.get_redshift_options()
        options["dbtable"] = table_name

        if predicate:
            options["pushdown"] = "true"
            return self.spark.read.jdbc(
                url=self.jdbc_url, table=table_name, predicate=predicate, properties=options
            )
        return self.spark.read.jdbc(url=self.jdbc_url, table=table_name, properties=options)

    def write_table(self, df, table_name, mode="append"):
        """
        Write a Spark DataFrame to a Redshift table.

        Args:
            df: Spark DataFrame to write
            table_name: Target Redshift table name
            mode: Write mode (append, overwrite, ignore, error)
        """
        options = self.get_redshift_options()
        options["dbtable"] = table_name

        df.write.jdbc(url=self.jdbc_url, table=table_name, mode=mode, properties=options)

    def execute_query(self, query):
        """
        Execute a SQL query and return results as DataFrame.

        Args:
            query: SQL query string

        Returns:
            pyspark.sql.DataFrame
        """
        options = self.get_redshift_options()
        options["query"] = f"({query}) AS query_result"
        return self.spark.read.jdbc(url=self.jdbc_url, table="query_result", properties=options)
