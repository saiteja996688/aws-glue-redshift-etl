"""AWS Glue ETL Transformation Job - Cleansing and enrichment layer."""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping, RenameField
from pyspark.sql.functions import col, when, concat_ws, to_timestamp

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH', 'REDSHIFT_JDBC_URL']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


class ETLTransformer:
    """PySpark-based ETL transformations for data pipeline."""
    
    @staticmethod
    def cleanse_data(df):
        """Remove duplicates, handle nulls, and standardize formats."""
        df_clean = df.dropDuplicates()
        df_clean = df_clean.fillna({
            'amount': 0,
            'category': 'UNKNOWN',
            'status': 'PENDING'
        })
        return df_clean
    
    @staticmethod
    def enrich_data(df, lookup_df):
        """Join with lookup tables for enrichment."""
        enriched = df.join(
            lookup_df,
            df.category_id == lookup_df.id,
            how='left'
        ).drop(lookup_df.id).drop(lookup_df.category)
        return enriched
    
    @staticmethod
    def partition_write(df, output_path):
        """Write partitioned Parquet to S3."""
        df.write.partitionBy('date', 'category').parquet(output_path)


def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    
    # Read raw data from S3 (bronze layer)
    raw_df = glueContext.create_data_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [args['S3_INPUT_PATH']]},
        format="parquet"
    )
    
    # Apply transformations
    transformer = ETLTransformer()
    clean_df = transformer.cleanse_data(raw_df)
    
    # Write to silver layer
    transformer.partition_write(clean_df, args['S3_OUTPUT_PATH'])
    
    job.commit()


if __name__ == '__main__':
    main()
