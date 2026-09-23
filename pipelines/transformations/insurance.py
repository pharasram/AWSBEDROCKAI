"""Lakeflow graph definitions. Evaluated by Databricks, not an imperative script."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from framework import load_contracts, normalize, quality_rules, rag_projection, raw_schema, validity_expression


def declare_dataset(config):
    name = config["name"]
    bronze, normalized = f"{name}_bronze", f"{name}_normalized"
    valid, quarantine = f"{name}_valid", f"{name}_quarantine"
    silver = f"{name}_silver"
    source = spark.conf.get("insurance.landing_path").rstrip("/") + "/" + config["source_subpath"]
    condition = validity_expression(config)

    @dp.table(name=bronze, comment="Approved append-only source; restricted raw access")
    def ingest():
        return (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaEvolutionMode", "rescue")
                .option("rescuedDataColumn", "_rescued_data")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .option("multiLine", "false")
                .schema(raw_schema(config)).load(source)
                .withColumn("_source_file", F.col("_metadata.file_path")))

    @dp.temporary_view(name=normalized)
    @dp.expect_all(quality_rules(config))
    def typed():
        return normalize(spark.readStream.table(bronze), config)

    @dp.temporary_view(name=valid)
    def accepted():
        return spark.readStream.table(normalized).where(condition)

    @dp.table(name=quarantine, comment="Invalid records; grant only to remediation operators")
    def rejected():
        invalid = spark.readStream.table(normalized).where(f"NOT ({condition})")
        reason = F.concat_ws(",", *[F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(label))
                                   for label, rule in quality_rules(config).items()])
        return invalid.withColumn("_quality_failures", reason)

    dp.create_streaming_table(name=silver, comment="Current claim state, deterministic SCD type 1")
    dp.create_auto_cdc_flow(
        name=f"{name}_upsert", target=silver, source=valid, keys=config["keys"],
        sequence_by=F.struct(F.col(config["sequence_column"]), F.col("row_hash_key"), F.col("_source_file")),
        stored_as_scd_type=1,
        except_column_list=["_source_updated_at", "_rescued_data", "_corrupt_record"],
    )

    @dp.materialized_view(name=f"{name}_rag_chunks", comment="Approved current documents for downstream embeddings")
    def documents():
        return rag_projection(spark.read.table(silver), config)

    @dp.materialized_view(name=f"{name}_gold_metrics", comment="Exact aggregates partitioned by currency")
    def metrics():
        return (spark.read.table(silver).groupBy("product", "status", "currency")
                .agg(F.count("claim_id").alias("claim_count"), F.sum("amount").alias("total_amount")))


# Each function call creates its own closure: datasets cannot capture a later loop value.
for dataset in load_contracts():
    declare_dataset(dataset)
