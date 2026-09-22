# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Imports
from pyspark.sql.functions import col, count as count_all, sum as spark_sum, when, isnull
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,schema_tables
def schema_tables(schema):
    """Retorna a lista de nomes de tabelas em um schema do Unity Catalog.
    Usa a variável global `spark` disponível no notebook.
    """
    tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
    return [row.tableName for row in tables]

# COMMAND ----------

# DBTITLE 1,get_columns_to_model
def get_columns_to_model():
    """Retorna o conjunto de colunas que pertencem ao modelo estrela."""
    return {
        "mql_id", "customer_id", "seller_id", "product_id", "order_id",
        "first_contact_date", "origin",
        "won_date", "sales_cycle",
        "customer_city", "customer_state",
        "seller_city", "seller_state",
        "purchase_state",
        "price", "sales_value", "commission", "quantity",
        "product_category_name", "category",
        "order_purchase_timestamp", "order_date",
        "year", "month", "quarter", "day_of_week",
        "order_status"
    }

# COMMAND ----------

# DBTITLE 1,get_pks
def get_pks(schema):
    """Retorna as primary keys das tabelas da camada silver ou gold."""
    pk_silver = {
        "customers": "customer_id",
        "sellers": "seller_id",
        "marketing_qualified_leads": "mql_id",
        "closed_deals": "mql_id",
        "products": "product_id",
        "orders": "order_id",
        "order_items": ["order_id", "product_id", "seller_id"],
    }
    pk_gold = {
        "dim_leads": "mql_id",
        "dim_sellers": "seller_id",
        "dim_produtos": "product_id",
        "dim_dates": "order_date",
        "fato_vendas": ["order_id", "product_id", "seller_id"],
    }
    return pk_silver if schema == "silver" else pk_gold

# COMMAND ----------

# MAGIC %md
# MAGIC As funções find_nulls e find_duplicates abaixo identificam problemas de qualidade na camada bronze antes da carga para silver. find_duplicates verifica tanto linhas totalmente duplicadas quanto violações de PK, comparando total de linhas vs. distintos.

# COMMAND ----------

# DBTITLE 1,find_duplicates
def find_duplicates(df, schema, table_name, num_nulls, problems_found, duplicates_set):
    """Detecta linhas duplicadas e violações de PK no dataframe."""
    df_cols = [c for c in df.columns if c in get_columns_to_model()]
    total_rows = df.count()
    distinct_rows = df.select(df_cols).distinct().count()
    duplicate_count = total_rows - distinct_rows

    w = Window.partitionBy([col(c) for c in df_cols])
    duplicated_rows = df.withColumn("_dup_count", count_all("*").over(w)) \
        .filter(col("_dup_count") > 1) \
        .drop("_dup_count")

    pk_col = get_pks(schema).get(table_name)
    pk_dup_count = 0
    if pk_col:
        pk_cols_list = pk_col if isinstance(pk_col, list) else [pk_col]
        if all(c in df_cols for c in pk_cols_list):
            pk_dup_count = total_rows - df.select(pk_cols_list).distinct().count()

    if duplicate_count > 0 or pk_dup_count > 0:
        if num_nulls == 0:
            print(f"\n{'='*60}")
            print(f"Table: {table_name}")

        duplicates_set.add(table_name)
        print(f"\nDuplicados:")
        print(f"Linhas: {total_rows} | Duplicados: {duplicate_count} | PK duplicados: {pk_dup_count}")

        print(f"\nLinhas duplicadas em {table_name}:")
        duplicated_rows.orderBy("order_id").limit(20).show(truncate=False)

        problems_found += 1

    return problems_found, duplicates_set

# COMMAND ----------

# DBTITLE 1,find_nulls
def find_nulls(df, table_name, problems_found, nulls_dict):
    """Conta valores nulos por coluna (apenas colunas do modelo estrela)."""
    columns = [c for c in df.columns if c in get_columns_to_model()]

    nulls_count = df.select([
        spark_sum(when(isnull(c), 1).otherwise(0)).alias(c) for c in columns
    ]).collect()[0]

    num_nulls = 0
    for col_name, null_count in nulls_count.asDict().items():
        if null_count > 0:
            if table_name not in nulls_dict.keys():
                nulls_dict[table_name] = [col_name]
            else:
                nulls_dict[table_name].append(col_name)

            if num_nulls == 0:
                print(f"\n{'='*60}")
                print(f"Table: {table_name}")
                print(f"\nNulls por coluna:")
            num_nulls += null_count
            print(f"  {col_name}: {null_count}")

            problems_found += 1

    return num_nulls, problems_found, nulls_dict