from configs import jdbc_config

def read_table_from_mysql(spark, table, limit=None):
    df = spark.read.format('jdbc').options(
        url=jdbc_config["url"],
        driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
        dbtable=table,
        user=jdbc_config["user"],
        password=jdbc_config["password"]) \
        .load()

    if limit is not None:
            df = df.limit(limit)

    return df