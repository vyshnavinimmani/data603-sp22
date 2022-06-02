from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
import pyspark.sql.functions as f
import pandas as pd
import plotly.express as px
stream_df = (spark.readStream.format('socket')
                             .option('host', '127.0.0.1')
                             .option('port', 22230)
                             .load())

json_df = stream_df.selectExpr("CAST(value AS STRING) AS payload")

writer = (
    json_df.writeStream
           .queryName('iss15')
           .format('memory')
           .outputMode('append')
)


streamer = writer.start()
import time

for _ in range(5):
    df = spark.sql("""
    SELECT get_json_object(payload, '$.timestamp') AS time,
           CAST(get_json_object(payload, '$.iss_position') AS string) AS position
    FROM iss15
    """)

    df.show(10)

    print(df)
    time.sleep(5)

streamer.awaitTermination(timeout=120)
print('streaming done!')
df1 = df.withColumn('time', df['time']) \
       .withColumn('latitude', f.split(df['position'], '"').getItem(3)) \
       .withColumn('longitude', f.split(df['position'], '"').getItem(7))
df2 = df1.toPandas()
df2['time'] = pd.to_datetime(df2['time'],unit='s')
fig = px.scatter_geo(df2,lat='latitude',lon='longitude',hover_name='time')
fig.update_layout(title = 'ISS location 2022-04-12 21:38:25 to  23:11:40 UTC ', title_x=0.5)
fig.show()