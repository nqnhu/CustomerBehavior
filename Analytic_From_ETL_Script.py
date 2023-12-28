# Import Library
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, isnan, when, count, lit, regexp_replace, desc
import pyspark.sql.functions as sf
from pyspark.sql.functions import rank, dense_rank, row_number, percentile_approx
from pyspark.sql.window import Window
from pyspark.sql.types import *
import os
import pandas as pd
import matplotlib.pyplot as plt

# Create SparkSession
spark = SparkSession.builder.config("spark.driver.memory", "7g").config('spark.jars.packages','mysql:mysql-connector-java:8.0.17').getOrCreate()

# Read ETL_Data from MyQSL Server
url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'customer_360_platform'
driver = "com.mysql.cj.jdbc.Driver"
user = 'root'
password = ''
sql = '(select * from data_behavior_daily_) A'
df = spark.read.format('jdbc').options(url = url, driver = driver, dbtable = sql, user = user, password = password).load()

df.show(5)
df.printSchema()

# Statistics Monthly
monthly_df = df.drop('Date')

def unpivot_data(df):
    a = df.select('Contract', 'TV_Duration').withColumn('Category', sf.lit('TV_Duration')).withColumnRenamed('TV_Duration', 'TotalDuration')
    b = df.select('Contract', 'Relax_Duration').withColumn('Category', sf.lit('Relax_Duration')).withColumnRenamed('Relax_Duration', 'TotalDuration')
    c = df.select('Contract', 'Movie_Duration').withColumn('Category', sf.lit('Movie_Duration')).withColumnRenamed('Movie_Duration', 'TotalDuration')
    d = df.select('Contract', 'Child_Duration').withColumn('Category', sf.lit('Child_Duration')).withColumnRenamed('Child_Duration', 'TotalDuration')
    e = df.select('Contract', 'Sport_Duration').withColumn('Category', sf.lit('Sport_Duration')).withColumnRenamed('Sport_Duration', 'TotalDuration')
    unpivot_data = a.union(b).union(c).union(d).union(e)
    unpivot_data = unpivot_data.groupBy('Contract', 'Category').sum()
    unpivot_data = unpivot_data.withColumnRenamed('sum(TotalDuration)', 'TotalDuration')
    return unpivot_data

## Calculate Taste
def calculate_contract_taste(df):
    data_pivot = df.groupBy('Contract').pivot('Category').sum('TotalDuration')
    contract_taste = data_pivot.withColumn('Relax_Duration', when(col('Relax_Duration').isNotNull(),'Relax').otherwise(col('Relax_Duration')))
    contract_taste = contract_taste.withColumn('Movie_Duration', when(col('Movie_Duration').isNotNull(),'Movie').otherwise(col('Movie_Duration')))
    contract_taste = contract_taste.withColumn('Child_Duration', when(col('Child_Duration').isNotNull(),'Child').otherwise(col('Child_Duration')))
    contract_taste = contract_taste.withColumn('Sport_Duration', when(col('Sport_Duration').isNotNull(),'Sport').otherwise(col('Sport_Duration')))
    contract_taste = contract_taste.withColumn('TV_Duration', when(col('TV_Duration').isNotNull(),'TV').otherwise(col('TV_Duration')))
    contract_taste = contract_taste.withColumn('Taste', sf.concat_ws('-', *[c for c in contract_taste.columns if c!='Contract']))
    contract_taste = contract_taste.select('Contract', 'Taste')
    return contract_taste

## Calculate Most watch
def calculate_most_watch(df):
    windowSpec  = Window.partitionBy("Contract").orderBy(desc("TotalDuration"))
    most_watch = df.withColumn("rank", rank().over(windowSpec))
    most_watch = most_watch.filter(col('rank') == 1)
    most_watch = most_watch.withColumnRenamed('Category', 'Most_Watch')
    most_watch = most_watch.withColumn('Most_Watch', regexp_replace('Most_Watch', '_Duration', ''))
    most_watch = most_watch.select('Contract', 'Most_watch')
    return most_watch

def import_data_to_mysql(data, table_name):
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'customer_360_platform'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = ''
    data.write.format('jdbc').option('url', url).option('driver', driver).option('dbtable', table_name).option('user', user).option('password', password).mode('append').save()
    return print('Data imported successfully.')

def statistics_monthly(df):
    # Group by Contract after removing the Date column
    data_unpivot = unpivot_data(df)
    contract_taste = calculate_contract_taste(data_unpivot)
    most_watch = calculate_most_watch(data_unpivot)

    # Find Most_watch and Taste for each Contract
    result = data_unpivot.groupBy('Contract').pivot('Category').sum('TotalDuration')
    result = result.join(contract_taste, 'Contract', 'inner')
    result = result.join(most_watch, 'Contract', 'inner')

    result = result.withColumn('Month', sf.lit('2022-04'))
    result = result.select('Month', 'Contract', 'Child_Duration', 'Movie_Duration', 'Relax_Duration', 'Sport_Duration', 'TV_Duration', 'Taste', 'Most_watch')

    print('-----------Saving Data Statistic Monthly ---------')
    import_data_to_mysql(data = result, table_name = 'data_statistic_monthly')
    result.repartition(1).write.csv('C:\\Users\\NKNhu\\Project\\Customer_Behavior\\DF_Statistic_Monthly', header=True)
    print('Data Saved Successfully.')
    return result

statistics_data = statistics_monthly(monthly_df)
statistics_data.show(5)

## Top 5 Category groups with the highest customer usage
statistics_data.groupBy('Taste').count().orderBy(desc('count')).show(5)

## Statistics on which Category has the most views by customers
statistics_data.groupBy('Most_watch').count().orderBy(desc('count')).show()

## Use IQR to visualize data
def calculate_iqr_point(df, column_name):
    # Calculate bounds
    quantiles = df.approxQuantile(column_name, [0.25, 0.5, 0.75], 0.01)
    q1, q2, q3 = quantiles[0], quantiles[1], quantiles[2]
    # IQR = Q3 - Q1
    # Q0 = Q1 - 1.5 * IQR
    # Q4 = Q3 + 1.5 * IQR
    return print(column_name+':', 'q1 =', q1, '; q2 =', q2, '; q3 =', q3)

import matplotlib.pyplot as plot
# Print the IQR bounds (25%, 50%, 75%) of each category
iqr_df = statistics_data.na.fill(0)
no_string_columns = [types[0] for types in iqr_df.dtypes if types[1] != 'string']
for column in no_string_columns:
    calculate_iqr_point(iqr_df, column)

# Plot IQR of each category
df = iqr_df.toPandas()
plot.figure(figsize=(15, 5))
b_plot = df.boxplot(column = ['Child_Duration', 'Movie_Duration', 'Relax_Duration', 'Sport_Duration', 'TV_Duration'])
plot.xlabel('Category')
plot.ylabel('Duration')
b_plot.plot()
plot.show()

# RFM Model
## Keep only latest record for each 'Contract' based on 'Contract' and 'Date'
def calculate_r_score(df):
    # Find last_day_access
    windowSpec = Window.partitionBy('Contract').orderBy(desc('Date'))
    last_date = df.withColumn('Rank',dense_rank().over(windowSpec))
    last_date = last_date.filter(last_date.Rank == 1).drop(last_date.Rank)
    last_date = last_date.select('Contract', 'Date').withColumnRenamed('Date', 'Last_date')

    # Calculate R_Score
    r_score = last_date.withColumnRenamed('Last_date', 'Recency')
    r_score = r_score.withColumn('Recency', (30 - regexp_replace('Recency', '2022-04-', '').cast(IntegerType())))
    # r_score = r_score.orderBy('Recency', ascending=True)
    r_score = r_score.withColumn('R_Score', when(((col('Recency') >= 0) & (col('Recency') <= 5)), 3)
                                           .when(((col('Recency') >= 6) & (col('Recency') <= 14)), 2)
                                           .when(((col('Recency') >= 15) & (col('Recency') <= 30)), 1))
    return r_score

def calculate_f_score(df):
    # Calculate number_date_active
    windowSpec1 = Window.partitionBy('Contract').orderBy('Date')
    windowSpec2 = Window.partitionBy('Contract')

    number_date_active = df.withColumn('row_number', sf.row_number().over(windowSpec1)) \
                        .withColumn('Number_Of_Date', sf.last('row_number').over(windowSpec2)) \
                            .filter('row_number = Number_Of_Date')
    
    number_date_active = number_date_active.select('Contract', 'Number_Of_Date')
    # number_date_active = number_date_active.orderBy('Number_Of_Date', ascending=False)
    number_date_active = number_date_active.withColumnRenamed('Number_Of_Date', 'Frequency')

    # Calculate F_Score
    f_score = number_date_active.withColumn('F_Score', when(((col('Frequency') >= 15) & (col('Frequency') <= 30)), 3)
                                                      .when(((col('Frequency') >= 6) & (col('Frequency') <= 14)), 2)
                                                      .when(((col('Frequency') >= 0) & (col('Frequency') <= 5)), 1))
    return f_score

def calculate_m_score(df):
    # Calculate Totalduration
    df = unpivot_data(df)
    total = df.groupBy('Contract').sum('TotalDuration').withColumnRenamed('sum(TotalDuration)', 'Monetary')
    # total = total.orderBy('Monetary', ascending=False)

    # Calculate F_Score
    seconds_of_month = 2592000
    m_score = total.withColumn('M_Score', when(col('Monetary') >= seconds_of_month*0.6, 3)
                                         .when(col('Monetary') >= seconds_of_month*0.3, 2)
                                         .otherwise(1))
    return m_score

def rfm_segmentation(df):
    r_scores = calculate_r_score(df).drop('Last_date')
    f_scores = calculate_f_score(df)
    m_scores = calculate_m_score(df)

    rfm_data = r_scores.join(f_scores, 'Contract', 'inner')
    rfm_data = rfm_data.join(m_scores, 'Contract', 'inner')
    rfm_data = rfm_data.select('Contract', 'Recency', 'Frequency', 'Monetary', 'R_Score', 'F_Score', 'M_Score')

    score_columns = ['R_Score', 'F_Score', 'M_Score']
    rfm_data = rfm_data.withColumn('RFM_CELL', sf.concat_ws(',', *[c for c in score_columns]))
    rfm_data = rfm_data.withColumn('RFM_SCORE', sf.format_number((col("R_Score") + col("F_Score") + col("M_Score"))/3, 2))

    rfm_data = rfm_data.withColumn('Segment', when((col('RFM_SCORE') >= 2), 'High')
                                             .when((col('RFM_SCORE') >= 1.5), 'Mid')
                                             .otherwise('Low'))
    return rfm_data

rfm_data = rfm_segmentation(df)
rfm_data.show(5)

## Visualize distribution of customer groups - 04/2022
data = rfm_data.groupBy('Segment').count().withColumnRenamed('count', 'Number of Customers').toPandas()
data = data.reindex(["Low", "Mid", "High"])
data.plot.bar(x="Segment", y="Number of Customers", rot=0, title="Distribution of customer groups - 04/2022")

## Visualize comparison of RFM Segments based on Rencency, Frequency, Monetary Scores
score_data = rfm_data.groupBy('Segment').mean('R_Score', 'F_Score', 'M_Score')
score_data = score_data.toPandas()
score_data = score_data.rename(columns={"avg(R_Score)": "Rencency Score", "avg(F_Score)": "Frequency Score", "avg(M_Score)": "Monetary Score"})
# plotting
score_data.plot.bar(x="Segment", y=["Rencency Score", "Frequency Score", "Monetary Score"], rot=0, title="Comparison of RFM Segments based on Rencency, Frequency, Monetary Scores")
plt.ylabel('Score')