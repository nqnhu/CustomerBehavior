# Import Library
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, lit, row_number
import pyspark.sql.functions as sf

# Create SparkSession
spark = SparkSession.builder.config("spark.driver.memory", "7g").config('spark.jars.packages','mysql:mysql-connector-java:8.0.17').getOrCreate()

# ETL data
def check_miss_value(df):
     miss_value = df.withColumn('Missing Value',
                        when(col('Contract').contains('None') | col('Contract').contains('NULL') | (col('Contract') == '' ) | col('Contract').isNull() | isnan('Contract') | 
                             col('AppName').contains('None') | col('AppName').contains('NULL') | (col('AppName') == '' ) | col('AppName').isNull() | isnan('AppName') |
                             col('TotalDuration').contains('None') | col('TotalDuration').contains('NULL') | (col('TotalDuration') == '' ) | col('TotalDuration').isNull() | isnan('TotalDuration'), 'True')
                        .otherwise('False'))
   
     df_miss_value = miss_value.where(col("Missing Value") == 'True').select('Contract', 'AppName', 'TotalDuration')
     if df_miss_value.count() > 0:
        df = df.exceptAll(df_miss_value)
     return df

def read_data(path, file_name):
    df = spark.read.json(path + file_name)
    df = df.select('_source.*')
    df = df.select('Contract', 'AppName', 'TotalDuration')
    df = check_miss_value(df)
    return df

def etl_1_day(path, file_name):
    df = read_data(path, file_name)
    df = df.withColumn('Category',
                       when((col('AppName') == 'KPLUS') | (col('AppName') == 'CHANNEL'), 'Truyền Hình')
                      .when((col('AppName') == 'VOD') | (col('AppName') == 'FIMS') | (col('AppName') == 'BHD'), 'Phim Truyện')
                      .when((col('AppName') == 'RELAX'), 'Giải Trí')
                      .when((col('AppName') == 'CHILD'), 'Thiếu Nhi')
                      .when((col('AppName') == 'SPORT'), 'Thể Thao')
                      .otherwise('Error'))
    
    date_of_file = (file_name[:4] + '-' + file_name[4:6] + '-' + file_name[6:]).replace('.json', '')
    df = df.withColumn('Date', lit(date_of_file))
    
    df = df.select('Contract', 'Date', 'Category', 'TotalDuration')
    df = df.filter(df.Category != 'Error')    

    df = df.groupBy('Contract', 'Date', 'Category').sum()
    df = df.withColumnRenamed('sum(TotalDuration)', 'TotalDuration')

    # ------ CHECK OUTLIER ------
    df = df.filter((df['TotalDuration'] >= 0) & (df['TotalDuration'] <= 86400))

    print('Finished Processing {}'.format(file_name))
    return df

def import_data_to_mysql(data, table_name):
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'customer_360_platform'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = ''
    data.write.format('jdbc').option('url', url).option('driver', driver).option('dbtable', table_name).option('user', user).option('password', password).mode('append').save()
    return print('Data imported successfully.')

def main_task():
    path = "C:\\Users\\NKNhu\\LearnBigData\\Dataset\\log_content\\"
    list_file = os.listdir(path)
    file_name1 = list_file[0]
    df = etl_1_day(path, file_name1)
    for i in list_file[1:]:
        file_name2 = i 
        df_ = etl_1_day(path ,file_name2)
        df = df.union(df_)
        df = df.cache()
    
    df = df.groupBy('Contract', 'Date').pivot('Category').sum('TotalDuration')
    df = df.withColumnRenamed('Truyền Hình', 'TV_Duration') \
                        .withColumnRenamed('Phim Truyện', 'Movie_Duration') \
                            .withColumnRenamed('Thiếu Nhi', 'Child_Duration') \
                                .withColumnRenamed('Thể Thao', 'Sport_Duration') \
                                    .withColumnRenamed('Giải Trí', 'Relax_Duration')
    
    print('-----------Saving Data ---------')
    import_data_to_mysql(data = df, table_name = 'data_behavior_daily_')
    df.repartition(1).write.csv('C:\\Users\\NKNhu\\Project\\Customer_Behavior\\DF_ETL_CLEAN_', header=True)
    print('Data Saved Successfully.')
    return df

df = main_task()