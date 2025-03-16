# Flipkart_Data_Engineering_Pyspark_Project

#imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import col,isnan,when,count
from pyspark.sql.functions import *



#setting up the enviroment
spark=SparkSession.builder.appName("Flipkart Data Engineering").getOrCreate()



#Load The csv data
file_path='dbfs:/FileStore/streaming_input/input1/test.csv'

flipkart_df=spark.read.csv(file_path,header=True,inferSchema=True)
flipkart_df.display(5)



#checking The schema 
flipkart_df.printSchema()
flipkart_df.describe().show()



#handling the missing data

flipkart_df.select([count(when(col(c).isNull(),c)).alias(c) for c in flipkart_df.columns]).display()

#drop the rors that is missing
flipkart_df_clean=flipkart_df.dropna()

#filling specific values to the nan columns or missing columns
flipkart_df_filled=flipkart_df.fillna({"Rating":0})



#Data transformation

#calculate the effective price after discount
flipkart_df_transformed = flipkart_df.withColuman("EffectivePrice", expr("Price - (Price * Discount / 100)"))

#show the upload DataFrame
flipkart_df_transformed.select("Productname", "Price", "Discount", "EffectivePrice").show(5)



#Filter the product with rating greater than 4 and priced below 1000
high_rated_products = flipkart_df_filled.filter((col("Rating") > 4) )

#show the result
high_rated_products.display(5)



#group the catagory and calculate the avg rating
avg_rating_by_catagory = flipkart_df_filled.groupBy("maincateg").avg("Rating") 
avg_rating_by_catagory.display()



#Total Revenue by catagory

total_revenue_by_catagory = flipkart_df_filled.groupBy("maincateg").agg(sum("Rating"))
total_revenue_by_catagory.display()



#save the processed data

output_table='Flipkart_Data_Analysis_Table'
flipkart_df_filled.write.mode("overwrite").saveAsTable(output_table)



%sql
select * from flipkart_data_analysis_table limit 20
