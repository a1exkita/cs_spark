#!/usr/bin/env python
# coding: utf-8

# # Task 2 Input

# In[1]:


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import Window
from pyspark.sql.functions import udf, col, regexp_extract, size, explode, lit, max
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType, IntegerType, DoubleType, StructType, StructField
import regex
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'


# In[2]:


spark = SparkSession.builder.getOrCreate()
df_small = spark.read.format('xml').options(
    rowTag='page').load('hdfs:/enwiki_small.xml')


# In[3]:


new_df_small = df_small.select("id", "title", "revision.text._VALUE")
new_df_small = new_df_small.na.drop()


# In[4]:


def find_all(line):
    return regex.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', line)


udf_find_all = udf(lambda x: find_all(x), ArrayType(StringType()))
ext_df_small = new_df_small.withColumn(
    "ext_links", udf_find_all(col('_VALUE')))
ext_df_small = ext_df_small.filter(size("ext_links") > 0)


# In[5]:


udf_length = udf(lambda s: len(s), IntegerType())


# In[6]:


def filter_second(links):
    ignore_colon = list(filter(lambda link: (":" not in link) or (
        link.split(":")[0] == "Category"), links))
    ignore_hash = list(filter(lambda link: "#" not in link, ignore_colon))
    get_first_link = list(map(lambda link: link.split("|")[
                          0].strip().lower(), ignore_hash))
    remove_empty_space = list(
        filter(lambda link: link != "" and link != " ", get_first_link))
    return remove_empty_space


udf_filter_second = udf(lambda row: filter_second(row),
                        ArrayType(StringType()))
udf_lower_title = udf(lambda title: title.strip().lower(), StringType())
filtered_df_small = ext_df_small.withColumn(
    "filtered", udf_filter_second(col("ext_links"))).select("title", "filtered")
lower_df_small = filtered_df_small.withColumn(
    "lower_title", udf_lower_title(col("title"))).select("lower_title", "filtered")

out_df_small = lower_df_small.withColumn(
    "num_neighbors", udf_length(lower_df_small.filtered))
out_df_small = out_df_small.select(out_df_small.lower_title, explode(
    out_df_small.filtered), out_df_small.num_neighbors)
input_df_small = out_df_small.na.drop()
# out_df_small.show()
# out_df_small.write.option("delimiter","\t").csv("/q2-small") # type cmd $hadoop fs -ls /


# # Task 3

# ## Input

# In[7]:


df = input_df_small.withColumn("rank", lit(1.0))
df = df.withColumnRenamed(
    "lower_title", "title")        .withColumnRenamed("col", "link")
# df.cache()
# df.show()


# In[8]:


for i in range(10):
    df = df.withColumn("contribution", df.rank/df.num_neighbors)
    df = df.withColumn("total", f.sum(
        'contribution').over(Window.partitionBy('link')))
    df = df.withColumn("rank", 0.15+0.85*df.total)
link_df = df.select("link", "rank")
title_df = df.select("title").withColumn("rank", lit(0.0))
link_df = link_df.withColumnRenamed("link", "article")
title_df = title_df.withColumnRenamed("title", "article")
res_df = title_df.union(link_df)
res_df = res_df.groupBy("article").agg(
    max("rank").alias("rank")).sort(["article", "rank"]).limit(5)
res_df.write.option("delimiter", "\t").csv("/q8")


# In[9]:


res_df.show()


# In[ ]:
