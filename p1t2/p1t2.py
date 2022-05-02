#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import udf, col, regexp_extract, size, explode
from pyspark.sql.types import ArrayType, StringType
import regex
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'


# In[2]:


spark = SparkSession.builder.getOrCreate()
df_test = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_small.xml')


# In[3]:


new_df_test = df_test.select("id", "title", "revision.text._VALUE")
new_df_test = new_df_test.na.drop()


# In[4]:


def find_all(line):
    return regex.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', line)

udf_find_all = udf(lambda x: find_all(x), ArrayType(StringType()))
ext_df_test = new_df_test.withColumn("ext_links", udf_find_all(col('_VALUE')))
ext_df_test = ext_df_test.filter(size("ext_links") > 0)


# In[6]:


def filter_second(links):
    ignore_colon = list(filter(lambda link: (":" not in link) or (link.split(":")[0] == "Category"), links))
    ignore_hash = list(filter(lambda link: "#" not in link, ignore_colon))
    get_first_link = list(map(lambda link: link.split("|")[0].lower(), ignore_hash))
    return get_first_link

udf_filter_second = udf(lambda row: filter_second(row), ArrayType(StringType()))
udf_lower_title = udf(lambda title: title.lower(), StringType())
filtered_df_test = ext_df_test.withColumn("filtered", udf_filter_second(col("ext_links"))).select("title", "filtered")
lower_df_test = filtered_df_test.withColumn("lower_title", udf_lower_title(col("title"))).select("lower_title", "filtered")

out_df_test = lower_df_test.select(lower_df_test.lower_title, explode(lower_df_test.filtered))
out_df_test = out_df_test.na.drop()
out_df_test = out_df_test.sort(["lower_title","col"],ascending=True).limit(5)
out_df_test.show()


# In[7]:


out_df_test.coalesce(1).write.option("delimiter","\t").csv("/q2-small") # type cmd $hadoop fs -ls /


# In[ ]:




