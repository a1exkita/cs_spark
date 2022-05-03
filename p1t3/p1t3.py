#!/usr/bin/env python
# coding: utf-8

# # Task 2 Input

# In[40]:


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import udf, col, regexp_extract, size, explode, when, sum
from pyspark.sql.types import ArrayType, StringType, DoubleType
import regex
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'


# In[41]:


spark = SparkSession.builder.getOrCreate()
df_small = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_small.xml')


# In[42]:


new_df_small = df_small.select("id", "title", "revision.text._VALUE")
new_df_small = new_df_small.na.drop()


# In[43]:


def find_all(line):
    return regex.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', line)

udf_find_all = udf(lambda x: find_all(x), ArrayType(StringType()))
ext_df_small = new_df_small.withColumn("ext_links", udf_find_all(col('_VALUE')))
ext_df_small = ext_df_small.filter(size("ext_links") > 0)


# In[44]:


def filter_second(links):
    ignore_colon = list(filter(lambda link: (":" not in link) or (link.split(":")[0] == "Category"), links))
    ignore_hash = list(filter(lambda link: "#" not in link, ignore_colon))
    get_first_link = list(map(lambda link: link.split("|")[0].strip().lower(), ignore_hash))
    remove_empty_space = list(filter(lambda link: link != "" and link != " ", get_first_link))
    return remove_empty_space

udf_filter_second = udf(lambda row: filter_second(row), ArrayType(StringType()))
udf_lower_title = udf(lambda title: title.strip().lower(), StringType())
filtered_df_small = ext_df_small.withColumn("filtered", udf_filter_second(col("ext_links"))).select("title", "filtered")
lower_df_small = filtered_df_small.withColumn("lower_title", udf_lower_title(col("title"))).select("lower_title", "filtered")

# out_df_small = lower_df_small.select(lower_df_small.lower_title, explode(lower_df_small.filtered))
# out_df_small = out_df_small.na.drop()
# out_df_small.write.option("delimiter","\t").csv("/q2-small") # type cmd $hadoop fs -ls /


# # Task 3

# In[45]:


from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import lit, udf


# In[51]:


def compute_length(links):
    return len(links)
udf_length = udf(lambda row: compute_length(row))


links = lower_df_small.na.drop().withColumnRenamed("lower_title", "link")
ranks = links
ranks = ranks.withColumn("rank", lit(1.0)).drop("filtered")

for _ in range(10):
    cont = links.join(ranks, "link")
    cont = cont.withColumn("num_neighbors", udf_length(cont.filtered))
    cont = cont.withColumn("contribution", cont.rank/cont.num_neighbors)            .drop("link", "rank", "num_neighbors")
    cont = cont.select(explode(cont.filtered),cont.contribution)
    cont = cont.withColumnRenamed("col", "link")
    cont = links.join(cont,on="link",how='fullouter')
    cont = cont.withColumn("cont", when(cont.contribution.isNull(), 0.0)                           .otherwise(cont.contribution)).drop("filtered","contribution")
    cont = cont.groupBy('link').agg(sum("cont").alias("total"))
    ranks = cont.withColumn("rank", when(cont.total>0, 0.15+0.85*cont.total).otherwise(0.0))
    ranks = ranks.drop("total")
    
ranks.cache()
ranks = ranks.filter(ranks.rank>0)
ranks.sort("link","rank").show()


# In[53]:


out_df = ranks.sort("link", "rank").limit(5)
out_df.write.option("delimiter","\t").csv("/q8")

