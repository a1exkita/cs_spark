#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# In[2]:


sc = SparkContext(appName="FilterRanksEmit")
ssc = StreamingContext(sc, 5)


# In[3]:


rankstream = ssc.textFileStream("gs://hw2_spark_bucket_ak47/q8-whole")
rankstream.saveAsTextFiles("/out-emit")


# In[4]:


ssc.start()


# In[ ]:


ssc.awaitTermination()


# In[ ]:




