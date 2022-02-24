#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install mrjob


# In[3]:


get_ipython().run_cell_magic('file', 'yelp.py', 'from mrjob.job import MRJob\nclass  average_cool_rating(MRJob):\n    def mapper(self,_,line):\n        row = line.split(\',\')\n        rating = row[3]\n        cool = row[7]\n        if cool != 0 and rating != \'stars\':\n            yield "cool", int(rating)\n    def reducer(self,key,values):\n        rating = list(values)\n        yield " avarage_cool != 0:",(sum(rating)/len(rating))')


# In[4]:


import yelp

mr_job = yelp.average_cool_rating(args=['yelp.csv'])
with mr_job.make_runner() as runner:
    runner.run()
    for key, value in mr_job.parse_output(runner.cat_output()):
        print(key, value)

