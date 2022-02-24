#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_cell_magic('file', 'yelp.py', "from mrjob.job import MRJob\n\nclass MRCount(MRJob):\n\n    def mapper(self, _, line):\n        \n        split_word = line.split(',')\n        review_col = split_word[4].split()\n        yield 'avg words in review',len(review_col)\n        \n\n    def reducer(self, review_col,values):\n        lines = 0\n        words = 0\n        for i in values:\n            lines += 1\n            words += i     \n        yield review_col, words/lines \n\n\nif __name__ == '__main__':\n    MRCount.run()")


# In[2]:


pip install mrjob


# In[3]:


import yelp

mr_job = yelp.MRCount(args=['yelp.csv'])
with mr_job.make_runner() as runner:
    runner.run()
    for key, value in mr_job.parse_output(runner.cat_output()):
        print(key, value)

