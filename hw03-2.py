#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_cell_magic('file', 'group_count.py', "from mrjob.job import MRJob\n\nclass MRCountByYearAndMonth(MRJob):\n\n    def mapper(self, _, line):\n        row = line.split(',')\n        if row[1] != 'date':\n            yield row[1][:7],1\n\n    def reducer(self, year_month, values):\n        yield year_month, sum(values)\n\n\nif __name__ == '__main__':\n    MRCountByYearAndMonth.run()")


# In[2]:


import group_count

mr_job = group_count.MRCountByYearAndMonth(args=['yelp.csv'])
with mr_job.make_runner() as runner:
    runner.run()
    for key, value in mr_job.parse_output(runner.cat_output()):
        print(key, value)

