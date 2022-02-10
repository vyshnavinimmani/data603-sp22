#!/usr/bin/env python
# coding: utf-8

# In[99]:


import requests
url = 'https://www.gutenberg.org/files/2600/2600-0.txt'
req = requests.get(url, allow_redirects=False)

open('file.txt', 'wb').write(req.content)

count = {}
for word in open('file.txt',encoding="utf8").read().split():
    if (word.isalnum()):
        if word.lower() in count:
            count[word.lower()] += 1  
        else:
            count[word.lower()] = 1

print("Number Of unique word", len(count))

