
# coding: utf-8

# In[8]:

import sys
sys.path.append("/IPythonNB/concrete/")


# In[9]:

from spark.crawlera import Crawlera
import json
import re
from nameparser import HumanName


# In[10]:

def install(s):
    import os
    os.system("sudo easy_install nameparser")
    return s

rdd = sc.parallelize(xrange(0,20))
rdd = rdd.map(install)
rdd.collect()


# In[12]:

def clean_name(name):
    if not name:
        return ""
    name = name.lower()
    name = re.sub("\s+"," ",name)
    name = re.sub("[^a-z ]","", name)
    return name


# In[22]:

def to_names(line):
    try:
        linkedin_data = json.loads(line)
    except:
        return []
    headline = linkedin_data.get("headline")
    if not headline or headline=="--":
        return []    
    num_connections = linkedin_data.get("num_connections","0")
    try:
        connections = int(num_connections)
    except:
        connections = 0  
    if connections < 5:
        return []
    locality = linkedin_data.get("locality")
    if not locality:
        return []
    full_name = linkedin_data.get("full_name")
    parsed_name = HumanName(full_name).as_dict()   
    first_name = clean_name(parsed_name.get("first"))
    last_name = clean_name(parsed_name.get("last"))
    nick_name = clean_name(parsed_name.get("nickname"))
    if not last_name:
        return []
    if not first_name and not nick_name:
        return []
    output = []
    if first_name:
        output.append((first_name + " " + last_name, linkedin_data))
    if nick_name:
        output.append((nick_name + " " + last_name, linkedin_data))
    return output


# In[23]:

c = Crawlera("2016-02",sc)
data = c.get_s3_data()
data = data.flatMap(to_names)
data.cache()
data.count()


# In[21]:

data.take(10)


# In[18]:

[line[0] for line in sample]


# In[20]:

print "hi"


# In[ ]:



