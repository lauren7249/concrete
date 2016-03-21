
# coding: utf-8
#PYSPARK_DRIVER_PYTHON=ipython pyspark
# In[85]:
#only works as root
from setuptools.command import easy_install
m = easy_install.main( ["nameparser"] )
m = easy_install.main( ["boto"] )
m = easy_install.main( ["geopy"] )
m = easy_install.main( ["geoindex"] )
m = easy_install.main( ["pandas"] )
# In[67]:
sc.addPyFile("/usr/local/lib/python2.7/dist-packages/nameparser-0.3.11-py2.7.egg")


# In[5]:

import boto

class Crawlera(object):

    def __init__(self, period, sc):
        self.sc = sc
        self.AWS_KEY = "AKIAIZZBJ527CKPNY6YQ"
        self.AWS_SECRET = "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"
        self.AWS_BUCKET = "ac-crawlera"
        self.AWS_BUCKET_ZIPPED = "crawlera-linkedin-profiles"
        self.S3_BUCKET = boto.connect_s3(self.AWS_KEY, self.AWS_SECRET).get_bucket(self.AWS_BUCKET_ZIPPED)
        self.S3_BUCKET_ZIPPED = boto.connect_s3(self.AWS_KEY, self.AWS_SECRET).get_bucket(self.AWS_BUCKET_ZIPPED)
        self.PERIOD = period

    def get_s3_data(self):
        self.keys = self.S3_BUCKET.list("linkedin/people/" + self.PERIOD + "/")
        self.keypaths = ["s3a://" + self.AWS_KEY + ":" + self.AWS_SECRET + "@" + self.AWS_BUCKET_ZIPPED + "/" + key.name for key in self.keys]
        self.filenames = ",".join(self.keypaths)
        self.data = self.sc.textFile(self.filenames)
        return self.data


# In[6]:

import re
import json

total = sc.accumulator(0)
kept = sc.accumulator(0)

def clean_name(name):
    if not name:
        return ""
    name = name.lower()
    name = re.sub("\s+"," ",name)
    name = re.sub("[^a-z ]","", name)
    return name

def to_names(line):
    total.add(1)
    try:
        linkedin_data = json.loads(line)
    except Exception, e:
        print "error"
        print str(e)
        print type(line)
        return []
    headline = linkedin_data.get("headline")
    if not headline or headline=="--":
        print "no headline"
        return []    
    num_connections = linkedin_data.get("num_connections","0")
    try:
        connections = int(num_connections.replace("+",""))
    except Exception, e:
        connections = 0  
    if connections < 5:
        print "not enough connections"
        return []
    locality = linkedin_data.get("locality")
    if not locality:
        print "no locality"
        return []
    full_name = linkedin_data.get("full_name")
    if not full_name:
        print "no full name"
        return []
    import nameparser
    parsed_name = nameparser.HumanName(full_name).as_dict()   
    first_name = clean_name(parsed_name.get("first"))
    last_name = clean_name(parsed_name.get("last"))
    nick_name = clean_name(parsed_name.get("nickname"))
    if not last_name:
        print "no last name"
        return []
    if not first_name and not nick_name:
        print "no first name"
        return []
    kept.add(1)
    output = []
    if first_name:
        output.append((first_name + " " + last_name, linkedin_data))
    if nick_name:
        output.append((nick_name + " " + last_name, linkedin_data))
    return output

c = Crawlera("2016-02",sc)
data = c.get_s3_data()
output = data.flatMap(to_names)

# In[7]:

from boto.s3.key import Key
import boto
import pandas
TP_BUCKET = boto.connect_s3(c.AWS_KEY, c.AWS_SECRET).get_bucket("advisorconnect-test")
key = Key(TP_BUCKET)
key.key = "New System Test Input.csv"
key.get_contents_to_filename(key.key)
tp = pandas.read_csv(key.key)
tp.count()


# In[8]:

key = Key(TP_BUCKET)
key.key = "zipcode.csv"
key.get_contents_to_filename(key.key)
zips = pandas.read_csv(key.key)


# In[9]:

firstname = "First Name"
lastname = "Last Name"
dob = "Date of Birth"
zipcode = "Zip"


# In[10]:

def clean_zip(z):
    try:
        return int(z.split("-")[0])
    except Exception:
        return None


# In[11]:

tp_columns = list(tp.columns.values)
tp["zip"] = tp[zipcode].apply(clean_zip)
#4974 -> 4963
tp.drop_duplicates(subset=[firstname,lastname, dob, "zip"], inplace=True)


# In[12]:

zips.rename(columns={"latitude":"tp_lat","longitude":"tp_lng"}, inplace=True)
zips = zips[["zip","tp_lat","tp_lng"]]


# In[13]:

tp =tp.merge(zips, how="inner", on=["zip"])
tp.fillna('',inplace=True)


# In[14]:

key = Key(TP_BUCKET)
key.key = "Nickname Database.csv"
key.get_contents_to_filename(key.key)
nicknames = pandas.read_csv(key.key, index_col="Name")


# In[15]:

nicknames.fillna('', inplace=True)
nicknames_dict = {}
for index, row in nicknames.iterrows():
    nnames = set(row.values)
    if '' in nnames: nnames.remove('')
    nicknames_dict[index] = nnames


# In[17]:

tp_list = []
for index, row in tp.iterrows():
    first_name = clean_name(row[firstname])
    last_name = clean_name(row[lastname])
    tp_list.append((first_name + " " + last_name, row.to_dict()))
    for nick_name in nicknames_dict.get(first_name, []):
        nick_name = clean_name(nick_name)
        tp_list.append((nick_name + " " + last_name, row.to_dict())) 


# In[18]:

tp_rdd = sc.parallelize(tp_list)
tp_rdd.count() #15644

joined= tp_rdd.leftOuterJoin(output)

# In[20]:

joined.cache()
joined.saveAsPickleFile("s3a://" + c.AWS_KEY + ":" + c.AWS_SECRET + "@advisorconnect-test/touchpoints_joined_by_name.pickle")
