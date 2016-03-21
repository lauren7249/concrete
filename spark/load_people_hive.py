
# coding: utf-8

# In[2]:

get_ipython().system(u'sudo apt-get install git maven gcc g++ libsasl2-dev')
get_ipython().system(u'sudo pip install boto pyhive')


# In[22]:

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


# In[19]:

import re
import json

def reformat_schools(educations):
    schools = []
    if not educations:
        return schools
    for education in educations:
        school = {}
        if education.get("degrees"):
            school["degree"] = ", ".join(education.get("degrees"))
        elif education.get("degree") and education.get("major"):
            school["degree"] =  "{}, {}".format(education.get("degree"), education.get("major"))
        elif education.get("degree"):
            school["degree"] = education.get("degree")
        elif education.get("major"):
            school["degree"] = education.get("major")

        school["end_date"] = education.get("end")
        school["college"] = education.get("name")
        school["start_date"] = education.get("start")
        if education.get("profile_url") and education.get("profile_url").split("=")[-1] and education.get("profile_url").split("=")[-1].isdigit():
            school["college_id"] = education.get("profile_url").split("=")[-1]
        school["college_linkedin_url"] = education.get("profile_url")
        school["major"] = education.get("major")
        school["degree_type"] = education.get("degree")
        schools.append(school)
    return schools

def reformat_jobs(jobs):
    experiences = []
    if not jobs:
        return experiences
    for job in jobs:      
        experience = {}
        experience["description"] = job.get("description")
        experience["end_date"] = job.get("end")
        experience["title"] = job.get("title")
        experience["company"] = job.get("organization",[{}])[0].get("name")
        if job.get("organization",[{}])[0].get("profile_url"):
            url =  job.get("organization",[{}])[0].get("profile_url")
            experience["company_linkedin_url"] = url
            if url.split("/")[-1].isdigit():
                experience["company_id"] = url.split("/")[-1]
        experience["start_date"] = job.get("start")
        experience["duration"] = job.get("duration")
        experience["location"] = job.get("location")
        experiences.append(experience)    
    return experiences

def reformat_crawlera(linkedin_data):
    if not linkedin_data or not linkedin_data.keys():
        return {}
    for key in linkedin_data.keys():
        if linkedin_data[key] is None: linkedin_data.pop(key)
    schools = reformat_schools(linkedin_data.get("education",[]))
    experiences = reformat_jobs(linkedin_data.get("experience",[]))
    groups = []
    for group in linkedin_data.get("groups",[]):
        if group.get("profile_url") and group.get("profile_url").split("=")[-1].isdigit():
            group["group_id"] = group.get("profile_url").split("=")[-1]
        group["group_url"] = group.pop("profile_url",None)
        group["image_url"] = group.pop("logo_url",None)
        groups.append(group)
    projects = []
    for p in linkedin_data.get("projects",[]):
        project = {}
        project["description"] = p.get("description")
        project["title"] = p.get("title")
        project["other_people"] = [member.get("full_name") for member in  p.get("members",[])]
        project["other_people_links"] = [member.get("url") for member in  p.get("members",[])]
        if p.get("date"):
            dates = re.findall("\D*\d{4}",p.get("date"))
            if len(dates) >1:
                project["start_date"] = dates[0].strip()
                project["end_date"] = dates[-1].strip()
            elif len(dates):
                project["start_date"] = dates[0].strip()
                project["end_date"] = dates[0].strip()
        projects.append(project)
    causes = linkedin_data.get("volunteering",[{}])[0].get("causes")
    num_connections = linkedin_data.get("num_connections","0")
    try:
        connections = int(num_connections.replace("+",""))
    except:
        connections = 0  
    location_raw = linkedin_data.get("locality")     
    linkedin_data =  {
        'image': linkedin_data.get("image_url"),
        'linkedin_id': linkedin_data.get("linkedin_id"),
        'full_name': linkedin_data.get("full_name"),
        'headline': linkedin_data.get("headline"),
        'schools': schools,
        'experiences': experiences,
        'skills': linkedin_data.get("skills"),
        'people': linkedin_data.get("also_viewed"),
        'connections': connections,
        'location': linkedin_data.get("locality"),
        'industry': linkedin_data.get("industry"),
        "groups": groups,
        "projects": projects,
        "urls":linkedin_data.get("also_viewed"),
        "interests": linkedin_data.get("interests"),
        "causes":causes,
        "organizations":linkedin_data.get("organizations"),
        "source_url": linkedin_data.get("url"),
        "family_name": linkedin_data.get("family_name"),
        "given_name": linkedin_data.get("given_name"),
        "updated": linkedin_data.get("updated"),
        "_key": linkedin_data.get("_key"),
        "websites": linkedin_data.get("websites"),
        "canonical_url": linkedin_data.get("canonical_url"),
        "courses": linkedin_data.get("courses"),
        "languages": linkedin_data.get("languages"),
        "summary": linkedin_data.get("summary"),
        "certifications": linkedin_data.get("certifications"),
        "honors_awards": linkedin_data.get("honors_awards"),
        "publications": linkedin_data.get("publications"),
        "recommendations": linkedin_data.get("recommendations"),
        "volunteering": linkedin_data.get("volunteering"),
        "patents": linkedin_data.get("patents"),
    }
    return linkedin_data


# In[20]:

def format(line):
    try:
        linkedin_data = reformat_crawlera(json.loads(line))
    except:
        print line
        return []
    return [linkedin_data]


# In[26]:

def make_indices(linkedin_data):
    data = {}
    data['linkedin_id'] = linkedin_data.get("linkedin_id")
    data['url'] = linkedin_data.get("source_url")
    data['headline'] = linkedin_data.get("headline")
    data['full_name'] = linkedin_data.get("full_name")
    data["linkedin_data"] = linkedin_data
    return data


# In[27]:

c = Crawlera("2016-02",sc)
data = c.get_s3_data()
data = data.flatMap(format).map(make_indices)
df = data.toDF()


# In[28]:

df.columns


# In[29]:

df.saveAsTable("people")


# In[ ]:



