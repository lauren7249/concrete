AWS_KEY = "AKIAIZZBJ527CKPNY6YQ"
AWS_SECRET = "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"

filtered = sc.pickleFile("s3a://" + AWS_KEY + ":" + AWS_SECRET + "@advisorconnect-test/touchpoints_joined_filtered.pickle").cache()
filtered.count()

counts = filtered.countByKey()
COUNTS_BY_KEY = sc.broadcast(counts)

from processing_service.person_request import PersonRequest
from helpers.linkedin_helpers import main_school
from helpers.stringhelpers import uu

def format_output(rec):
    key = rec[0]
    match_count = COUNTS_BY_KEY.value[key]
    data = rec[1]
    tp_data = data[0]
    linkedin_data = data[1]
    current_job = PersonRequest()._current_job_linkedin(linkedin_data)
    main_college = main_school(linkedin_data)
    if current_job:
        if current_job.get("title"):
            tp_data['TouchPoints Job Title'] = uu(current_job.get("title").replace('\n',' ')) 
        if current_job.get("company"):
            tp_data['TouchPoints Employer'] = uu(current_job.get("company").replace('\n',' ')) 
        tp_data['TouchPoints Employer Start'] = current_job.get("start_date")
        tp_data['TouchPoints Employer End'] = current_job.get("end_date")
    if main_college:
        if main_college.get("college"):
            tp_data['TouchPoints School'] = uu(main_college.get("college").replace('\n',' ')) 
        tp_data['TouchPoints School Start'] = main_college.get("start_date")
        tp_data['TouchPoints School End'] =  main_college.get("end_date")
        if main_college.get("degree"):
            tp_data['TouchPoints Degree and Major'] = uu(main_college.get("degree").replace('\n',' '))         
    tp_data['TouchPoints Industry'] = uu(linkedin_data.get("industry"))
    if linkedin_data.get("interests"):
        # print "interests"
        # print linkedin_data.get("interests")
        tp_data['TouchPoints Interests'] = uu(", ".join(linkedin_data.get("interests")).replace('\n',' '))
    if linkedin_data.get("causes"):
        # print "causes"
        # print linkedin_data.get("causes")
        tp_data['TouchPoints Causes'] = uu(", ".join(linkedin_data.get("causes")).replace('\n',' '))
    if linkedin_data.get("organizations"):
        # print "organizations"
        # print linkedin_data.get("organizations")
        tp_data['TouchPoints Organizations'] = uu(", ".join([org.get("name") for org in linkedin_data.get("organizations")]).replace('\n',' '))   
    for col in ["TouchPoints Employer", "TouchPoints Job Title", "TouchPoints Industry", "TouchPoints Employer Start", "TouchPoints Employer End", "TouchPoints School", "TouchPoints Degree and Major", "TouchPoints School Start", "TouchPoints School End", "TouchPoints Interests", "TouchPoints Causes", "TouchPoints Organizations"]:
        if not tp_data.get(col):
            tp_data[col] = ''
    tp_data["single_match"] = (match_count == 1)
    tp_data["url"] = linkedin_data.get("source_url")
    return tp_data

formatted = filtered.map(format_output).cache()
fdf = formatted.toDF()
fdf = fdf.drop("zip")
fdf = fdf.drop("tp_lat")
fdf = fdf.drop("tp_lng")
fdf = fdf.fillna('')
fdf_pandas = fdf.toPandas()
fdf_pandas.fillna('',inplace=True)
fdf_pandas.to_csv("New System Test Output.csv", encoding='utf-8', index=None)

import tinys3
conn = tinys3.Connection(AWS_KEY, AWS_SECRET, tls=True)
f = open("New System Test Output.csv",'rb')
conn.upload("New System Test Output.csv",f,'advisorconnect-test')