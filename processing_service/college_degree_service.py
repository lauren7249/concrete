import logging
import hashlib
import boto
import lxml.html
import re
import dateutil
import numpy
import requests
from requests import HTTPError
from boto.s3.key import Key
import datetime
from service import Service, S3SavedRequest
from constants import GLOBAL_HEADERS
from helper import parse_date, is_college

def wrapper(person):
    try:
        college_grad = CollegeDegreeRequest()._has_college_degree(person.get("linkedin_data",{}))
        person["college_grad"] = college_grad    
        return person
    except Exception, e:
        print __name__ + str(e)
        return person
        
class CollegeDegreeService(Service):
    """
    Expected input is JSON with profile info
    Output is going to be existig data enriched with college_grad boolean
    """

    def __init__(self, data, *args, **kwargs):
        super(CollegeDegreeService, self).__init__(data, *args, **kwargs)
        self.wrapper = wrapper

class CollegeDegreeRequest(S3SavedRequest):

    def __init__(self):
        super(CollegeDegreeRequest, self).__init__()
        self.logger = logging.getLogger(__name__)

    def _has_college_degree(self, person):
        for school in person.get("schools",[]):
            #still in school; hasnt earned degree
            if school.get("end_date") == "Present":
                continue
            if is_college(school):
                return True
                # start_date = parse_date(school.get("start_date"))
                # end_date = parse_date(school.get("end_date"))
                # #cant be a 4-year degree if you finished in less than 3 years
                # if end_date and start_date and end_date.year - start_date.year >= 3:
                #     return True
        return False
