
import hashlib
import datetime
import logging
import time
import sys
import os
import boto
import json
from boto.s3.key import Key

from service import Service, S3SavedRequest
from constants import SOCIAL_DOMAINS

from person_request import PersonRequest
from person_service import wrapper as person_wrapper
from social_profiles_service import SocialProfilesRequest
from profile_builder_service import ProfileBuilderRequest
from helper import parse_date, uu

class ResultService(Service):

    def __init__(self, data, *args, **kwargs):
        super(ResultService, self).__init__(data, *args, **kwargs)


    def get_agent_prospect(self):
        email = self.client_data.get("email")
        person = person_wrapper({"email":email})
        person = SocialProfilesRequest(person).process()
        request = ProfileBuilderRequest(person)
        profile = request.process()
