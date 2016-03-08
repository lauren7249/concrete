import boto, re
from boto.s3.key import Key
import gzip
import StringIO
from flatmappers import *
from foldbykey import *

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
