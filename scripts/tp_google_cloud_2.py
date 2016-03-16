AWS_KEY = "AKIAIZZBJ527CKPNY6YQ"
AWS_SECRET = "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"
joined = sc.pickleFile("s3a://" + AWS_KEY + ":" + AWS_SECRET + "@advisorconnect-test/touchpoints_joined_by_name.pickle")

# from processing_service.geocode_service import wrapper as geocode_wrapper
from processing_service.age_service import wrapper as age_wrapper
from geoindex.geo_point import GeoPoint
from utils.crawlera import reformat_crawlera
def filter_matches(rec):
    name =rec[0]
    tp_record = rec[1][0]
    tp_lat = tp_record.get("tp_lat")
    tp_lng = tp_record.get("tp_lng")
    if not tp_lat or not tp_lng:
        return []
    tp_geopoint = GeoPoint(tp_lat, tp_lng)
    linkedin_data = rec[1][1]
    if not linkedin_data:
        return []
    linkedin_data = reformat_crawlera(linkedin_data)
    geocode = linkedin_data.get("geocode")
    if not geocode:
        return []
    latlng = geocode.get("latlng")
    if not latlng:
        return []    
    geopoint = GeoPoint(latlng[0], latlng[1])
    if geopoint.distance_to(tp_geopoint)>75:
        return []
    tp_dob = tp_record.get("Date of Birth")
    try:
        tp_dob_year = tp_dob.split("/")[-1]
        tp_dob_year = int(tp_dob_year)
    except:
        tp_dob_year = None
    if tp_dob_year:
        person = age_wrapper({"linkedin_data":linkedin_data})
        dob_min = person.get("dob_min")
        if tp_dob_year < dob_min:
            return []
        dob_max = person.get("dob_max")
        if tp_dob_year > dob_max:
            return []
    tp_id = tp_record.get("Constituent ID")
    return [(tp_id, [tp_record, linkedin_data])]

filtered = joined.flatMap(filter_matches).cache()
filtered.saveAsPickleFile("s3a://" + AWS_KEY + ":" + AWS_SECRET + "@advisorconnect-test/touchpoints_joined_filtered.pickle")

