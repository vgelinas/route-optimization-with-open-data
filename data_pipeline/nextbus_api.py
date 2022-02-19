"""
Wrapper class for the NextBus Web API. 

See more information at 
https://retro.umoiq.com/xmlFeedDocs/NextBusXMLFeed.pdf
""" 
import datetime
import requests
import time

class NextBusAPI:
    """
    Wrapper class for the NextBus Web API.  

    See more information at 
    https://retro.umoiq.com/xmlFeedDocs/NextBusXMLFeed.pdf

    -----------------------------------------------------------------------
    Note: 

    'predictions' & 'predictionsForMultiStops' endpoints are not supported currently.
    'messages' endpoint only supports fetching messages for all routes.  
    'routeConfig' only supports the verbose endpoint for now.

    -----------------------------------------------------------------------
    API Rate Limit (from the documentation):   

    In order to prevent some users from being able to download so much data 
    that it would interfere with other users we have imposed restrictions on 
    data usage. These limitations could change at any time. They currently 
    are:

    Maximum characters per requester for all commands (IP address): 2MB/20sec
    Maximum routes per "routeConfig" command: 100
    Maximum stops per route for the "predictionsForMultiStops" command: 150
    Maximum number of predictions per stop for prediction commands: 5
    Maximum timespan for "vehicleLocations" command: 5min

    """

    def __init__(self, verbose=False, wait_time=0):
        self.verbose = verbose
        self.wait_time = wait_time
        self.endpoints = {
            "agencyList": ("https://retro.umoiq.com/service/publicJSONFeed"
                           "?command=agencyList"), 
            "routeList": ("https://retro.umoiq.com/service/publicJSONFeed"
                          "?command=routeList"
                          "&a={agency_tag}"),   
            "routeConfig": ("https://retro.umoiq.com/service/publicJSONFeed"
                            "?command=routeConfig"
                            "&a={agency_tag}"
                            "&r={route_tag}"
                            "&verbose"),    
            "schedule": ("https://retro.umoiq.com/service/publicJSONFeed"
                          "?command=schedule"
                          "&a={agency_tag}"
                          "&r={route_tag}"), 
            "messages": ("https://retro.umoiq.com/service/publicJSONFeed"
                        "?command=messages" 
                        "&a={agency_tag}"), 
            "vehicleLocations": ("https://retro.umoiq.com/service/publicJSONFeed"
                                 "?command=vehicleLocations"
                                 "&a={agency_tag}"
                                 "&r={route_tag}" 
                                 "&t={epoch_time_in_msec}"),
            "vehicleLocation": ("https://retro.umoiq.com/service/publicJSONFeed"
                                "?command=vehicleLocation" 
                                "&a={agency_tag}"
                                "&v={vehicle_id}"),    
        }  

    def get_response_dict_from_web(self, endpoint_name, **kwarg):     
        """Wrapper for the requests get method.  

        Args:
            endpoint_name (str): Name corresponding to the 'command' type, 
                                 e.g. "agencyList", "routeList", etc. 

        Kwargs: 
            Arguments expected by the NextBus API (e.g. route_tag). 

        Returns:
            response_dict: response object parsed into json. 
        """
        url = self.endpoints[endpoint_name].format(**kwarg) 

        if self.verbose:
            now = datetime.datetime.now().strftime("%H:%M:%S %h %d")
            print("API call at {time} ~ {url}".format(time=now, url=url))

        time.sleep(self.wait_time)  # Slow request rate, if necessary.

        return requests.get(url).json() 

