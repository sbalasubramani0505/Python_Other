Latest code


#This is just a simple shared plaintext pad, with no execution capabilities.

#When you know what language you'd like to use for your interview,
#simply choose it from the dropdown in the top bar.

#You can also change the default language your pads are created with
#in your account settings: https://coderpad.io/settings

#Enjoy your interview!

import json
from datetime import datetime
import pprint
import re
pp = pprint.PrettyPrinter(indent=4)

def pretty_print(stuff):
    pp.pprint(stuff)

# INTRO – Oops! I broke the log files!

# I made a mistake. I was asked to go to a bunch of servers and download the log files to a central location so that they would be easy to review. The log files were all in a JSON format, so I figured I could just concatenate them together to have fewer files in the directory. Later I was told that they are actually 3 different formats that are similar, but have differences that can't be ignored.

# REQUIREMENTS

# Here are the differences that need resolved between the formats:

# some dates are numeric in seconds since epoch
# some dates are numeric in milliseconds since epoch
# some dates are in an ISO string format
# a log record may have 1 or more events records that produces a separate log record per event
# in some records,indicator-level maps to severity by mapping:
# 1 - TRACE, 2 - DEBUG, 3..5 INFO, 6..8 WARN, 9..10 ERROR
# in some records, indicator-type and indicator-value are concatenated to form the message field
# ... but when indicator-type contains the text "message", then only the indicator-value is used to form the message field
# the source field is mapped into the process field for output
# Please help by writing a function that takes as input the mixed up concatenated log records I created, and converts them into the normalized correct format. The JSON parsing is handled for you, and so is the serialization back into JSON.

# GETTING STARTED

# The "transmorgify" function is where you may start writing your code. Add any other classes, helper functions, utility functions, and tests that you think are appropriate (FYI, test writing is not required, but you can do it if you want).

# But be careful not to change the transmogrify function signature or tests would break. Tests are in tests directory and there is a sample input test/transmogrify-input.json and a sample result test/transmogrify-output.json. Even though you are not dealing with the JSON directly, these are helpful to view.

# FINISHING THE EXERCISE

# There are no tests to run, take as long as you would like. We will review your code and the input/output. Do your best to get the input/output to match exactly, but there are no tests to run so it's OK if you overlook something small, I will just let you know and you can make an update to fix it.

# EXAMPLE DATA

# The source format (LEFT side), and the target normalized format (RIGHT side)

# [                                                 [
#   {                                                 {
#     "server": "10.10.0.123",                          "server": "10.10.0.123",
#     "date": 1561994754,                               "date": "2019-07-01T15:25:54.000000",
#     "severity": "INFO",                               "severity": "INFO",
#     "process": "webapp",                              "process": "webapp",
#     "message": "server started."                      "message": "server started."
#   },                                                },
#   {                                                 {
#     "server": "10.10.0.123",                          "server": "10.10.0.123",
#     "date": 1561994756,                               "date":  "2019-07-01T15:25:56.000000",
#     "severity": "WARN",                               "severity": "WARN",
#     "process": "webapp",                              "process": "webapp",
#     "message": "one registered cluster node ..."      "message": "one registered cluster node ..."
#   },                                                },
#   {                                                 {
#     "server": "10.10.0.202",                          "server": "10.10.0.202",
#     "date": 1561994757000,                            "date": "2019-07-01T15:25:57.000000",
#     "source": "jvm-x994a",                            "severity": "WARN",
#     "events": [{                                      "process": "jvm-x994a",
#         "indicator-level": 7,                         "message": "memory-low 200mb"
#         "indicator-type": "memory-low",             },
#         "indicator-value": "200mb"                  {
#       }]                                              "server": "10.10.0.123",
#   },                                                  "date": "2019-07-01T15:25:58.000000",
#   {                                                   "severity": "ERROR",
#     "server": "10.10.0.123",                          "process": "webapp",
#     "date": 1561994758,                               "message": "invalid cluster node removed ..."
#     "severity": "ERROR",                            },
#     "process": "webapp",                            {
#     "message": "invalid cluster node removed ..."     "server": "10.10.0.202",
#   },                                                  "date": "2019-07-01T15:26:13.000000",
#   {                                                   "severity": "WARN",
#     "server": "10.10.0.202",                          "process": "jvm-x994a",
#     "date": 1561994773000,                            "message": "memory-low 190mb"
#     "source": "jvm-x994a",                          },
#     "events": [{                                    {
#         "indicator-level": 7,                         "server": "10.10.0.202",
#         "indicator-type": "memory-low",               "date": "2019-07-01T15:27:23.000000",
#         "indicator-value": "190mb"                    "severity": "WARN",
#       }]                                              "process": "jvm-x994a",
#   },                                                  "message": "memory-low 180mb"
#   {                                                 },
#     "server": "10.10.0.202",                        {
#     "date": 1561994843000,                            "server": "10.10.0.202",
#     "source": "jvm-x994a",                            "date": "2019-07-01T15:27:23.000000",
#     "events": [{                                      "severity": "INFO",
#         "indicator-level": 7,                         "process": "jvm-x994a",
#         "indicator-type": "memory-low",               "message": "full GC"
#         "indicator-value": "180mb"                  },
#       },                                            {
#       {                                               "server": "10.10.0.177",
#         "indicator-level": 3,                         "date": "2019-07-01T15:27:25.000000",
#         "indicator-type": "message",                  "severity": "WARN",
#         "indicator-value": "full GC"                  "process": "microsrvc",
#       }]                                              "message": "retry failed to downstream ..."
#   },                                                }
#   {                                               ]
#     "server": "10.10.0.177",
#     "date": "2019-07-01T15:27:25.000000",
#     "severity": "WARN",
#     "process": "microsrvc",
#     "message": "retry failed to downstream ..."
#   }
# ]
#
#
# WRITE YOUR CODE BELOW
#
# Implement the "transmorgify" function below

def Date_Formatter(date_val):
    return ((datetime.fromtimestamp(date_val)).strftime("%Y-%m-%dT%H:%M:%S.%f"))

def ParseEventsFormat_II(event_source,output):
    # Date manipulation
    
    if bool(re.match('^[0-9]+$',str(event_source["date"]))):
        event_source["date"] = Date_Formatter(event_source["date"])

    output.append(event_source)
    #print(output)

def ParseEventsFormat_I(event_source,output):
    fin_dict = {}
    mapping_arr = ['TRACE','DEBUG','INFO','INFO','INFO','WARN','WARN','WARN','ERROR','ERROR']
    
    #print(event_source)
    
    for events in event_source["events"]:
        fin_dict["server"] = event_source["server"]
                
        if bool(re.match('^[0-9]+$',str(event_source["date"]))):
            fin_dict["date"] = Date_Formatter(event_source["date"]/1000)
        else:
            fin_dict["date"] = event_source["date"]
                    
        fin_dict["severity"] = mapping_arr[events["indicator-level"]-1]
        fin_dict["process"] = event_source["source"]
        if  events["indicator-type"] == "message":
            fin_dict["message"] = events["indicator-value"]
        else:
            fin_dict["message"] = events["indicator-type"] + events["indicator-value"]
        output.append(fin_dict)
        fin_dict = {}
    
def transmorgrify(events):
    
    output = []

    for event in events:
        # perform your transformations
        #pretty_print(event) # you can use pretty_print() to print formatted information
        if "severity" not in event:
            ParseEventsFormat_I(event,output)
        else:
            ParseEventsFormat_II(event,output)   
    print(output)
    return output

# This section runs the "transmorgrify()" function with the input data
with open('/home/coderpad/data/transmogrify-input.json') as f:
    data = json.load(f)
    transmorgrify(data)
