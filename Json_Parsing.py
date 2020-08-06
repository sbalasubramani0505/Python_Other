import json
from datetime import datetime
import pprint
import re
pp = pprint.PrettyPrinter(indent=4)

def pretty_print(stuff):
    pp.pprint(stuff)

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
