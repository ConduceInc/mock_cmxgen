"""CMX mock data generator w/coords in XY base don scale/location of initial warehouse.

Usage: mock_cmxdata_generator.py [options]

Options:
    -e <entity_count>, --entity-count=<entity_count>  Number of entities to generate [default: 100]
    -p <update_period>, --period=<update_period>  Time (secs) between location updates [default: 15]
    -m <max_speed>, --max-speed=<max_speed>  Max forklift speed (KPH)) [default: 10]
    -d <start_date>, --start-date=<date>  Start date [default: 2016-08-01]
    -h <host>, --host=<server_name>  Name of Conduce server [default: dev-app.conduce.com]
    -a <api_key>, --apikey=<key>  API Key
    -d <dataset>, --dataset=<id>  Dataset ID

"""

from docopt import docopt
import time
import datetime
from calendar import monthrange
import random
import copy
import json
import os
import sys
import httplib,urllib
import time
import requests

ENTITY_COUNT=100
entities=[]

# -------------------- Map

# Coords of map/floor (mm)
MAP_BOTTOM_LEFT=(0,0)
MAP_TOP_RIGHT=(150000,160000)

# Dimensions of map/floor location
X_AXIS_POINTS=0
Y_AXIS_POINTS=0

#
# Calculate the distance that the entity should move
#
def getDelta( speedMPS ):
    # Convert distance in m/s to mm/s to match the map
    return int(speedMPS * UPDATE_PERIOD_S * 1000)

# -------------------- Entity speed

# Range of entity speeds when moving (in KPH)
SPEED_MIN_KPH=3
SPEED_MAX_KPH=10

#
# Generate a random speed in meters/sec for an entity
#
def getSpeed():
    speedMPS = 0.277778 * random.uniform(SPEED_MIN_KPH, SPEED_MAX_KPH)
    #print "Speed (MPS):", speedMPS
    return speedMPS

# -------------------- Date/time

# Seconds between location updates
UPDATE_PERIOD_S=15

START_DATE='2016-07-01'
START_TIME='02:00'
STOP_TIME='03:00'

#
# Concatenate the date/time and return the time in seconds.
#
def getTime(dateStr, timeStr):
    date_time = dateStr + " " + timeStr
    return int( time.mktime( time.strptime(date_time, '%Y-%m-%d %H:%M') ) )

#
# Parse the date for the month and return how many days.
#
def getDays(dateStr):
    dt_obj = datetime.datetime.strptime(dateStr, '%Y-%m-%d')
    n = monthrange(dt_obj.year, dt_obj.month)[1]
    return n


# -------------------- Entity operations

#
# Init and return a list of entities.
#
def initEntities(entityCount, startTime, startLoc):
    #print "Entities: ", entityCount

    attribs = [ { "key":"equip",  "type":"STRING", "str_value":"scanner" } ]

    loc = []
    pt = { "x":float(startLoc[1]), "y":float(startLoc[0]), "z":0 }
    loc.append(pt)

    for i in range(entityCount):
        entity = {
            "identity": str(i+1),
            "kind": "employee",
            "timestamp-ms": int(startTime * 1000),
            #"endtime_ms": int(startTime * 1000) + 24*3600000,
            "path": copy.deepcopy(loc),
            "attrs": copy.deepcopy(attribs)
        }
        entities.append(entity);

MOVE_NONE = 0
MOVE_UP = 1
MOVE_DOWN = 2
MOVE_LEFT = 3
MOVE_RIGHT = 4

def getMove():
    val = random.uniform(0, 100)
    if ( val < 23 ):
        return MOVE_UP
    elif( val < 46 ):
        return MOVE_DOWN
    elif( val < 69 ):
        return MOVE_LEFT
    elif( val < 92 ):
        return MOVE_RIGHT
    else:
        return MOVE_NONE

def getNextMove( move, delta, pos ):
    posX = pos[0]
    posY = pos[1]
    
    x = posX
    y = posY
    #print "pre ", move, x, y, delta

    if ( move == MOVE_RIGHT ):
        x = min( (posX + delta), MAP_TOP_RIGHT[0] )
    
    elif ( move == MOVE_LEFT ):
        x = max( (posX - delta), MAP_BOTTOM_LEFT[0] )

    elif ( move == MOVE_UP ):
        y = min( (posY + delta), MAP_TOP_RIGHT[1] )

    else: # DOWN
        y = max( (posY - delta), MAP_BOTTOM_LEFT[1] )

    #print "post XY", x, y
    return (move, x, y)

#
# Given an entity, generate its next position and update the entry.
#

def updateEntityMovement( entity ):
    move = getMove()
    #print "Move: ", move
    
    if ( move ): # not NONE=0
        # Speed varies for each move
        newSpd = getSpeed()
        delta = getDelta( newSpd )
        #print "New Spd, Delta", newSpd, delta

        # Assume the path location we're updating is the first/only one
        pos = entity.get("path")[0]
        newMove = getNextMove( move, delta, (pos["x"], pos["y"]) )
        # Update entity
        pos["x"] = newMove[1]
        pos["y"] = newMove[2]


def updateLocations(tm):
    for entity in entities:
        entity["timestamp-ms"] = tm * 1000
        updateEntityMovement(entity)

#
# Create a JSON representation of the entities suitable for upload.
#
def getConduceEntitySetJSON():
    myEntities = {}
    myEntities["entities"] = entities
    #return json.dumps(myEntities, indent=2)
    return json.dumps(myEntities, separators=(',',':'))

def printCSV():
    for entity in entities:
        pos = entity.get("path")[0]
        posX = pos["x"]
        posY = pos["y"]
        print "%s, %d, %d" % (entity["identity"], int(posX), int(posY))

def printEntities(timestampMs):
    print
    print datetime.datetime.fromtimestamp(timestampMs).strftime('%Y-%m-%d %H:%M:%S')
    #print getConduceEntitySetJSON()
    printCSV()

#
# -------------------- Upload to Conduce
#
def waitForUploadJob(authStr, jobURL):
    headers = { 'Authorization': authStr }
    #print headers
    
    finished = False
    while not finished:
        time.sleep(0.5)
        #print jobURL
        response = requests.get(jobURL, headers=headers)
        if int(response.status_code / 100) != 2:
            print "Error code %s: %s" % (response.status_code, response.text)
            return;
        
        if response.ok:
            print response.content
            msg = response.json()
            if 'response' in msg:
                print "Job completed successfully."
                finished = True
        else:
            print resp, resp.content
            break

def uploadEntities(apiKey, datasetId, hostServer, timestampMs):
    print
    print datetime.datetime.fromtimestamp(timestampMs).strftime('%Y-%m-%d %H:%M:%S')
    
    authStr = 'Bearer ' + apiKey
    URI = '/conduce/api/datasets/add_datav2/' + datasetId
    payload = getConduceEntitySetJSON()
    headers = {
        'Authorization': authStr,
        'Content-type': 'application/json',
        'Content-Length': len(payload)
    }
    #print headers
    #print datetime.datetime.fromtimestamp(timestampMs).strftime('%Y-%m-%d %H:%M:%S')
    print "Uploading ", hostServer, URI
    #print payload

    connection = httplib.HTTPSConnection(hostServer)
    connection.request("POST", URI, payload, headers)
    response = connection.getresponse()
    print response.status, response.reason, response.read()
    connection.close()

    #wait for the job to finish
    job_loc = response.getheader('location')
    if job_loc:
        jobURL = "https://%s/conduce/api%s" % (hostServer, job_loc)
        waitForUploadJob( authStr, jobURL )
    else:
        print "Error: Response contains no job location."

#
# -------------------- M A I N --------------------
#
def main():
    arguments = docopt(__doc__)

    ENTITY_COUNT = int(arguments.get('--entity-count'))
    UPDATE_PERIOD_S = int(arguments.get('--period'))
    SPEED_MAX_KPH = int(arguments.get('--max-speed'))
    START_DATE = arguments.get('--start-date')

    hostServer = arguments.get('--host')
    apiKey = str(arguments.get('--apikey'))
    datasetId = str(arguments.get('--dataset'))

    if apiKey == 'None':
        print "API Key required."
        return
    if datasetId == 'None':
        print "Dataset ID required."
        return

    X_AXIS_POINTS = MAP_TOP_RIGHT[0] - MAP_BOTTOM_LEFT[0]
    Y_AXIS_POINTS = MAP_TOP_RIGHT[1] - MAP_BOTTOM_LEFT[1]

    #startX = int(MAP_BOTTOM_LEFT[0] + X_AXIS_POINTS/2)
    #startY = int(MAP_BOTTOM_LEFT[1] + Y_AXIS_POINTS/2)
    startLoc = MAP_BOTTOM_LEFT #(startX,startY)

    nDays = getDays(START_DATE)
    startDateTime = getTime(START_DATE, START_TIME)
    stopDateTime = getTime(START_DATE, STOP_TIME)

    initEntities(ENTITY_COUNT, startDateTime, startLoc);
    uploadEntities(apiKey, datasetId, hostServer, startDateTime)

    for i in range(nDays):
        tm = startDateTime
        #printEntities(tm)

        while ( tm < stopDateTime ):
            tm += UPDATE_PERIOD_S
            updateLocations(tm)
            #printEntities(tm)
            uploadEntities(apiKey, datasetId, hostServer, tm)
        
        # Jump to next day
        startDateTime += 86400
        stopDateTime += 86400

if __name__=="__main__":
    main()
