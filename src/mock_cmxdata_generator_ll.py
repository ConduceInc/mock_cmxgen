"""CMX mock data generator fwith coords in Latd/Long in USA.

Usage: mock_cmxdata_generator.py [options]

Options:
    -e <entity_count>, --entity-count=<entity_count>  Number of entities to generate [default: 100]
    -p <update_period>, --period=<update_period>  Time (secs) between location updates [default: 15]
    -m <max_speed>, --max-speed=<max_speed>  Max forklift speed (KPH)) [default: 30]
    -s <start_date>, --start-date=<date>  Start date [default: 2016-09-01]
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
import math
import time
import requests

ENTITY_COUNT=100
entities=[]

# -------------------- Map

# Coords of map in latd/long
MAP_BOTTOM_LEFT=(30.00,-100.02)
MAP_TOP_RIGHT=(30.02,-100.00)

LATD_MULT=0.0
LONG_MULT=0.0

def distance_on_geoid(lat1, lon1, lat2, lon2):
    # Convert degrees to radians
    lat1 = lat1 * math.pi / 180.0;
    lon1 = lon1 * math.pi / 180.0;
    
    lat2 = lat2 * math.pi / 180.0;
    lon2 = lon2 * math.pi / 180.0;
    
    # radius of earth in meters
    r = 6378100.0;
    
    # P
    rho1 = r * math.cos(lat1);
    z1 = r * math.sin(lat1);
    x1 = rho1 * math.cos(lon1);
    y1 = rho1 * math.sin(lon1);
    
    # Q
    rho2 = r * math.cos(lat2);
    z2 = r * math.sin(lat2);
    x2 = rho2 * math.cos(lon2);
    y2 = rho2 * math.sin(lon2);
    
    # Dot product
    dot = (x1 * x2 + y1 * y2 + z1 * z2);
    cos_theta = dot / (r * r);
    
    theta = math.acos(cos_theta);
    
    # Distance in meters
    return r * theta;


def getDegreesPerMeter( bl, tr ):
    #print bl, tr
    latDist = distance_on_geoid( bl[0], bl[1], tr[0], bl[1] )
    #print "Distance Latd (m): ", latDist
    latdDelta = tr[0] - bl[0]
    #print "Latd Delta: ", latdDelta
    latdDegPerMeter = latdDelta / latDist
    #print "Latd dec degrees / meter: ", latdDelta / latDist
    
    lonDist = distance_on_geoid( bl[0], bl[1], bl[0], tr[1] )
    #print "Distance Long (m): ", lonDist
    lonDelta = tr[1] - bl[1]
    #print "Long Delta: ", latdDelta
    longDegPerMeter = latdDelta / lonDist
    #print "Long dec degrees / meter: ", latdDelta / lonDist
    return ( latdDegPerMeter, longDegPerMeter )


#
# Calculate the distance that the entity should move
#
def getDelta( speedMPS, multiplier ):
    #print "Distance travelled: ", speedMPS * UPDATE_PERIOD_S
    return (speedMPS * UPDATE_PERIOD_S * multiplier)

# -------------------- Entity speed

# Range of entity speeds when moving (in KPH)
SPEED_MIN_KPH=3
SPEED_MAX_KPH=30

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

START_DATE='2016-09-01'
START_TIME='06:00'
STOP_TIME='18:00'

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

    path = []
    pt = { "x":float(startLoc[1]), "y":float(startLoc[0]), "z":0 }
    path.append(pt)

    for i in range(entityCount):
        entity = {
            "identity": str(i+1),
            "kind": "entity",
            "timestamp-ms": int(startTime * 1000),
            "path": copy.deepcopy(path),
            "attrs": copy.deepcopy(attribs)
        }
        entities.append(entity);

MOVE_STAY = 0
MOVE_UP = 1
MOVE_DOWN = 2
MOVE_LEFT = 3
MOVE_RIGHT = 4

def getMove():
    val = random.uniform(0, 100)
    if ( val < 24 ):
        return MOVE_UP
    elif( val < 48 ):
        return MOVE_DOWN
    elif( val < 72 ):
        return MOVE_LEFT
    elif( val < 96 ):
        return MOVE_RIGHT
    else:
        return MOVE_STAY


#def getRandomMove():
#    newX = random.uniform(MAP_BOTTOM_LEFT[1], MAP_TOP_RIGHT[1])
#    newY = random.uniform(MAP_BOTTOM_LEFT[0], MAP_TOP_RIGHT[0])
#    return (newX, newY)

def getNextMove( move, pos, spd ):
    posX = pos[0] # Longitude
    posY = pos[1] # Latitude
    
    x = posX
    y = posY

    move = getMove()
    if ( move < 3 ): # moving UP/DOWN
        delta = getDelta( spd, LATD_MULT )
    else:
        delta = getDelta( spd, LONG_MULT )

    if ( move == MOVE_RIGHT ):
        x = min( (posX + delta), MAP_TOP_RIGHT[1] )
    
    elif ( move == MOVE_LEFT ):
        x = max( (posX - delta), MAP_BOTTOM_LEFT[1] )

    elif ( move == MOVE_UP ):
        y = min( (posY + delta), MAP_TOP_RIGHT[0] )
    
    else: # DOWN
        y = max( (posY - delta), MAP_BOTTOM_LEFT[0] )

    #print "post XY", x, y

    return (move, x, y)

#
# Given an entity, generate its next position and update the entry.
#
def getEntityMovement( entity ):
    move = getMove()
    #print "Next Move (Pre): ", move
    
    if ( move == MOVE_STAY ):
        return

    # Assume the path location we're updating is the first/only one
    pos = entity.get("path")[0]
    posX = pos["x"]
    posY = pos["y"]

    # Speed varies for each move
    newSpd = getSpeed()
    newMove = getNextMove( move, (posX, posY), newSpd )

    #print pos, newMove
    # Update entity
    pos["x"] = newMove[1]
    pos["y"] = newMove[2]


def updateLocations(tm):
    newTime = tm * 1000
    for entity in entities:
        entity["timestamp-ms"] = newTime
        getEntityMovement(entity)

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
        tm = entity.get("timestamp-ms") / 1000
        pos = entity.get("path")[0]
        posX = pos["x"]
        posY = pos["y"]
        print "%d, forklift, %s, %.6f, %.6f" % (tm, entity["identity"], posY, posX)

def printEntities():
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

    global ENTITY_COUNT
    global UPDATE_PERIOD_S
    global SPEED_MAX_KPH
    global START_DATE
    global LATD_MULT
    global LONG_MULT
    
    ENTITY_COUNT = int(arguments.get('--entity-count'))
    UPDATE_PERIOD_S = int(arguments.get('--period'))
    SPEED_MAX_KPH = int(arguments.get('--max-speed'))
    START_DATE = arguments.get('--start-date')

    hostServer = arguments.get('--host')
    apiKey = str(arguments.get('--apikey'))
    datasetId = str(arguments.get('--dataset'))

    llMult = getDegreesPerMeter( MAP_BOTTOM_LEFT, MAP_TOP_RIGHT )
    LATD_MULT = llMult[0]
    LONG_MULT = llMult[1]
    #print "LATD/LONG MULT ", llMult

    startLoc = MAP_BOTTOM_LEFT

    nDays = getDays(START_DATE)
    startDateTime = getTime(START_DATE, START_TIME)
    stopDateTime = getTime(START_DATE, STOP_TIME)
    
    initEntities(ENTITY_COUNT, startDateTime, startLoc);
    uploadEntities(apiKey, datasetId, hostServer, startDateTime)

    for i in range(nDays):
        tm = startDateTime
        #print "------", datetime.datetime.fromtimestamp(tm).strftime('%Y-%m-%d %H:%M:%S')
        #printEntities()

        while ( tm < stopDateTime ):
            tm += UPDATE_PERIOD_S
            #print datetime.datetime.fromtimestamp(tm).strftime('%Y-%m-%d %H:%M:%S')
            updateLocations(tm)
            #printEntities()
            uploadEntities(apiKey, datasetId, hostServer, tm)
        
        # Jump to next day
        startDateTime += 86400
        stopDateTime += 86400

if __name__=="__main__":
    main()
