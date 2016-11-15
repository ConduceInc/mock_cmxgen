"""Mock data generator.

Usage: mock_data_generator.py [options]

Options:
    -e <entity_count>, --entity-count=<entity_count>  Number of entities to generate [default: 100]
    -p <update_period>, --period=<update_period>  Time (secs) between location updates [default: 15]
    -m <max_speed>, --max-speed=<max_speed>  Max forklift speed (KPH)) [default: 5]
    -c <confidence>, --conf-dist=<distance>  Max location confidence radius (feet) [default: 1000.0]
    -d <start_date>, --start-date=<date>  Start date [default: 2016-01-01]
    -y <days>, --days=<n>  Number of days from start date to generate
    -s <start_time>, --start-time=<time>  Start time (HH:MM) [default: 06:00]
    -p <stop_time>, --stop-time=<time>  Stop time (HH:MM) [default: 18:00]
    -h <host>, --host=<server_name>  Name of Conduce server [default: dev-app.conduce.com]
    -a <api_key>, --apikey=<key>  API Key
    -x <cmx_dataset>, --cmx-dataset=<id>  CMX Dataset ID (Entities)
    -i <ims_dataset>, --ims-dataset=<id>  Optional IMS Dataset ID (Impacts)
    -g <format> --gen=<format>  Generate output in the given format (JSON | CSV) only; no uploading

"""

from docopt import docopt
import time
from datetime import datetime
from datetime import timedelta
from calendar import monthrange
import math
import random
import copy
import json
import os
import sys
import httplib,urllib
import requests



ENTITY_COUNT=100
entities=[]

# Pick whether repeatable data is generated or not
RANDOM_SEED=101
#RANDOM_SEED=time.utcnow()

# Generate data for output in the given format; no uploading
GENONLY_FORMAT=None

#
# -------------------- Coordinates
#

# Coordinate system for map (note: XY is specified in meters)
COORDS_LL=0
COORDS_XY=1

# >>> Comment/uncomment one of these blocks
MAP_COORDS=COORDS_XY
MAP_BOTTOM_LEFT=(0.0,0.0)
MAP_TOP_RIGHT=(150.0,160.0)

#MAP_COORDS=COORDS_LL
#MAP_BOTTOM_LEFT=(30.00,-100.02)
#MAP_TOP_RIGHT=(30.02,-100.00)


#
# -------------------- Latd/Long
#
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


# -------------------- Speed/Distance

# Range of entity speeds (in KPH)
SPEED_MIN_KPH=1
SPEED_MAX_KPH=5

X_AXIS_METERS=0
Y_AXIS_METERS=0

# Calculate the total distance for each axis in meters
def initAxisDimensions(bl, tr):
    global X_AXIS_METERS
    global Y_AXIS_METERS
    global LATD_MULT
    global LONG_MULT

    if MAP_COORDS == COORDS_XY:
        X_AXIS_METERS = abs(tr[0] - bl[0])
        Y_AXIS_METERS = abs(tr[1] - bl[1])
        #print "Total Distance XY", X_AXIS_METERS, Y_AXIS_METERS
    else:
        X_AXIS_METERS = distance_on_geoid( bl[0], bl[1], bl[0], tr[1] )
        Y_AXIS_METERS = distance_on_geoid( bl[0], bl[1], tr[0], bl[1] )
        llMult = getDegreesPerMeter( MAP_BOTTOM_LEFT, MAP_TOP_RIGHT )
        LATD_MULT = llMult[0]
        LONG_MULT = llMult[1]
        #print "Total Distance LL, Multipliers", X_AXIS_METERS, Y_AXIS_METERS, llMult

# Generate a random speed in meters/sec
def getSpeedMPS():
    speedMPS = 0.277778 * random.randint(SPEED_MIN_KPH, SPEED_MAX_KPH)
    #print "Speed (MPS):", speedMPS
    return speedMPS

# Calculate the distance as a percent of total
def getDistancePercent( distMeters, maxDist ):
    #print "getDistancePercent", distMeters, maxDist
    return (distMeters / maxDist) * 100

# Calculate the distance moved, rounded to nearest meter
def getDistanceMeters( speedMPS, tmSecs ):
    return int(round(speedMPS * tmSecs))


# -------------------- Date/Time

# Seconds between location updates
UPDATE_PERIOD_S=15

TIME_OFFSET = int(round((datetime.now() - datetime.utcnow()).total_seconds()))

START_DATE='2016-01-01'
START_TIME='06:00'
STOP_TIME='18:00'

# Concatenate the date/time and return the time in seconds.
def getTime(dateStr, timeStr):
    date_time = dateStr + " " + timeStr
    return int( time.mktime( time.strptime(date_time, '%Y-%m-%d %H:%M') ) )

# Parse the date for the month and return how many days.
def getDays(dateStr):
    dt_obj = datetime.strptime(dateStr, '%Y-%m-%d')
    n = monthrange(dt_obj.year, dt_obj.month)[1]
    return n

# Print the time elapsed
def printElapsedTime(strMsg, startTime, endTime):
    hours, rem = divmod(endTime - startTime, 3600)
    minutes, seconds = divmod(rem, 60)
    if minutes > 0:
        print "%s %02d mins %02.3f secs" % (strMsg, int(minutes), seconds)
    else:
        print "%s %02.3f secs" % (strMsg, seconds)

# Delay the required amount til the next update period
def delayTime(periodSecs, startTime, endTime):
    hours, rem = divmod(endTime - startTime, 3600)
    minutes, seconds = divmod(rem, 60)
    if minutes > 0:
        totalElapsed = (minutes * 60) + seconds
    else:
        totalElapsed = seconds

    delaySecs = 0.0
    if totalElapsed < periodSecs:
        delaySecs = float(periodSecs) - float(totalElapsed)
        print "Delay %02.3f" % (delaySecs)
        time.sleep(delaySecs)


# -------------------- Entity operations


# Generate a confidence value for an entity poistion
# The size of the circle around the entity location (feet), so
# a larger circle == less confident of its position accuracy.
MIN_CONFIDENCE_VALUE=10.0
MAX_CONFIDENCE_VALUE=1000.0

# Convert confidence factor in (ft) to substrate units (mm)
def getPositionConfidence():
    return int(round(0.3048 * 1000 * random.triangular(MIN_CONFIDENCE_VALUE, MAX_CONFIDENCE_VALUE, 320.0), 0))

#
# Init and return a list of entities.
#
def initEntities(entityCount, startTime, startLoc):
    #print "Entities: ", entityCount

    attribs = [
        { "key":"heading", "type":"INT64", "int64-value":0 },
        { "key":"confidence", "type":"INT64", "int64-value":0 }
    ]

    loc = []
    pt = { "x":float(startLoc[0]), "y":float(startLoc[1]), "z":0 }
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

#
# Movement
#
MOVE_UP = 1
MOVE_DOWN = 2
MOVE_LEFT = 3
MOVE_RIGHT = 4

def getMove():
    val = random.randint(1, 100)
    if ( val < 26 ):
        return MOVE_UP
    elif( val < 51 ):
        return MOVE_DOWN
    elif( val < 76 ):
        return MOVE_LEFT
    elif( val < 101 ):
        return MOVE_RIGHT

def getNextMove( posX, posY ):
    move = getMove()
    x = posX
    y = posY
    #print "Pre:", move, x, y
    hdg = 0

    d = getDistanceMeters( getSpeedMPS(), UPDATE_PERIOD_S )
    
    if ( move == MOVE_UP ):
        hdg = 0
        dyPct = getDistancePercent( d, Y_AXIS_METERS )
        y = min( (posY + dyPct), 100.0 )

    elif (move == MOVE_DOWN ):
        hdg = 180
        dyPct = getDistancePercent( d, Y_AXIS_METERS )
        y = max( (posY - dyPct), 0.0 )

    elif ( move == MOVE_LEFT ):
        hdg = 90
        dxPct = getDistancePercent( d, X_AXIS_METERS )
        x = max( (posX - dxPct), 0.0 )

    else: # MOVE_RIGHT
        hdg = 270
        dxPct = getDistancePercent( d, X_AXIS_METERS )
        x = min( (posX + dxPct), 100.0 )

    #print "Move:", hdg, d, x, y
    return (hdg, x, y)

# Generate the next movement and update the entity.
def updateEntityPosition( entity ):
    # Assume the path location we're updating is the first/only one?
    pos = entity.get("path")[0]
    newMove = getNextMove( pos["x"], pos["y"] )
    pos["x"] = newMove[1]
    pos["y"] = newMove[2]
    hdg = entity.get("attrs")[0]
    hdg["int64-value"] = newMove[0]
    conf = entity.get("attrs")[1]
    conf["int64-value"] = getPositionConfidence()

# Update all the entities locations for the given time
def updateLocations(tm):
    for entity in entities:
        entity["timestamp-ms"] = tm * 1000
        updateEntityPosition(entity)

# Translate the XY (percentage distance from the origin) to the coordinate system.
def getMappedPosition( xPct, yPct ):
    #print "Map Pct", xPct, yPct
    if MAP_COORDS == COORDS_XY:
        newX = MAP_BOTTOM_LEFT[0] + (xPct/100.0) * X_AXIS_METERS
        newY = MAP_BOTTOM_LEFT[1] + (yPct/100.0) * Y_AXIS_METERS
        #print "Map (XY)", (xPct, yPct), (newX, newY)
        # Convert to mm
        newX = int(newX * 1000)
        newY = int(newY * 1000)
        #print "Map mm (XY)", (newX, newY)
    else:
        newX = MAP_BOTTOM_LEFT[1] + ((xPct/100.0) * X_AXIS_METERS) * LONG_MULT #LON
        newY = MAP_BOTTOM_LEFT[0] + ((yPct/100.0) * Y_AXIS_METERS) * LATD_MULT #LAT
        #print "Map (LL)", (xPct, yPct), (newX, newY)
    return (newX, newY)

# Get the mapped positions for all entities.
def getMappedEntityPositions():
    mappedEntities = []
    m = {}
    for entity in entities:
        pos = entity.get("path")[0]
        newPos = getMappedPosition( pos["x"], pos["y"] )
        
        me = copy.deepcopy(entity)
        mePos = me.get("path")[0]
        mePos["x"] = newPos[0]
        mePos["y"] = newPos[1]
        #print "XY ", entity
        #print "Map", me
        mappedEntities.append(me)
    return mappedEntities


#
# -------------------- Impacts
#

# Return a random time 30 secs to 30 minutes in the future
def getNextImpactTime(dt):
    timestampMs = dt + random.randint(30, 1800)
    #print ">>> Next Impact", timestampMs, datetime.fromtimestamp(timestampMs).strftime('%Y-%m-%d %H:%M:%S')
    return timestampMs

# Return one of the impact levels and it's data.
IMPACT_LEVEL_1_FORCE = "5.6 G / 2.5 G"
IMPACT_LEVEL_2_FORCE = "6.4 G / 4.1 G"
IMPACT_LEVEL_3_FORCE = "7.7 G / 6.3 G"

def getImpact():
    val = random.randint(1, 100)
    if ( val < 61 ):
        return (1, IMPACT_LEVEL_1_FORCE)
    elif( val < 91 ):
        return (2, IMPACT_LEVEL_2_FORCE)
    else:
        return (3, IMPACT_LEVEL_3_FORCE)

#
# Get an impact entity based on the mapped entity and the impact info.
#
IMPACT_COUNTER=0
def getImpactEntity(mappedEntity, impact):
    global IMPACT_COUNTER
    
    IMPACT_COUNTER += 1
    
    impact_entity = {
        "timestamp-ms": mappedEntity["timestamp-ms"],
        "identity": str(IMPACT_COUNTER),
        "kind": "impact",
        "attrs": [
            {
                "int64-value": impact[0],
                "type": "INT64",
                "key": "level"
            },
            {
                "str-value": str(impact[1]),
                "type": "STRING",
                "key": "intensity"
            }],
        "path": copy.deepcopy(mappedEntity["path"]),
    }

    #print mappedEntity
    return impact_entity


# -------------------- Output formatting

#
# Create a JSON representation of the entities suitable for upload.
#
def getConduceEntitySetJSON(ents):
    myEntities = {}
    myEntities["entities"] = ents

    if GENONLY_FORMAT == "JSON":
        return json.dumps(myEntities, indent=2)
    else:
        return json.dumps(myEntities, separators=(',',':'))

def printCSV(ents):
    entity = {}
    for entity in ents:
        pos = entity.get("path")[0]
        posX = pos["x"]
        posY = pos["y"]
        conf = entity.get("attrs")[1]
        confVal = conf["double-value"]
        print "%d, %s, %s, %.9f, %.9f, %.1f" % (entity["timestamp-ms"]/1000, entity["identity"], "employee", posX, posY, confVal)

def printEntities(ents):
    if GENONLY_FORMAT == "JSON":
        print getConduceEntitySetJSON(ents)
    else:
        printCSV(ents)


#
# -------------------- Upload to Conduce
#
def waitForUploadJob(authStr, jobURL):
    headers = { 'Authorization': authStr }
    #print headers
    
    finished = False
    while not finished:
        time.sleep(1.0)
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

def uploadEntities(apiKey, datasetId, hostServer, timestampMs, ents):
    if GENONLY_FORMAT:
        printEntities(ents)
        return

    strGenDate = datetime.fromtimestamp(timestampMs).strftime('%Y-%m-%d %H:%M:%S')

    authStr = 'Bearer ' + apiKey
    URI = '/conduce/api/datasets/add_datav2/' + datasetId
    payload = getConduceEntitySetJSON(ents)
    headers = {
        'Authorization': authStr,
        'Content-type': 'application/json',
        'Content-Length': len(payload)
    }
    print "%s: Uploading..." % (strGenDate)
    #print headers
    #print payload

    tStart = time.time()
    connection = httplib.HTTPSConnection(hostServer)
    connection.request("POST", URI, payload, headers)
    response = connection.getresponse()
    print response.status, response.reason, response.read()
    connection.close()

    # Wait for the job to finish
    job_loc = response.getheader('location')
    if job_loc:
        jobURL = "https://%s/conduce/api%s" % (hostServer, job_loc)
        waitForUploadJob( authStr, jobURL )
        tStop = time.time()
        printElapsedTime( strGenDate + " Duration:", tStart, tStop )
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
    global START_TIME
    global STOP_TIME

    global MAP_COORDS
    global MAX_CONFIDENCE_VALUE
    global GENONLY_FORMAT

    random.seed(RANDOM_SEED)
    
    gen = arguments.get('--gen')
    if gen:
        if (gen == "JSON") or (gen == "CSV"):
            GENONLY_FORMAT=gen
        else:
            print "Unsupported output format:", gen
            return

    ENTITY_COUNT = int(arguments.get('--entity-count'))
    UPDATE_PERIOD_S = int(arguments.get('--period'))
    SPEED_MAX_KPH = int(arguments.get('--max-speed'))
    MAX_CONFIDENCE_VALUE = float(arguments.get('--conf-dist'))
    START_DATE = arguments.get('--start-date')
    START_TIME = arguments.get('--start-time')
    STOP_TIME = arguments.get('--stop-time')

    startDateTime = getTime(START_DATE, START_TIME) + TIME_OFFSET
    stopDateTime = getTime(START_DATE, STOP_TIME) + TIME_OFFSET

    nDays = 1
    strDays = arguments.get('--days')
    if strDays == None:
        nDays = getDays(START_DATE)
    else:
        nDays = int(strDays)
    #print "Date %s %s-%s Days=%d" %(START_DATE, START_TIME, STOP_TIME, nDays)

    hostServer=None
    apiKey=None
    datasetIdCMX=None
    datasetIdIMS=None

    if not GENONLY_FORMAT:
        hostServer = arguments.get('--host')
        apiKey = str(arguments.get('--apikey'))
        datasetIdCMX = str(arguments.get('--cmx-dataset'))
        datasetIdIMS = str(arguments.get('--ims-dataset'))

        if apiKey == 'None':
            print "API Key required."
            return
        if datasetIdCMX == 'None':
            print "CMX Dataset ID required."
            return
        if datasetIdIMS == 'None':
            datasetIdIMS = None

        #print apiKey
        #print datasetIdCMX
        #print datasetIdIMS

    # -----------------

    initAxisDimensions(MAP_BOTTOM_LEFT, MAP_TOP_RIGHT)
    initEntities(ENTITY_COUNT, startDateTime, (0,0));

    mapped = []
    impacts = []
    for i in range(nDays):
        tm = startDateTime
        impactTm = getNextImpactTime(tm)

        mapped = getMappedEntityPositions()
        uploadEntities(apiKey, datasetIdCMX, hostServer, startDateTime, mapped)

        while ( tm < stopDateTime ):
            if datasetIdIMS and tm >= impactTm:
                impact = getImpactEntity( mapped[random.randint(0, ENTITY_COUNT-1)], getImpact() )
                impacts.append(impact)
                #print "Impact:", impact
                impactTm = getNextImpactTime(tm)
            
            tm += UPDATE_PERIOD_S
            updateLocations(tm)
            mapped = getMappedEntityPositions()
            tBeg = time.time()
            uploadEntities(apiKey, datasetIdCMX, hostServer, tm, mapped)
            tEnd = time.time()
            delayTime(UPDATE_PERIOD_S, tBeg, tEnd)

        if datasetIdIMS and len(impacts) > 0:
            #print "Numberof Impacts Today:", len(impacts)
            #print impacts
            uploadEntities(apiKey, datasetIdIMS, hostServer, tm, impacts)
            # Empty the list
            del(impacts[:])

        # Jump to next day
        startDateTime += 86400
        stopDateTime += 86400

if __name__=="__main__":
    main()
