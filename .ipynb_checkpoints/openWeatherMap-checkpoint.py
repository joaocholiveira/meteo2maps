import os, urllib.request, json, shapefile, shutil, subprocess, csv, fiona, shapely, geopandas
from osgeo import gdal, ogr, osr
from datetime import datetime

os.environ['SHAPE_ENCODING'] = "utf-8"
basePath = 'C:\saprog\projeto'
os.chdir(basePath)

def deleteAndCreateOutput():
    print ('A limpar e criar directoria de outputs')
    outDir = os.path.join(basePath, 'output')
    if os.path.isdir(outDir):
        shutil.rmtree(outDir)
    os.mkdir(outDir)

deleteAndCreateOutput()

# caop = shapefile.Reader('caop')
# print(type(caop))
# print(caop.bbox,'\n',caop.shapeType,'\n',caop.numRecords)
# print('caop fields:\n',caop.fields)

caop = geopandas.read_file('caop.shp')
caop.plot()


apiKey = '44cfad7f82ec61d3f420de3201b703d4'
coordinates = {'lat': '39.3137278', 'long': '-9.1828055'}


def getWeather(coord, api):
    # url = 'http://api.openweathermap.org/data/2.5/weather?lat='+coord.get('lat')+'&lon='+coord.get('long')+\
    # '&appid='+api+'&units=metric'
    url = 'https://api.openweathermap.org/data/2.5/onecall?lat=' + coord.get('lat') + '&lon=' + coord.get('long') + \
          '&exclude=current,minutely,daily,alerts&appid=' + api + '&units=metric'
    print(url)
    with urllib.request.urlopen(url) as url:
        # data = json.loads(url.read().decode())
        data = json.loads(url.read().decode())
    return data


# data = getWeather(coordinates, apiKey)

# print(data)

# ts = int('1615719600')
# date = datetime.utcfromtimestamp(ts).strftime('%d-%m-%Y')
# time = datetime.utcfromtimestamp(ts).strftime('%H:%M:%S')
# print(date, 'and', time)

# def cleanThatMess(jsonData):
#     cleanData = {'locationName':'',
#     'locationCoord':'',
#     'date':'',
#     'time':'',
#     'tempNow':0,
#     'tempMax':0,
#     'tempMin':0,
#     'pressure':0,
#     'humidity':0,
#     'visibility':0,
#     'windSpeed':0,
#     'windDir':0}
#     for i in jsonData:
