import os, urllib.request, json, subprocess, geopandas, pandas,\
shutil, psycopg2, re, requests, time
from datetime import datetime
from psycopg2 import extras as psy2extras
from geo.Geoserver import Geoserver


os.environ['SHAPE_ENCODING'] = "utf-8"
basePath = 'C:\\saprog\\projeto'
outputPath = 'C:\\saprog\\projeto\\output\\'
os.chdir(basePath)

def deleteAndCreateOutput():
    print ('A limpar e criar directoria de outputs...')
    outDir = os.path.join(basePath, 'output')
    if os.path.isdir(outDir):
        shutil.rmtree(outDir)
    os.mkdir(outDir)

# deleteAndCreateOutput()


print('Working on initial steps... Please, wait a moment')

if os.path.exists(outputPath+'distritos.shp') == False:
    print('distritos.shp not found')
    # Reading main CAOP shapefile (it needs to already exist)
    caop = geopandas.read_file('caop.shp', encoding='utf-8')
    # Executing dissolve from parishes to districts
    districts = caop.dissolve(by = 'Distrito')
    # Saving dissolve output as shapefile
    districts.to_file(outputPath+'distritos.shp', encoding='utf-8')
else:
    print('found distritos.shp')
    # Reading distritos shapefile that already exists
    districts = geopandas.read_file(outputPath+'distritos.shp', encoding='utf-8')

# if os.path.exists('\\output\\centroides.shp') == False:
#     print('centroides.shp not found')
#     # Extracting distric centroids to aquire forecast coordinates
#     centroids = districts.centroid
#     # Saving centroides output as shapefile
#     centroids.to_file(outputPath+'centroides.shp', encoding='utf-8')
# else:
#     print('found centroides.shp')
#     # Reading distritos shapefile that already exists
#     centroids = geopandas.read_file('\\output\\centroides.shp', encoding='utf-8')

# # Structuring coordinates into two dictionaries {'district':'x or y coordinate'}
# coordX = centroids.x.to_dict()
# coordY = centroids.y.to_dict()

# Converting separate coordinate dictionaries (x, y) to a actually usable dictionary
def getCoordTogether(dicX, dicY):
    '''
    Extração de coordenadas geográficas úteis ao harvest de dados meteorológicos.
    Devolve um dicionário onde as chaves são os distritos, à qual está associado
    um tuplo com as coordenadas Lat Long.
    '''
    coord = [dicX, dicY]
    coordDic = {}
    for i in dicX.keys():
        coordDic[i] = tuple(coordDic[i] for coordDic in coord)
    return coordDic

# coord = getCoordTogether(coordX, coordY)

# print(coord)

# # Check the existence of districts table in meteo PG database
# pgPassword = open(os.path.join('pw.txt'), 'r').readline()
# pgPassword = str(password)

# con = psycopg2.connect(dbname='meteo', user='postgres', password=password,\
# host='localhost', port='5432')
