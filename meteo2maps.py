import os, urllib.request, json, subprocess, geopandas, pandas,\
shutil, psycopg2, re, requests, time, warnings
from datetime import datetime
from psycopg2 import extras as psy2extras
from geo.Geoserver import Geoserver

# To disable geopandas CRS warnings in terminal
warnings.filterwarnings('ignore')

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


print('Working on initial steps... Please, wait a moment.')

# Working on districts
if os.path.exists(outputPath+'distritos.shp') == False:
    print('distritos.shp not found. Let\'s work on it.')
    # Reading main CAOP shapefile (it needs to already exist)
    caop = geopandas.read_file('caop.shp', encoding='utf-8')
    # Executing dissolve from parishes to districts
    districts = caop.dissolve(by = 'Distrito')
    # Saving dissolve output as shapefile and reading it
    districts.to_file(outputPath+'distritos.shp', encoding='utf-8')
    districts = geopandas.read_file(outputPath+'distritos.shp', encoding='utf-8')
    # # districts = districts.to_crs(3763)
else:
    print('found distritos.shp')
    # Reading distritos shapefile that already exists
    districts = geopandas.read_file(outputPath+'distritos.shp', encoding='utf-8')
    # # districts = districts.to_crs(3763)

def getCoordTogether(geoDataFrame):
    '''
    Extração de coordenadas geográficas úteis ao harvest de dados meteorológicos.
    Devolve um dicionário onde as chaves são os distritos, à qual está associado
    um tuplo com as coordenadas Lat Long.
    '''
    pre = districts.set_index('Distrito')
    coordDicX = pre.geometry.centroid.x.to_dict()
    coordDicY = pre.geometry.centroid.y.to_dict()
    coord = [coordDicX, coordDicY]
    coordDic = {}
    for i in coordDicX.keys():
        coordDic[i] = tuple(coordDic[i] for coordDic in coord)
    return coordDic

coord = getCoordTogether(districts)

# print(coord)


# Check the existence of districts table in meteo PG database
pgPassword = open(os.path.join('pw.txt'), 'r').readline()
pgPassword = str(pgPassword)

conn = psycopg2.connect(dbname='meteo', user='postgres', password=pgPassword,\
host='localhost', port='5432')

def checkPgTable(connectionParameters, table):
    '''
    '''
    cur = connectionParameters.cursor()
    cur.execute('SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE\
    table_catalog=\'meteo\' AND table_schema=\'public\' AND table_name=\'{}\');'.format(table))
    return(bool(cur.rowcount))

print(checkPgTable(conn, 'centroides'))