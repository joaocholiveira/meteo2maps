import os, urllib.request, json, subprocess, geopandas, pandas,/
shutil, psycopg2, re, requests, time
from datetime import datetime
from psycopg2 import extras as psy2extras
from geo.Geoserver import Geoserver


os.environ['SHAPE_ENCODING'] = "utf-8"
basePath = 'C:\\saprog\\projeto'
outputPath = 'C:\\saprog\\projeto\\output'
os.chdir(basePath)

def deleteAndCreateOutput():
    print ('A limpar e criar directoria de outputs...')
    outDir = os.path.join(basePath, 'output')
    if os.path.isdir(outDir):
        shutil.rmtree(outDir)
    os.mkdir(outDir)

deleteAndCreateOutput()


print('Working on initial steps... Please, wait a moment')

caop = geopandas.read_file('caop.shp', encoding='utf-8')

os.chdir(outputPath)

# Executing dissolve from parishes to districts
distritos = caop.dissolve(by = 'Distrito')

# Saving dissolve output as shapefile
distritos.to_file('distritos.shp', encoding='utf-8')

# Extracting distric centroids to aquire forecast coordinates
centroides = distritos.centroid
centroides.to_file('centroides.shp', encoding='utf-8')

# Structuring coordinates into two dictionaries {'district':'x or y coordinate'}
coordX = centroides.x.to_dict()
coordY = centroides.y.to_dict()

# Converting separate coordinate dictionaries (x, y) to a actualy usable dictionary
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

coord = getCoordTogether(coordX, coordY)

