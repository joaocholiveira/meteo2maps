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
# Reading ocult password from txt file in dir
pgPassword = open(os.path.join('pw.txt'), 'r').readline()
pgPassword = str(pgPassword)

# Defining PostgreSQL connection parameters
conn = psycopg2.connect(dbname='meteo', user='postgres', password=pgPassword,\
host='localhost', port='5432')

def checkPgTable(connectionParameters, table):
    '''
    Função para verificação de presença/ausência de tabela dentro de BD PostgreSQL.
    Devolve True se existir; False se não existir.
    '''
    cur = connectionParameters.cursor()
    cur.execute("select * from information_schema.tables where table_name=%s", (table,))
    return(bool(cur.rowcount))

# print(checkPgTable(conn, 'outra coisa'))

if checkPgTable(conn, 'distritos') == False:
    # Loading table to meteo databaase
    command = ["C:\\OSGeo4W64\\bin\\ogr2ogr.exe",
          "-f", "PostgreSQL",
          "PG:host=localhost user=postgres dbname=meteo password=3763", outputPath,
          "-lco", "GEOMETRY_NAME=the_geom", "-lco", "FID=gid", "-lco",
          "PRECISION=no", "-nlt", "PROMOTE_TO_MULTI", "-nln", "distritos", "-overwrite"]
    subprocess.check_call(command)
else:
    print('já lá tava oh nabo')