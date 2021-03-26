import os, urllib.request, json, subprocess, geopandas, pandas,\
shutil, psycopg2, re, requests, time, warnings
from datetime import datetime
from psycopg2 import extras as psy2extras
from geo.Geoserver import Geoserver

# camelCase style adopted

# To disable geopandas CRS warnings in terminal
warnings.filterwarnings('ignore')

os.environ['SHAPE_ENCODING'] = "utf-8"
basePath = 'C:\\saprog\\projeto'
outputPath = 'C:\\saprog\\projeto\\output\\'
os.chdir(basePath)


print('\n1 - Working on initial steps... Please, wait a moment.')

def outputDir():
    if os.path.exists(outputPath) == False:
        os.mkdir(basePath, 'output')
        print('\n2 - Output dir. created.')
    else:
        print('\n2 - Found output dir.')

outputDir()


# Working on districts
if os.path.exists(outputPath+'districts.shp') == False:
    print('\n3 - districts.shp not found. Let\'s work on it.')
    # Reading main CAOP shapefile (it needs to already exist)
    caop = geopandas.read_file('caop.shp', encoding='utf-8')
    # Executing dissolve from parishes to districts
    districts = caop.dissolve(by = 'Distrito')
    # Saving dissolve output as shapefile and reading it
    districts.to_file(outputPath+'districts.shp', encoding='utf-8')
    districts = geopandas.read_file(outputPath+'districts.shp', encoding='utf-8')
    # # districts = districts.to_crs(3763)
else:
    print('\n3 - Found districts.shp')
    # Reading districts shapefile that already exists
    districts = geopandas.read_file(outputPath+'districts.shp', encoding='utf-8')
    # # districts = districts.to_crs(3763)

def getCoordTogether(geoDataFrame):
    '''
    Extração de coordenadas geográficas úteis ao harvest de dados meteorológicos.
    Devolve um dicionário onde as chaves são os districts, à qual está associado
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
print('\n4 - District coordinates compiled.')

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

if checkPgTable(conn, 'districts') == False:
    # Loading table to meteo databaase
    command = ["C:\\OSGeo4W64\\bin\\ogr2ogr.exe",
          "-f", "PostgreSQL",
          "PG:host=localhost user=postgres dbname=meteo password=3763", outputPath,
          "-lco", "GEOMETRY_NAME=the_geom", "-lco", "FID=gid", "-lco",
          "PRECISION=no", "-nlt", "PROMOTE_TO_MULTI", "-nln", "districts", "-overwrite"]
    subprocess.check_call(command)
    print('\n5 - Districts table loaded into meteo database.')
else:
    print('\n5 - Districts table already exists in meteo database.')


# Meteo request to API
def requestOWM():
    '''
    Função para recolha dos dados meteorológicos provenientes da OpenWeather One Call API.
    Exige ao utilizador a introduçaõ interativa do tipo de pedido de mapa meteorológico.
    Devolve uma dataframe (pandas) das váriáveis meteorológicas por distrito para o momento indicado.
    '''
    requestType = input('\n6 - Specify the wanted type of meteomap request [Y for yesterday, N for now, T for tomorrow]:' )
    if requestType == 'Y':
        
    elif requestType == 'N':

    elif requestType == 'T':
        print('bela shit')

requestOWM()