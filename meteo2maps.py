import os, sys, urllib.request, json, subprocess, geopandas, pandas,\
shutil, psycopg2, re, requests, time, warnings
from datetime import datetime, timedelta
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
# Reading hidden password from txt file in dir
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


# Reading hidden Open Weather Map API key from txt file
apiKey = open(os.path.join('apikey.txt'), 'r').readline()
apiKey = str(apiKey)

# Meteo request to API
def requestOWM(coordDic, apiKey):
    '''
    Função para recolha dos dados meteorológicos provenientes da OpenWeather One Call API.
    Exige ao utilizador a introduçaõ interativa do tipo de pedido de mapa meteorológico.
    Devolve uma dataframe (pandas) das váriáveis meteorológicas por distrito para o momento indicado.
    '''
    # Requesting input
    while True:
        requestType = input('\n6 - Specify the wanted type of meteomap request [Y for yesterday, N for now, T for tomorrow]:' )
        if not re.match('[YNT]', requestType):
            if requestType == 'EXIT':
                sys.exit()
            else:
                print('Please, limit your input to Y or N or T or exit with \'EXIT\'.')
                time.sleep(2)
        else:
            break
    # Dealing with user request
    forecast = []
    for item in coord.items():
        lat = str(item[1][1])
        long = str(item[1][0])
        # For yesterday's weather
        if requestType == 'Y':
            yesterday = datetime.now() - timedelta(days=1)
            unixTimestamp = int(yesterday.timestamp())
            url = 'https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={}&lon={}&dt={}&appid={}&units=metric'\
            .format(lat, long, unixTimestamp, apiKey)
            # To be continued
        # For current weather
        elif requestType == 'N':
            url = 'https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude=minutely,hourly,daily,alerts&appid={}&units=metric'\
            .format(lat, long, apiKey) # https://api.openweathermap.org/data/2.5/onecall?lat=33.441792&lon=-94.037689&exclude=minutely,hourly,daily,alerts&appid=44cfad7f82ec61d3f420de3201b703d4
            with urllib.request.urlopen(url) as url:
                data = json.loads(url.read().decode())
                districtForecast = {}
                districtForecast['distrito'] = item[0]
                districtForecast['forecast_date'] = datetime.utcfromtimestamp(data.get('current').get('dt')).strftime('%d-%m-%Y')
                districtForecast['forecast_time'] = datetime.utcfromtimestamp(data.get('current').get('dt')).strftime('%H:%M:%S')
                main = data.get('current').get('weather')
                for item in main:
                    districtForecast['weather_desc'] = item.get('main')
                districtForecast['temperature'] = data.get('current').get('temp')
                districtForecast['feels_like'] = data.get('current').get('feels_like')
                districtForecast['pressure'] = data.get('current').get('pressure')
                districtForecast['humidity'] = data.get('current').get('humidity')
                districtForecast['dew_point'] = data.get('current').get('dew_point')
                districtForecast['ultrav_index'] = data.get('current').get('uvi')
                districtForecast['wind_speed'] = data.get('current').get('wind_speed')
                districtForecast['wind_deg'] = data.get('current').get('wind_deg')
                forecast.append(districtForecast)
        # For tomorrow's weather
        elif requestType == 'T':
            url = 'https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude=current,minutly,hourly,alerts&appid={}&units=metric'\
            .format(lat, long, apiKey) # https://api.openweathermap.org/data/2.5/onecall?lat=33.441792&lon=-94.037689&exclude=current,minutly,hourly,alerts&appid=44cfad7f82ec61d3f420de3201b703d4
            with urllib.request.urlopen(url) as url:
                data = json.loads(url.read().decode())
                data = data['daily'][1]
                districtForecast = {}
                districtForecast['distrito'] = item[0]
                districtForecast['forecast_date'] = datetime.utcfromtimestamp(data.get('dt')).strftime('%d-%m-%Y')
                districtForecast['forecast_time'] = datetime.utcfromtimestamp(data.get('dt')).strftime('%H:%M:%S')
                main = data.get('weather')
                for item in main:
                    districtForecast['weather_desc'] = item.get('main')
                districtForecast['temperature'] = data.get('temp').get('day')
                districtForecast['feels_like'] = data.get('feels_like').get('day')
                districtForecast['pressure'] = data.get('pressure')
                districtForecast['humidity'] = data.get('humidity')
                districtForecast['dew_point'] = data.get('dew_point')
                districtForecast['ultrav_index'] = data.get('uvi')
                districtForecast['wind_speed'] = data.get('wind_speed')
                districtForecast['wind_deg'] = data.get('wind_deg')
                forecast.append(districtForecast)
    forecast_df = pandas.DataFrame(forecast)
    return forecast_df

print(requestOWM(coord, apiKey))