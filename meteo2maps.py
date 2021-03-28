import os, sys, urllib.request, json, subprocess, geopandas, pandas,\
shutil, psycopg2, re, requests, time, warnings
from datetime import datetime, timedelta
from psycopg2 import extras as psy2extras
from geo.Geoserver import Geoserver

# camelCase style adopted

# Execution time (start)
start_time = time.time()

# To disable geopandas CRS warnings in terminal
warnings.filterwarnings('ignore')

os.environ['SHAPE_ENCODING'] = "utf-8"
basePath = 'C:\\saprog\\projeto'
outputPath = 'C:\\saprog\\projeto\\output\\'
os.chdir(basePath)


print('\nWorking on initial steps... Please, wait a moment.')

def outputDir():
    if os.path.exists(outputPath) == False:
        os.mkdir(outputPath)
        print('\nOutput dir. created.')
    else:
        print('\nFound output dir.')

outputDir()


# Working on districts
if os.path.exists(outputPath+'districts.shp') == False:
    print('\ndistricts.shp not found. Let\'s work on it.')
    # Reading main CAOP shapefile (it needs to already exist)
    caop = geopandas.read_file('caop.shp', encoding='utf-8')
    # Executing dissolve from parishes to districts
    districts = caop.dissolve(by = 'Distrito')
    # Saving dissolve output as shapefile and reading it
    districts.to_file(outputPath+'districts.shp', encoding='utf-8')
    districts = geopandas.read_file(outputPath+'districts.shp', encoding='utf-8')
    # # districts = districts.to_crs(3763)
else:
    print('\nFound districts.shp')
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
print('\nDistrict coordinates compiled.')

# print(coord)


# Check the existence of districts table in meteo PG database
# Reading hidden password from txt file in dir
pgPassword = open(os.path.join('pw.txt'), 'r').readline()
pgPassword = str(pgPassword)

# Defining PostgreSQL connection parameters
conn = psycopg2.connect(dbname='meteo', user='postgres', password=pgPassword,\
host='localhost', port='5432')

def checkPgGeoTable(connectionParameters, table):
    '''
    Função para verificação de boleana de tabela geográfica dentro de BD PostgreSQL.
    Carrrega shapefile se a tabela não existir na BD.
    '''
    cur = connectionParameters.cursor()
    cur.execute("select * from information_schema.tables where table_name=%s", (table,))
    # Checking for existence of districts table
    if bool(cur.rowcount) == False:
    # Creating forecast table in databaase
        command = ["C:\\OSGeo4W64\\bin\\ogr2ogr.exe",
            "-f", "PostgreSQL",
            "PG:host=localhost user=postgres dbname=meteo password=3763", outputPath,
            "-lco", "GEOMETRY_NAME=the_geom", "-lco", "FID=gid", "-lco",
            "PRECISION=no", "-nlt", "PROMOTE_TO_MULTI", "-nln", table, "-overwrite"]
        subprocess.check_call(command)
        print('\n{} table loaded into meteo database.'.format(table))
    else:
        print('\n{} table already exists in meteo database.'.format(table))

checkPgGeoTable(conn, 'districts')


# Meteo request to API
def requestType():
    '''
    Especificação do momento da previsão meteorológica. Exige um input ao user fo tipo str().
    Devolve str() que identifica o momento da previsão meteorológica equerida.
    '''
    # Requesting input
    while True:
        requestType = input('\nSpecify the wanted type of meteomap request (Y for yesterday, N for now, T for tomorrow): ' )
        if not re.match('[YNT]', requestType):
            if requestType == 'exit':
                sys.exit()
            else:
                print('Warning - please, limit your input to Y or N or T or exit with \'exit\'.')
                time.sleep(2)
        else:
            break
    return requestType

def harvestOWM(coordDic, apiKey, requestType):
    '''
    Execução do tipo de pedido através da Open Weather Map One Call API.
    Devolve uma dataframe (pandas) com os princiapis parâmetros meteorológicos para cada distrito.
    '''
    forecast = []
    for item in coord.items():
        lat = str(item[1][1])
        long = str(item[1][0])
        # For yesterday's weather
        if requestType == 'Y':
            yesterday = datetime.now() - timedelta(days=1)
            unixTimestamp = int(yesterday.timestamp())
            url = 'https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={}&lon={}&dt={}&appid={}&units=metric'\
            .format(lat, long, unixTimestamp, apiKey) # http://api.openweathermap.org/data/2.5/onecall/timemachine?lat=60.99&lon=30.9&dt=1616943972&appid=44cfad7f82ec61d3f420de3201b703d4
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
                districtForecast['wind_speed'] = data.get('current').get('wind_speed')
                districtForecast['wind_deg'] = data.get('current').get('wind_deg')
                forecast.append(districtForecast)
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
                districtForecast['wind_speed'] = data.get('wind_speed')
                districtForecast['wind_deg'] = data.get('wind_deg')
                forecast.append(districtForecast)
    forecast_df = pandas.DataFrame(forecast)
    return forecast_df

# Reading hidden Open Weather Map API key from txt file
apiKey = open(os.path.join('apikey.txt'), 'r').readline()
apiKey = str(apiKey)

# Requesting type of meteo ((Y)esterday, (N)ow or (T)omorrow)
request = requestType()
print('\nYour request has been successfully validated.')

# Havesting data from Open Weather Map
meteoDataFrame = harvestOWM(coord, apiKey, request)
print('\nMeteorological data has been successfully harvested.')
print('\n', meteoDataFrame)


# Checking for existence of forecast table
def checkPgTable(connectionParameters, dataFrame, table):
    '''
    Função para verificação de presença/ausência de tabela dentro de BD PostgreSQL.
    Devolve True se existir; False se não existir.
    '''
    cur = connectionParameters.cursor()
    cur.execute("select * from information_schema.tables where table_name=%s", (table,))
    # Checking for existence of districts table
    if bool(cur.rowcount) == False:
        cols = list(dataFrame.columns)        
    return cols

print(checkPgTable(conn, meteoDataFrame, 'forecast'))

def df2PgSQL(connectionParameters, dataFrame, table):
    '''
    Utilização da função psycopg2.extras.execute_values()
    para carregamento da data frame colhida na tabela PostGreSQL "forecast"
    '''
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in dataFrame.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(dataFrame.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = connectionParameters.cursor()
    try:
        psy2extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print('\nMeteorological data has been successfully loaded into DB.')
    cursor.close()

# # Loading meteorological dataframe into DB
# df2PgSQL(conn, meteoDataFrame, 'forecast')


# # Finnaly building the map
# def viewExtraction(connectionParameters):
#     '''
#     '''
#     query = 'drop view if exists last_forecast;\
#     create view last_forecast as\
#     select forecast.*, {}.the_geom\
#     from forecast, centroides\
#     where forecast.distrito = centroides.distrito\
#     order by forecast.forecast_id desc limit 18'
#     cursor = conn


# Execution time (finish)
print("\nmeteo2map executed in %s seconds" % (time.time() - start_time))