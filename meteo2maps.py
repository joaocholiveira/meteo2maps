# Made by João H. Oliveira (2021), as a project for Software Aberto e Programação em SIG

#%%
import os, sys, urllib.request, json, subprocess, geopandas, pandas,\
shutil, psycopg2, re, requests, time, warnings, webbrowser
from datetime import datetime, timedelta
from psycopg2 import extras as psy2extras
from geo.Geoserver import Geoserver

#%%
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

#%%

# Working on districts
if os.path.exists(outputPath+'districts.shp') == False:
    print('\ndistricts.shp not found. Let\'s work on it.')
    # Reading main CAOP shapefile (it needs to already exist)
    caop = geopandas.read_file('caop.shp', encoding='utf-8')
    # Executing dissolve from parishes to districts
    districts = caop.dissolve(by = 'Distrito')
    districtsEtrs = districts.to_crs(3763)
    # Saving dissolve output as shapefile (WGS and) and reading it
    districts.to_file(outputPath+'districts.shp', encoding='utf-8')
    districtsEtrs.to_file(outputPath+'districtsetrs.shp', encoding='utf-8')
    districts = geopandas.read_file(outputPath+'districts.shp', encoding='utf-8')
else:
    print('\nFound districts.shp')
    # Reading districts shapefile that already exists
    districts = geopandas.read_file(outputPath+'districts.shp', encoding='utf-8')
    districtsEtrs = districts.to_crs(3763)
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

#%%

# Check the existence of districts table in meteo PG database
# Reading hidden PostgreSQL password from txt file in dir
pgPassword = open(os.path.join('postgresPw.txt'), 'r').readline()
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
        command = 'shp2pgsql -s 3763 C:\\saprog\\projeto\\output\\districtsetrs.shp public.districtsetrs\
        | psql -q -U postgres -d meteo -h localhost -p 5432'
        os.system(command)
        time.sleep(5)
        print('\n{} table loaded into meteo database.'.format(table))
    else:
        print('\n{} table already exists in meteo database.'.format(table))

checkPgGeoTable(conn, 'districtsetrs')

#%%

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
            .format(lat, long, unixTimestamp, apiKey)
            # example http://api.openweathermap.org/data/2.5/onecall/timemachine?lat=60.99&lon=30.9&dt=1616943972&appid=44cfad7f82ec61d3f420de3201b703d4
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
                districtForecast['request_type'] = 'yesterday'
                forecast.append(districtForecast)
        # For current weather
        elif requestType == 'N':
            url = 'https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude=minutely,hourly,daily,alerts&appid={}&units=metric'\
            .format(lat, long, apiKey)
            # example https://api.openweathermap.org/data/2.5/onecall?lat=33.441792&lon=-94.037689&exclude=minutely,hourly,daily,alerts&appid=44cfad7f82ec61d3f420de3201b703d4
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
                districtForecast['request_type'] = 'now'
                forecast.append(districtForecast)
        # For tomorrow's weather
        elif requestType == 'T':
            url = 'https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude=current,minutly,hourly,alerts&appid={}&units=metric'\
            .format(lat, long, apiKey)
            # example https://api.openweathermap.org/data/2.5/onecall?lat=33.441792&lon=-94.037689&exclude=current,minutly,hourly,alerts&appid=44cfad7f82ec61d3f420de3201b703d4
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
                districtForecast['request_type'] = 'tomorrow'
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
# print('\n', meteoDataFrame)

#%%

# Checking for existence of forecast table
def checkPgTable(connectionParameters, table):
    '''
    Função para verificação de presença/ausência de tabela dentro de BD PostgreSQL.
    Devolve True se existir; False se não existir.
    '''
    cur = connectionParameters.cursor()
    cur.execute("select * from information_schema.tables where table_name=%s", (table,))
    # Checking for existence of districts table
    if bool(cur.rowcount) == False:
        query  = "CREATE TABLE forecast(\
        forecast_id SERIAL, distrito VARCHAR(80), forecast_date DATE, forecast_time TIME, weather_desc VARCHAR(80),\
        temperature FLOAT, feels_like FLOAT, pressure INT, humidity INT, dew_point FLOAT, wind_speed FLOAT, wind_deg INT,\
        request_type VARCHAR(80), CONSTRAINT forecast_pkey PRIMARY KEY (forecast_id))"
        cur.execute(query)
        conn.commit()
        cur.close()
        print('\n{} table created into meteo database.'.format(table))
    else:
        print('\n{} table already exists in meteo database.'.format(table))

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
    query  = "INSERT INTO public.%s(%s) VALUES %%s" % (table, cols)
    cur = connectionParameters.cursor()
    try:
        psy2extras.execute_values(cur, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cur.close()
        return 1
    print('\nMeteorological data has been successfully loaded into DB.')
    cur.close()

# Checking for forecast table
checkPgTable(conn, 'forecast')

# Loading meteorological dataframe into DB
df2PgSQL(conn, meteoDataFrame, 'forecast')

#%%

# Finnaly building the map
def geoViewExtraction(connectionParameters):
    '''
    Construção de view dentro da BD, respeitante ao último request.
    Devolve view que associa uma componente espacial às variáveis meteorológicas por distritito.
    '''
    query = "DROP VIEW IF EXISTS last_forecast;\
    CREATE VIEW last_forecast as\
    select forecast.*, districtsetrs.geom\
    from forecast, districtsetrs\
    where forecast.distrito = districtsetrs.distrito\
    order by forecast.forecast_id desc limit 18"
    cur = connectionParameters.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    print('\nPostgreSQL geoview created.')

geoViewExtraction(conn)

# Geoserver loading
# Reading hidden Geoserver password from txt file in dir
gsPassword = open(os.path.join('geoserverPw.txt'), 'r').readline()
gsPassword = str(gsPassword)

geoserverCred = {'username':'admin', 'password':gsPassword}

def initializeGeoserver():
    '''
    Inicialização do Geoserver através da linha de comandos.
    '''
    print('\nStarting Geoserver. Just wait a moment please.')
    # Geoserver starting in a new command line
    cmd = 'start C:\\"Program Files"\\geoserver\\bin\\startup.bat'
    subprocess.Popen(cmd, shell=True)
    # To give time to Geoserver startup
    time.sleep(20)

initializeGeoserver()

def checkWorkspace(geoserverCredentials, workspaceName):
    '''
    Verificação de ausência/presença de workspace dentro do Geoserver.
    Caso o workspace especificado não exista, a função cria-o.
    '''
    # Initialize the library
    geo = Geoserver('http://localhost:8080/geoserver', username=geoserverCredentials.get('username'),\
    password=geoserverCredentials.get('password'))
    # Checking for workspace
    if geo.get_workspace(workspace=workspaceName) == None:
        print('\n',workspaceName, 'workspace not found. Let\'s create it.')
        geo.create_workspace(workspace=workspaceName)
    else:
        print('\nFound', workspaceName, 'workspace.')

checkWorkspace(geoserverCred, 'saprog_meteo')

postgresCred = {'dbname':'meteo', 'user':'postgres', 'password':pgPassword,\
'host':'localhost', 'port':'5432'}

def createFeatureStore(geoserverCredentials, postgresCredentials, workspaceName, storeName):
    '''
    Verificação de ausência/presença de featurestore dentro do Geoserver.
    Caso a featurestore especificado não exista, a função cria-a.
    '''
    geo = Geoserver('http://localhost:8080/geoserver', username=geoserverCredentials.get('username'),\
    password=geoserverCredentials.get('password'))
    if geo.get_featurestore(workspace=workspaceName, store_name=storeName)\
    == 'Error: Expecting value: line 1 column 1 (char 0)':
        print('\n',storeName, 'featurestore not found. Let\'s create it.')
        geo.create_featurestore( workspace=workspaceName, store_name=storeName, db=postgresCredentials.get('dbname'),\
        host=postgresCredentials.get('host'), pg_user=postgresCredentials.get('user'), pg_password=postgresCredentials.get('password'))
    else:
        print('\nFound', storeName, 'featurestore.')

createFeatureStore(geoserverCred, postgresCred, 'saprog_meteo', 'meteomap')

def publishFeatureStore(geoserverCredentials, workspaceName, storeName, tableName):
    '''
    Função para carregamento da view em base de dados para o featurestore.
    Necessita de exista a referida view dentro da base de dados Postgres,
    e do workspace e featurestore definida dentro do Geoserver.
    '''
    geo = Geoserver('http://localhost:8080/geoserver', username=geoserverCredentials.get('username'),\
    password=geoserverCredentials.get('password'))
    if geo.get_layer(workspace=workspaceName, layer_name=tableName)\
    == "get_layer error: Expecting value: line 1 column 1 (char 0)".format(tableName, storeName):
        print('\nForecast layer successfully uploaded.')
        geo.publish_featurestore(workspace=workspaceName, store_name=storeName, pg_table=tableName)
    else:
        print("\nThere was an old forecast stored. Let's create an updated layer")
        geo.delete_layer(layer_name=tableName, workspace=workspaceName)
        geo.publish_featurestore(workspace=workspaceName, store_name=storeName, pg_table=tableName)
        

publishFeatureStore(geoserverCred, 'saprog_meteo', 'meteomap', 'last_forecast')

# geo = Geoserver('http://localhost:8080/geoserver', username=geoserverCred.get('username'),\
# password=geoserverCred.get('password'))
# print(geo.get_layer(workspace='saprog_meteo', layer_name='last_forecast'))
        


# doc in use : https://geoserver-rest.readthedocs.io/en/latest/how_to_use.html?highlight=geo.get_featurestore#creating-and-publishing-featurestores-and-featurestore-layers


# %%
# # Execution time (finish)
print("\nmeteo2map executed in %s seconds" % (time.time() - start_time))