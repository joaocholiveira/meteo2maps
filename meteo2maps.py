# METEO2MAP
# Made by João H. Oliveira (2021), as final project for Software Aberto e Programação em SIG

# Licenced under GPLv3
# camelCase style adopted
# #%% Stands for VSC interactive execution directly on .py, like a .ipynb

#%%

import os, sys, urllib.request, json, geopandas, pandas,\
psycopg2, re, time, warnings
from datetime import datetime, timedelta
from psycopg2 import extras as psy2extras
from geo.Geoserver import Geoserver

# Execution time (start)
start_time = time.time()

# To disable geopandas CRS warnings in terminal
warnings.filterwarnings('ignore')

os.environ['SHAPE_ENCODING'] = "utf-8"
basePath = 'C:\\saprog\\projeto'
outputPath = 'C:\\saprog\\projeto\\output\\'
os.chdir(basePath)

# Counter support variable for getMessageString
global counter
counter = {'counter': 0}

def getMessageString(msg):
    '''
    Função desenhada para substituir o simples "print", adicionando-lhe
    uma contagem incrementável ao longo da execução do programa.
    Deverá ser empregue sempre que se pretender estabelecer contacto com o
    utilizador pela linha de comandos.
    '''
    counter['counter'] += 1
    print('\n', str(counter['counter']) + '. ' + msg)
    
getMessageString('Working on initial steps... Please, wait a moment.')

def outputDir():
    if os.path.exists(outputPath) == False:
        os.mkdir(outputPath)
        getMessageString('Output dir. created.')
    else:
        getMessageString('Found output dir.')

outputDir()

#%%

# Working on districts
if os.path.exists(outputPath+'districts.shp') == False:
    getMessageString('districts.shp not found. Let\'s work on it.')
    # Reading main CAOP shapefile (it needs to already exist)
    caop = geopandas.read_file('caop.shp', encoding='utf-8')
    # Executing dissolve from parishes to districts
    districts = caop.dissolve(by = 'Distrito')
    districtsEtrs = districts.to_crs(3763)
    # Saving dissolve output as shapefile (WGS) and reading it
    districts.to_file(outputPath+'districts.shp', encoding='utf-8')
    districtsEtrs.to_file(outputPath+'districtsetrs.shp', encoding='utf-8')
    districts = geopandas.read_file(outputPath+'districts.shp', encoding='utf-8')
else:
    getMessageString('Found districts.shp')
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

coordDist = getCoordTogether(districts)
getMessageString('Districts centroid coordinates compiled.')

 #%%

# Check the existence of districts table in meteo PG database
# Reading hidden PostgreSQL password from txt file in dir
pgPassword = open(os.path.join('postgresPw.txt'), 'r').readline()
pgPassword = str(pgPassword)

# Defining PostgreSQL connection parameters
conn = psycopg2.connect(dbname='meteo', user='postgres', password=pgPassword,\
host='localhost', port='5432')

def checkPgDistrictsTable(connectionParameters, table):
    '''
    Função para verificação de boleana de tabela geográfica dentro de BD PostgreSQL.
    Carrrega shapefile se a tabela não existir na BD.
    '''
    cur = connectionParameters.cursor()
    cur.execute("select * from information_schema.tables where table_name=%s", (table,))
    # Checking for existence of districts table
    if bool(cur.rowcount) == False:
    # Uploading districts sgapefile in database
        command = 'shp2pgsql -s 3763 C:\\saprog\\projeto\\output\\districtsetrs.shp public.districtsetrs\
        | psql -q -U postgres -d meteo -h localhost -p 5432'
        os.system(command)
        time.sleep(5)
        getMessageString('{} table uploaded in meteo database.'.format(table))
    else:
        getMessageString('{} table already exists in meteo database.'.format(table))

# Checking for existence of forecast table
def checkPgForecastTable(connectionParameters, table):
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
        getMessageString('{} table created into meteo database.'.format(table))
    else:
        getMessageString('{} table already exists in meteo database.'.format(table))

# Checking for districts table
checkPgDistrictsTable(conn, 'districtsetrs')

# Checking for forecast table
checkPgForecastTable(conn, 'forecast')

#%%

# Meteo request to API
def requestType():
    '''
    Especificação do momento da previsão meteorológica. Exige um input ao user fo tipo str().
    Devolve str() que identifica o momento da previsão meteorológica requerida.
    '''
    # Requesting input
    while True:
        requestType = input(getMessageString('Specify the wanted type of meteomap request (Y for yesterday, N for now, T for tomorrow): ' ))
        if not re.match('[YNT]', requestType):
            if requestType == 'exit':
                sys.exit()
            else:
                getMessageString('Warning - please, limit your input to Y or N or T or exit with \'exit\'.')
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
    for item in coordDic.items():
        lat = str(item[1][1])
        long = str(item[1][0])
        # For yesterday's weather
        if requestType == 'Y':
            yesterday = datetime.now() - timedelta(days=1)
            unixTimestamp = int(yesterday.timestamp())
            url = 'https://api.openweathermap.org/data/2.5/onecall/timemachine?'+\
			'lat={}&lon={}&dt={}&appid={}&units=metric'.format(lat, long, unixTimestamp, apiKey)
            # example http://api.openweathermap.org/data/2.5/onecall/timemachine?
			# \lat=60.99&lon=30.9&dt=1616943972&appid=44cfad7f82ec61d3f522de3201b703d4
        # For current weather
        elif requestType == 'N':
            url = 'https://api.openweathermap.org/data/2.5/onecall?'+\
            'lat={}&lon={}&exclude=minutely,hourly,daily,alerts&appid={}&units=metric'.format(lat, long, apiKey)
            # example https://api.openweathermap.org/data/2.5/onecall?\
			# lat=33.441792&lon=-94.037689&exclude=minutely,hourly,daily,alerts&appid=44cfad7f82ec61d3f522de3201b703d4
        # For tomorrow's weather
        elif requestType == 'T':
            url = 'https://api.openweathermap.org/data/2.5/onecall?'+\
            'lat={}&lon={}&exclude=current,minutly,hourly,alerts&appid={}&units=metric'.format(lat, long, apiKey)
            # example https://api.openweathermap.org/data/2.5/onecall?\
			# lat=33.441792&lon=-94.037689&exclude=current,minutly,hourly,alerts&appid=44cfad7f82ec61d3f522de3201b703d4
        with urllib.request.urlopen(url) as url:
            data = json.loads(url.read().decode())
            if requestType == 'T':
                data = data['daily'][1]
            else:
                pass
            districtForecast = {}
            districtForecast['distrito'] = item[0]
            if requestType == 'Y' or requestType == 'N':
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
                if requestType == 'Y':
                    districtForecast['request_type'] = 'yesterday'
                elif requestType == 'N':
                    districtForecast['request_type'] = 'now'
            else:
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
        getMessageString("Error: %s" % error)
        conn.rollback()
        cur.close()
        return 1
    getMessageString('Meteorological data has been successfully loaded into DB.')
    cur.close()

# Reading hidden Open Weather Map API key from txt file
apiKey = open(os.path.join('apikey.txt'), 'r').readline()
apiKey = str(apiKey)

# Requesting type of meteo ((Y)esterday, (N)ow or (T)omorrow)
request = requestType()
getMessageString('Your request has been successfully validated.')

# Havesting data from Open Weather Map
meteoDataFrame = harvestOWM(coordDist, apiKey, request)
getMessageString('Meteorological data has been successfully harvested.')

# Loading meteorological dataframe into DB
df2PgSQL(conn, meteoDataFrame, 'forecast')

# Finnaly building the map service
def geoViewExtraction(connectionParameters):
    '''
    Construção de view dentro da BD, respeitante ao último request.
    Devolve view que associa uma componente espacial às variáveis meteorológicas por distritito.
    '''
    query = "DROP VIEW IF EXISTS forecast_map;\
    CREATE VIEW forecast_map as\
    select forecast.*, districtsetrs.geom\
    from forecast, districtsetrs\
    where forecast.distrito = districtsetrs.distrito\
    order by forecast.forecast_id desc limit 18"
    cur = connectionParameters.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    getMessageString('PostgreSQL forecast_map geoview created.')

geoViewExtraction(conn)

#%%

# Reading hidden Geoserver password from txt file in dir
gsPassword = open(os.path.join('geoserverPw.txt'), 'r').readline()
gsPassword = str(gsPassword)

geoserverCred = {'username':'admin', 'password':gsPassword}

def initializeGeoserver():
    '''
    Inicialização do Geoserver através da linha de comandos.
    '''
    getMessageString('Starting Geoserver. Please, wait a moment.')
    # Geoserver starting in a new command line
    # cmd = 'start C:\\"Program Files"\\geoserver\\bin\\startup.bat'
    # subprocess.Popen(cmd, shell=True)
    os.system('start C:\\"Program Files"\\geoserver\\bin\\startup.bat')
    # To give time to Geoserver startup correctly
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
        getMessageString('{} workspace not found. Let\'s create it.'.format(workspaceName))
        geo.create_workspace(workspace=workspaceName)
    else:
        getMessageString('Found {} workspace.'.format(workspaceName))

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
        getMessageString('{} featurestore not found. Let\'s create it.'.format(storeName))
        geo.create_featurestore( workspace=workspaceName, store_name=storeName, db=postgresCredentials.get('dbname'),\
        host=postgresCredentials.get('host'), pg_user=postgresCredentials.get('user'), pg_password=postgresCredentials.get('password'))
    else:
        getMessageString('Found {} featurestore.'.format(storeName))

createFeatureStore(geoserverCred, postgresCred, 'saprog_meteo', 'meteomap')

def publishFeatureStore(geoserverCredentials, workspaceName, storeName, pgTableName):
    '''
    Função para carregamento da view em base de dados para o featurestore.
    Necessita que exista a referida view dentro da base de dados Postgres,
    e do workspace e featurestore definida dentro do Geoserver.
    '''
    geo = Geoserver('http://localhost:8080/geoserver', username=geoserverCredentials.get('username'),\
    password=geoserverCredentials.get('password'))
    if geo.get_layer(workspace=workspaceName, layer_name=pgTableName)\
    == "get_layer error: Expecting value: line 1 column 1 (char 0)".format(pgTableName, storeName):
        getMessageString('Forecast WMS successfully uploaded.')
        geo.publish_featurestore(workspace=workspaceName, store_name=storeName, pg_table=pgTableName)
    else:
        getMessageString("There was an old forecast WMS stored. Let's overwrite it.")
        geo.delete_layer(layer_name=pgTableName, workspace=workspaceName)
        geo.publish_featurestore(workspace=workspaceName, store_name=storeName, pg_table=pgTableName)
        
publishFeatureStore(geoserverCred, workspaceName='saprog_meteo', storeName='meteomap', pgTableName='forecast_map')


# %%
# Execution time (finish)
exTime = time.time() - start_time
getMessageString("meteo2map executed in {} seconds".format(exTime))

# End of METEO2MAP script