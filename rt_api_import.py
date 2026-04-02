# %% [markdown]
# ## Abfrage Schnittstelle und Ablage in DuckDBzum Hafas Echtzeit-Archiv Produktiv / Demo-System / Ablage in Parquet
# 
# Stand: 19.03.2026
# 
# #### Aufgaben
# - Schema XML V14 prod https://fahrplaner.vbn.de/archive/services/archiveExportService/v14?wsdl 
# - Schema XML V15 demo https://vbn.demo.hafas.de/archive/services/archiveExportService/v15?wsdl
# - Schema XML V15 prod https://fahrplaner.vbn.de/archive/services/archiveExportService/v15?wsdl ab 12.11.2024
# - Dokumentation unter docs/
# - Einbauen Fahrt Start ende scheduleDepartureStation scheduleDepartureTime bzw. Arrival

# %% [markdown]
# #### Import Module

# %%
import requests
import xml.etree.ElementTree as ET
import xml.dom.minidom
import datetime as dt
import time

import numpy as np
import pandas as pd
import geopandas as gpd

import tarfile

from datetime import timedelta

import os

from sqlalchemy import create_engine #als Alternative zu Mysql pyscopg2 Connector
from sqlalchemy import text

from importlib import reload

from dotenv import load_dotenv, dotenv_values
import logging
os.chdir("/home/zvbn/python/rt2")

log_file = f"log/log_rt.txt"
logging.basicConfig(filename=log_file, 
                        level=logging.INFO,
                        style="{",
                        format="{asctime} [{levelname:8}] {message}",
                        datefmt="%d.%m.%Y %H:%M:%S")

load_dotenv()

# %%
logging.info("Import xml gestartet")

# %%
import sys
sys.path.append('/home/zvbn/python/rt2')
import rt_func #Import der benutzerdefinierten Funktionen
reload(rt_func)

# %%
config = dotenv_values(".env")
#config['CLIENT_ID_DEMO']

# %%
pd.options.display.max_columns = 500

# %% [markdown]
# ## Ermitteln verschiedener Zeitpunkte 

# %%
os.chdir("/home/zvbn/python/rt2")

# %%
jetzt = dt.datetime.now().strftime('%Y%m%d%H%M')
heute = dt.date.today().strftime('%Y%m%d')
heute_ll = dt.datetime.now().strftime('%d.%m.%Y %H:%M')
gestern = (dt.date.today() - timedelta(1)).strftime('%Y-%m-%d')

# %%
start = sys.argv[1]
#start = gestern
ende = sys.argv[1]
#ende = gestern

# %% [markdown]
# # Funktionen

# %% [markdown]
# ## Aufrufen der SOAP-Abfrage

# %%
def request_xml(api_version, xml_request, xml_out, myUrl):
    #Zugriff auf Hafas RT Archiv Produktiv System und Zugriffsschlüssel 

    req_ini = requests.post(myUrl, data=xml_request)
    root = ET.fromstring(req_ini.text)
    print(req_ini.text)
    
    #Ermitteln der Export ID
    for child in root.iter('exportId'):
        print(child.tag, child.attrib, child.text)
        exportId = child.text
    xml_status = f"""
                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                    xmlns:v{api_version}="http://v{api_version}.export.service.data.archive.itcs.hafas.hacon.de/">
               <soapenv:Header/>
                    <soapenv:Body>
                        <v{api_version}:getArchiveExportStatus>
                            <exportId>{exportId}</exportId>
                        </v{api_version}:getArchiveExportStatus>
                    </soapenv:Body>
              </soapenv:Envelope>
              """
    #Abfragen und Warten auf Completed
    status = ''
    time.sleep(2) # initiales Warten auf Beendigung
    while status != 'COMPLETED':
        r = requests.post(myUrl, data=xml_status)
        #print(r, '\n',r.text)
        root = ET.fromstring(r.text)
        for child in root.iter('status'):
            #print(child.tag, child.attrib, child.text)
            status = child.text
            print(f'{dt.datetime.now()} Status: {status}')
            if status != 'COMPLETED': # Pause falls Job nicht beendet (Status nicht completed d.h. in process)
                time.sleep(10) # Pause von 20 Sekunden bis zur nächsten Abfrage des Status
    
    # Afrage nach Beendigung Journey List

    xml_jl = ('<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
               'xmlns:v'+str(api_version)+'="http://v'+str(api_version)+'.export.service.data.archive.itcs.hafas.hacon.de/">'
                 '<soapenv:Header/><soapenv:Body>'
                    '<v'+str(api_version)+':getArchiveJourneyList>'
                       '<exportId>' + exportId + '</exportId>'              
                     '</v'+str(api_version)+':getArchiveJourneyList>'
                 '</soapenv:Body>'
          '</soapenv:Envelope>')
    
    rj = requests.post(myUrl, data=xml_jl)

    #Ausgabe des Ergebnis XML Journey
    dom = xml.dom.minidom.parseString(rj.text)
    pretty_xml_as_string = dom.toprettyxml()
    
    jl = open(os.path.join(xml_out), 'w')
    print(pretty_xml_as_string, file = jl)
    print(os.path.join(xml_out), 'gespeichert')

    jl.close()

# %% [markdown]
# ## Import xml Fahrten > Dataframe

# %%
def import_rt_xml_to_df_fahrten(xml_file):
    format_date = '%Y-%m-%dT%H:%M:%S'
    lop = []
    
    # create element tree object 
    tree = ET.parse(xml_file)
    
    # get root element 
    root = tree.getroot() 

    for child in root.iter('archiveExportJourneyAndDetailsDto'):
        for journey in child.iter('journey'):

            #Ermitteln der Feldinhalte
            deviceid = rt_func.isnone(journey.find('deviceId'))
            operday = dt.datetime.strptime(rt_func.isnone(journey.find('operatingDay'))[:-6], format_date).strftime('%Y-%m-%d')
            fnr = rt_func.isnone(journey.find('journeyID'))

            deviceId = rt_func.isnone(journey.find('deviceId'))
            clientId = rt_func.split_deviceid(journey.find('deviceId'))            

            journeyOperator = rt_func.isnone(journey.find('journeyOperator'))
            ex_lineid = rt_func.isnone(journey.find('externalLineId'))
            ex_linid_short = ':'.join(ex_lineid.split(':')[0:3])
            lineshortname = rt_func.isnone(journey.find('lineShortName'))
            destination = rt_func.isnone(journey.find('destination'))

            hasRealtime = rt_func.isnone_boolean(journey.find('hasRealtime'))
            realtimeHasEverBeenReported = rt_func.isnone_boolean(journey.find('realtimeHasEverBeenReported'))
            journeyRtType = rt_func.isnone(journey.find('journeyRtType'))            

            journeycancelled = rt_func.isnone(journey.find('journeyCancelled')).capitalize()
            ts_reported_cancelled = rt_func.isnone(journey.find('lastTimestampJourneyCancellationReported'))
            reported_cancelled = True if len(ts_reported_cancelled) > 0 else False
            cancelled_kum = True if str(reported_cancelled) == 'True' else True if str(journeycancelled) == 'True' else False

            #Ermitteln FahrtStartEnde
            for sub in journey.iter('scheduleDepartureTime'):
                fahrtstarttime = rt_func.isnone_delay(sub.find('scheduleTime'))
            for sub in journey.iter('scheduleArrivalTime'):
                fahrtendtime = rt_func.isnone_delay(sub.find('scheduleTime'))
            for sub in journey.iter('scheduleDepartureStation'):
                fahrtstartstationname = rt_func.isnone_delay(sub.find('stationName'))
                fahrtstartstationdhid = rt_func.isnone_delay(sub.find('dhid'))
            for sub in journey.iter('scheduleArrivalStation'):
                fahrtendstationname = rt_func.isnone_delay(sub.find('stationName'))
                fahrtendstationdhid = rt_func.isnone_delay(sub.find('dhid'))

            
            lop.append([operday, fnr, destination, hasRealtime, realtimeHasEverBeenReported, journeyOperator, ex_lineid, ex_linid_short, lineshortname, \
                        reported_cancelled, journeycancelled, ts_reported_cancelled, cancelled_kum, deviceId, clientId, journeyRtType, \
                            fahrtstarttime, fahrtstartstationname, fahrtstartstationdhid, fahrtendtime, fahrtendstationname, fahrtendstationdhid])
            
            child.clear()

    df_fahrten = pd.DataFrame(lop, columns=['datum','fnr' ,'destination','hasRealtime','realtimeHasEverBeenReported' ,'vu', 'lineid', 'lineid_short', 'lineshort', \
                                            'reported_cancelled', 'journey_cancelled','ts_reported_cancelled' ,'cancelled_kum', 'deviceid', \
                                                'clientid', 'journeyrttype', 'fahrtstarttime', 'fahrtstartstationname', 'fahrtstartstationdhid',\
                                                      'fahrtendtime', 'fahrtendstationname', 'fahrtendstationdhid'])
    return df_fahrten

# %% [markdown]
# ## Import xml Verlauf > Dataframe

# %%
def import_rt_xml_to_df_verlauf(xml_file):
    format_dt = '%Y-%m-%dT%H:%M:%S'
    lop = []

    # create element tree object 
    tree = ET.parse(xml_file)
    
    # get root element 
    root = tree.getroot() 
    for child in root.iter('archiveExportJourneyAndDetailsDto'):
        for journey in child.iter('journey'):
            has_rt = rt_func.isnone(journey.find('hasRealtime'))
            
            deviceid = rt_func.isnone(journey.find('deviceId'))
            fnr = rt_func.isnone(journey.find('journeyID'))
            lineshortname = str(rt_func.isnone(journey.find('lineShortName'))).strip()
            ex_lineid = rt_func.isnone(journey.find('externalLineId'))
            journeyOperator = rt_func.isnone(journey.find('journeyOperator'))
            operday = dt.datetime.strptime(rt_func.isnone(journey.find('operatingDay'))[:-6], format_dt).strftime('%Y-%m-%d')
            ts_reported_cancelled = rt_func.isnone(journey.find('lastTimestampJourneyCancellationReported'))
            reported_cancelled = True if len(ts_reported_cancelled) > 0 else False

        for details in child.iter('details'):
            index = rt_func.isnone(details.find('index'))
            for ddelay in details.iter('departureDelay'):
                dep_del = rt_func.isnone_delay(ddelay.find('delay'))

            for adelay in details.iter('arrivalDelay'):
                arr_del = rt_func.isnone_delay(adelay.find('delay'))
            
            canc = rt_func.isnone(details.find('cancelled'))
            
            additional =  rt_func.isnone(details.find('additional'))

            for station in details.iter('station'):
                lat = int(station.find('latitude').text)/1000000
                lon = int(station.find('longitude').text)/1000000
                station_nr = station.find('stationExternalNumber').text
                if station.find('stationName') is not None:
                    station_name = station.find('stationName').text
                else:
                    station_name = '-'
            
            for dschedule in details.iter('scheduleDepartureTime'):
                dschedtime= dschedule.find('scheduleTime')
                if dschedtime is not None:
                    dschedtime = dt.datetime.strptime(dschedtime.text[:-6], format_dt).strftime('%Y%m%d%H%M%S') #Umwandlung der Zeitformat da in 3.6 kein ISO-Format vorhanden
                else:
                    dschedtime =''
            for aschedule in details.iter('scheduleArrivalTime'):
                aschedtime = aschedule.find('scheduleTime')
                if aschedtime is not None:
                    aschedtime = dt.datetime.strptime(aschedtime.text[:-6], format_dt).strftime('%Y%m%d%H%M%S')
                else: 
                    aschedtime =''

            lop.append([operday, journeyOperator, deviceid, lineshortname, ex_lineid, 
                                    fnr, index, has_rt, dschedtime, aschedtime, dep_del, arr_del, station_nr, station_name, lat, lon, canc, additional, 
                                    ts_reported_cancelled, reported_cancelled])
    
    df_verlauf = pd.DataFrame(lop, columns=['operday','journeyOperator' ,'deviceid','lineshortname' ,'ex_lineid', 'fnr', 'index', 'has_rt', 
                                            'dschedtime', 'aschedtime','dep_del' ,'arr_del', 'station_nr', 'station_name', 'lat', 'lon', 'canc', 'additional', 
                                            'ts_reported_cancelled', 'reported_cancelled'])
    return df_verlauf

# %% [markdown]
# ## Ausgabe als formatiertes xml

# %%
#Testen des XML mit schöner Ausgabe
def print_pretty_xml(xml_request):
    dom = xml.dom.minidom.parseString(xml_request)
    pretty_xml_as_string = dom.toprettyxml()
    print(pretty_xml_as_string)

# %% [markdown]
# ## Xml to tar.gz
# - Packen und Löschen des Ausgangs xml Files

# %%
def xml_to_targz(xml_path,xml_file):
    """Packen des xml-files"""
    tar_gz = xml_file + '.tar.gz'

    if os.path.exists(os.path.join(xml_path, tar_gz)):
        with tarfile.open(os.path.join(xml_path, tar_gz), 'r:gz') as tar:
            # Extract all files to the specified directory    
            tar.extractall(xml_path)
    else:
        print('no tar.gz')

    with tarfile.open(os.path.join(xml_path, tar_gz), 'w:gz') as archive:
        # Add files to the tarball
        archive.add(os.path.join(xml_path, xml_file), arcname= xml_file)
                    
    os.remove(os.path.join(xml_path, xml_file))

# %% [markdown]
# ## Umwandlung der Datentypen Fahrten

# %%
def type_df_fahrten(df_rt_vbn_fahrten):
    """Umwandlung in verwendbare Boolean Typen"""
    df_rt_vbn_fahrten['datum'] = pd.to_datetime(df_rt_vbn_fahrten['datum'], format='%Y-%m-%d')
    #Umwandlung be gemischten Zeitzonen manuell mit strptime
    #df_rt_vbn_fahrten['fahrtstarttime'] = pd.to_datetime(df_rt_vbn_fahrten['fahrtstarttime'], utc=True)
    df_rt_vbn_fahrten['journey_cancelled'] = df_rt_vbn_fahrten['journey_cancelled'].replace({'True':True,'False':False},regex=True)
    return df_rt_vbn_fahrten

# %% [markdown]
# ## Umwandlung der Datentypen Verlauf

# %%
def type_df_verlauf(df_rt_vbn_verlauf):
    """ Anpassung der verschiedenen Datentypen in der Datei Verlauf"""
    df_rt_vbn_verlauf['lat'] = df_rt_vbn_verlauf['lat'].astype(float)
    df_rt_vbn_verlauf['lon'] = df_rt_vbn_verlauf['lon'].astype(float)
    df_rt_vbn_verlauf['dep_del'] = df_rt_vbn_verlauf['dep_del'].astype(float)
    df_rt_vbn_verlauf['arr_del'] = df_rt_vbn_verlauf['arr_del'].astype(float)
    df_rt_vbn_verlauf['canc'] = df_rt_vbn_verlauf['canc'].replace({'true':True,'false':False},regex=True) #wird in künftigen Versionen nicht unterstützt downcast
    df_rt_vbn_verlauf['has_rt'] = df_rt_vbn_verlauf['has_rt'].replace({'true':True,'false':False},regex=True) #wird in künftigen Versionen nicht unterstützt downcast
    df_rt_vbn_verlauf['additional'] = df_rt_vbn_verlauf['additional'].replace({'true':True,'false':False},regex=True) #wird in künftigen Versionen nicht unterstützt downcast
    df_rt_vbn_verlauf['reported_cancelled'] = df_rt_vbn_verlauf['reported_cancelled'].replace({'True':True,'False':False},regex=True)
    df_rt_vbn_verlauf['index'] = df_rt_vbn_verlauf['index'].astype('Int32')
    df_rt_vbn_verlauf['operday'] = pd.to_datetime(df_rt_vbn_verlauf['operday'], format='%Y-%m-%d')
    df_rt_vbn_verlauf['dschedtime'] = pd.to_datetime(df_rt_vbn_verlauf['dschedtime'], format='%Y%m%d%H%M%S')
    df_rt_vbn_verlauf['aschedtime'] = pd.to_datetime(df_rt_vbn_verlauf['aschedtime'], format='%Y%m%d%H%M%S')
    return df_rt_vbn_verlauf

# %% [markdown]
# # Einlesen der Linienliste / Zuordnung Bündel

# %% [markdown]
# Einlesen aus der lokalen DM Datenbank Wortmann Server

# %%
try:
    engine = create_engine(f"postgresql+psycopg2://{config['POSTGRES_USER']}:{config['POSTGRES_PW']}@127.0.0.1:5432/zvbn_postgis")
    #conn_dm = psycopg2.connect(database='zvbn_postgis', user='postgres', password=para.key_dm_db, host = '127.0.0.1')
    sql_lin = """SELECT nummer AS linie, buendel, \'\' AS rt_operator, ebene, dlid, id 
        FROM basis.linien 
        WHERE buendel IS NOT NULL AND aktiv IS TRUE 
        ORDER BY buendel, ebene, nummer """
    sql_buendel = 'SELECT * FROM basis.lin_buendel'
    df_lin_dm =  pd.read_sql(text(sql_lin), engine.connect())
    df_buendel = pd.read_sql(text(sql_buendel), engine.connect())
    df_lin_dm.to_csv('input/linien_dm.csv', sep=';', index=False)
    df_lin_dm.to_excel('input/linien_dm.xlsx', index=False)
    print('Verbindung erfolgreich -lokale Datei aktualisiert')
except:
    df_lin_dm = pd.read_csv('input/linien_dm.csv', sep=';') #aktuelle Zuordnung Linie zu Bündel aus DM
    print(f'Verbindung nicht erfolgreich - Verwendung lokale Datei')

# %% [markdown]
# # Abruf XML und Erstellen Dataframe

# %% [markdown]
# ## Gesamt VBN
# 
# - Abfagen aller Daten für einen Tag über die Externallinid (de:VBN:* und Metronomlinien mit de:hvv:) de:VBN:*,de:hvv:RB33:,de:hvv:RB41:,de:hvv:RE4: und 910 aus Cloppenburg
# - lineExternalNamePattern Abfrage über DLID

# %% [markdown]
# ### Erstellen der Abfrage für xml-Soap mit Funktion

# %%
def def_xml_request_dlid(start, ende, api_version, clientID, matrix, lineExternalNamePattern):
     """ Erstellen der SOAP Abfrage mit verschiedenen Parametern"""
     if api_version >= 15:
        options = f"""
               <options>
                    <includeMatrixData>{str(matrix).lower()}</includeMatrixData>
               </options>
               """
     else:
        options = ""
     
     xml_request_dlid = f"""
     <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v{api_version}="http://v{api_version}.export.service.data.archive.itcs.hafas.hacon.de/">
                    <soapenv:Header/>
                    <soapenv:Body>
                    <v{api_version}:createArchiveJob>
                         <filter>
                              <clientId>{clientID}</clientId>                    
                              <startDate>{start}</startDate>
                              <endDate>{ende}</endDate>
                              <lineExternalNamePattern>{lineExternalNamePattern}</lineExternalNamePattern>            
                              <hasRealtime>ALL</hasRealtime>
                         </filter>
                         {options}
                    </v{api_version}:createArchiveJob>
                    
               </soapenv:Body>
          </soapenv:Envelope>
                    """
     return xml_request_dlid

# %%
print(def_xml_request_dlid(start=start, ende=ende, api_version=14, clientID=config['CLIENT_ID_PROD'], matrix=False, lineExternalNamePattern='xx'))

# %% [markdown]
# ### Erstellen der Abfrage für xml-Soap mit Funktion Zusatzfahrten

# %%
def def_xml_request_zusatz(start, ende, api_version, clientID):
        """Erstellen der SOAP-Anfrage für den Teil Zusatzfahrten"""
        xml_request_zusatz_umleitung = f"""
                                    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                xmlns:v{api_version}="http://v{api_version}.export.service.data.archive.itcs.hafas.hacon.de/">
                <soapenv:Header/><soapenv:Body><v{api_version}:createArchiveJob>
                <filter>
                        <clientId>{clientID}</clientId>         
                        <startDate>{start}</startDate>
                        <endDate>{ende}</endDate>
                        <filterJourneyRtTypeList>REALTIME_EXTRA</filterJourneyRtTypeList>
                        <filterJourneyRtTypeList>REALTIME_EXTRA_REPLACEMENT</filterJourneyRtTypeList>
                        <filterJourneyRtTypeList>REALTIME_EXTRA_REPORTED</filterJourneyRtTypeList>
                        <filterJourneyRtTypeList>REALTIME_EXTRA_MAINTENANCE</filterJourneyRtTypeList>
                        <filterJourneyRtTypeList>DEVIATION_OF_SCHEDULED</filterJourneyRtTypeList>
                        <filterJourneyRtTypeList>DEVIATION_OF_REALTIME_EXTRA</filterJourneyRtTypeList>         
                        <filterJourneyRtTypeList>DEVIATION_OF_REPLACEMENT</filterJourneyRtTypeList>             
                        <filterJourneyRtTypeList>SUPPLEMENTARY</filterJourneyRtTypeList>'                
                        <filterJourneyRtTypeList>UNKNOWN</filterJourneyRtTypeList>               
                        <hasRealtime>ALL</hasRealtime>
                </filter>
                </v{api_version}:createArchiveJob></soapenv:Body></soapenv:Envelope>
                """
        return xml_request_zusatz_umleitung

# %% [markdown]
# ## SOAP Abfrage ausführen Verlauf / Fahrten

# %% [markdown]
# ### Produktivsystem

# %%
#gestern = '2024-10-31' #Eingabe eines bestimmten Datums um Automatismus auszusetzen
start = start
ende = start
api_version = 15
clientID = config['CLIENT_ID_PROD']
server = 'prod' #prod oder demo
#lineExternalNamePattern = 'de:VBN:62:*' #Auswahl einer Linie nicht für produktiv
lineExternalNamePattern = 'de:VBN:*,de:hvv:RB33:,de:hvv:RB41:,de:hvv:RE4:,de:VBN-VGC:910:' #Gesamt VBN

#Festlegen Prod oder Demosystem
if server == 'prod':
    clientID = config['CLIENT_ID_PROD'] #prod
    api_version = 15
    matrix = False #ab Version 15 true möglich
    myUrl = f"https://fahrplaner.vbn.de/archive/services/archiveExportService/v{api_version}?wsdl"
else:
    clientID = config['CLIENT_ID_DEMO'] #demo
    api_version = 15
    matrix = True #ab Version 15 und nur für Testwecke
    myUrl = f"https://vbn.demo.hafas.de/archive/services/archiveExportService/v{api_version}?wsdl"

xml_request_dlid = def_xml_request_dlid(start=start, ende=ende, api_version=api_version, 
                                        clientID=clientID, matrix=matrix, lineExternalNamePattern=lineExternalNamePattern)
print(xml_request_dlid)

xml_path_pre = 'api_xml'

xml_file = f"rt_archiv_{api_version}_{start}_{ende}_alle_{server}_matrix_{matrix}.xml"
xml_path = os.path.join(xml_path_pre, server)
xml_out = os.path.join(xml_path_pre, server, xml_file)
tar_gz = f"{xml_out}.tar.gz"

if os.path.exists(os.path.join(xml_path, tar_gz)):
    with tarfile.open(os.path.join(xml_path, tar_gz), 'r:gz') as tar:
        # Extract all files to the specified directory    
        tar.extractall(xml_path) 
else:
    print('no tar.gz')   

request_xml(api_version=api_version, xml_request=xml_request_dlid, xml_out=xml_out, myUrl=myUrl)


# %%
print(f"XML-Datei {xml_out} erstellt")
df_rt_vbn_fahrten = import_rt_xml_to_df_fahrten(xml_out)
df_rt_vbn_verlauf = import_rt_xml_to_df_verlauf(xml_out)

print("Erstellen DF")
df_rt_vbn_verlauf = type_df_verlauf(df_rt_vbn_verlauf)
df_rt_vbn_fahrten = type_df_fahrten(df_rt_vbn_fahrten)

xml_to_targz(xml_file=xml_file, xml_path=xml_path)

logging.info(f"Anzahl Fahrten importiert {df_rt_vbn_fahrten.shape[0]}")

# %%
df_rt_vbn_fahrten.to_parquet(f"out/parquet/{server}/fahrten_{start.replace('-', '_')}.parquet")
df_rt_vbn_verlauf.to_parquet(f"out/parquet/{server}/verlauf_{start.replace('-', '_')}.parquet")

# %%
del df_rt_vbn_fahrten
del df_rt_vbn_verlauf

# %% [markdown]
# ## Für Zusatzfahrten
# 
# - Abfragen aller Daten über die RTTypes
#     - REALTIME_EXTRA und weitere
#     - DEVIATION_OF_SCHEDULED
#     - etc.

# %%

xml_request_zusatz_umleitung = f"""
                                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
               xmlns:v{api_version}="http://v{api_version}.export.service.data.archive.itcs.hafas.hacon.de/">
               <soapenv:Header/><soapenv:Body><v{api_version}:createArchiveJob>
               <filter>
                    <clientId>{clientID}</clientId>         
                    <startDate>{start}</startDate>
                    <endDate>{ende}</endDate>
                    <filterJourneyRtTypeList>REALTIME_EXTRA</filterJourneyRtTypeList>
                    <filterJourneyRtTypeList>REALTIME_EXTRA_REPLACEMENT</filterJourneyRtTypeList>
                    <filterJourneyRtTypeList>REALTIME_EXTRA_REPORTED</filterJourneyRtTypeList>
                    <filterJourneyRtTypeList>REALTIME_EXTRA_MAINTENANCE</filterJourneyRtTypeList>
                    <filterJourneyRtTypeList>DEVIATION_OF_SCHEDULED</filterJourneyRtTypeList>
                    <filterJourneyRtTypeList>DEVIATION_OF_REALTIME_EXTRA</filterJourneyRtTypeList>         
                    <filterJourneyRtTypeList>DEVIATION_OF_REPLACEMENT</filterJourneyRtTypeList>             
                    <filterJourneyRtTypeList>SUPPLEMENTARY</filterJourneyRtTypeList>'                
                    <filterJourneyRtTypeList>UNKNOWN</filterJourneyRtTypeList>               
                    <hasRealtime>ALL</hasRealtime>
               </filter>
               </v{api_version}:createArchiveJob></soapenv:Body></soapenv:Envelope>
               """

# %%
print_pretty_xml(xml_request_zusatz_umleitung)

# %%
xml_path_pre = 'api_xml'
server = 'prod'
xml_file = f"rt_archiv_{start}_zusatz.xml"
api_version = 1

#Festlegen Prod oder Demosystem
if server == 'prod':
    clientID = config['CLIENT_ID_PROD'] #prod
    api_version = 15
    matrix = False #ab Version 15 true möglich
    myUrl = f"https://fahrplaner.vbn.de/archive/services/archiveExportService/v{api_version}?wsdl"
else:
    clientID = config['CLIENT_ID_DEMO'] #demo
    api_version = 15
    matrix = True #ab Version 15
    myUrl = f"https://vbn.demo.hafas.de/archive/services/archiveExportService/v{api_version}?wsdl"

xml_path = os.path.join(xml_path_pre, server)
xml_out = os.path.join(xml_path_pre, server, xml_file)

request_xml(api_version=api_version, xml_request=xml_request_zusatz_umleitung, xml_out=xml_out, myUrl=myUrl)

df_rt_zusatz = import_rt_xml_to_df_fahrten(xml_file=xml_out)

xml_to_targz(xml_file=xml_file, xml_path=xml_path)

# %% [markdown]
# ## Schreiben der Daten nach Parquet

# %%
df_rt_zusatz.to_parquet(f"out/parquet/{server}/zusatz_{start.replace('-', '_')}.parquet")

# %%
logging.info(f"Anzahl Fahrten Zusatz importiert {df_rt_zusatz.shape[0]}")


