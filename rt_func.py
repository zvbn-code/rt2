#Benutzerdefinierte Funktionen für die Echtzeitstatsitik
#Stand 03.11.2024
# Numpy 2.0 NaN > nan

import numpy as np

#Rückgabewert falls xml-Attribut nicht definiert
def isnone(xml) -> str:
    """Ersetzen eines leeren xml text durch leeren String"""
    if xml is not None:
        return str(xml.text)
    else:
        return ''

def isnone_delay(xml):
    """Ersetzen eines leeren xml text durch np.nan"""
    if xml is not None:
        return xml.text
    else:
        return np.nan

# Umwandlung des Zeitstrings in hh:mm
def format_zeit(zeit):
    """Umformatieren der Zeitformate in h:m"""
    if len(str(zeit)) > 10:
        h = str(zeit)[8:10]
        m = str(zeit)[10:12]
        s = str(zeit)[12:14]
        return h + ':' + m 
    else:
        return ''

def split_deviceid(xml):
    """Extrahieren der ClientID aus der DeviceID """
    try:
        list = str(xml.text).split('#')
        clientid = list[2]        
    except:
        clientid = '-'
    return clientid

def isnone_boolean(xml) -> bool:
    """Ersetzen True / False bei Boolean-Werten, bei nicht vorhandenen Werten False"""
    try:
        if str(xml.text).lower() == 'true':
            val = True
        else:
            val = False
    except:
        val = False
    return val


def ebene_agg(ebene) -> str:
    """Zusammenfassen der Ebenen 1, 1+ und 2 und Ermittlung der Vorfälle"""
    if ebene in ('1', '2', '1+'):
        ebene_agg = '1_2'
    else:
        ebene_agg = ebene
    return ebene_agg


def vorfaelle_virtuell(ebene_agg, trip_id_all, anteil_true, trip_rt_false):
    """Berechnung der Vorfälle unter Einbeziehung der Ebene 3 (nicht vertragsrelevant)"""
    if ebene_agg == '1_2':
        if anteil_true >= 0.85:
            vorfaelle = 0
        else:
            #begrenzt die Zahl der Vorfälle auf die Zahl der Fahrten
            vorfaelle_pre = int((1 - 0.05 - anteil_true) * 10)
            if vorfaelle_pre > trip_rt_false:
                vorfaelle = trip_rt_false
            else:
                vorfaelle = vorfaelle_pre
    elif ebene_agg == '3':
        if anteil_true >= 0.75:
            vorfaelle = 0
        else:
            vorfaelle_pre = int((1 - 0.15 - anteil_true) * 10)
            if vorfaelle_pre > trip_id_all:
                vorfaelle = trip_id_all
            else:
                vorfaelle = vorfaelle_pre
    else:
        vorfaelle = 0
    return vorfaelle

#Berechnung gemäß den meisten neueren Verträgen für die Ebene 1 und 2
def vorfaelle(ebene_agg, trip_id_all, anteil_true, trip_rt_false):
    if ebene_agg == '1_2':
        if anteil_true >= 0.85:
            vorfaelle = 0
        else:
            #begrenzt die Zahl der Vorfälle auf die Zahl der Fahrten ohne Echtzeit
            vorfaelle_pre = int((1 - 0.05 - anteil_true) * 10)
            if vorfaelle_pre > trip_rt_false:
                vorfaelle = trip_rt_false
            else:
                vorfaelle = vorfaelle_pre
    elif ebene_agg == '3':
        vorfaelle = 0
    else:
        vorfaelle = 0
    return vorfaelle

def dlid_create(externallinid):
    """Kürzen der DLID bei Teillinien Aufteilung nach Subunternehmen"""
    dlid_pre = externallinid.split(':')[0:3]
    dlid = ':'.join(dlid_pre)
    return dlid

def secret_code(buendel):
    """bildet aus der Bündelbezeichnung einen eindeutigen Code ohne Umlaute """
    buen = buendel.lower().replace(u'ü','ue').replace(u'ä','ae').replace(u'ö','oe').replace(' ', '_')
    jj = 0
    for i in buendel_lower:
        try: 
            j = string.ascii_lowercase.index(i)
            jj = jj + j
        except:
            pass
    jj = jj + 1000
    return str(jj)


def del_up_dmsf(project_url, folder_id, file, file_name, key_redmine= 'b9a402948f0db83704b8d0e6324abc65ff80bdbe'):
    """
    Upload nach redmine Steuerungsprojekt ZVBN
    
    """

    import requests
    import xml.etree.ElementTree as ET
    #key_redmine = 'b9a402948f0db83704b8d0e6324abc65ff80bdbe'
    dirUrl = project_url + 'dmsf.xml?folder_id=' + str(folder_id)
    print(dirUrl)
    rdir = requests.get(dirUrl, headers={'X-Redmine-API-Key': key_redmine})
    #Löschen der Dateien im Echtzeitordner
    for child in ET.fromstring(rdir.text).iter('file'):
        for f in child.iter('id'):
            print(f.tag, f.text)
            delUrl = 'https://vms.zvbn.de/dmsf/files/'+f.text+'.xml?commit=yes'
            rdel = requests.delete(delUrl, headers={'X-Redmine-API-Key': key_redmine})
            print(rdel)
    
    #Upload der Datei und Abfragen des Tokens
    urlUp = project_url + 'dmsf/upload.xml?filename=' + file_name
    print(urlUp)
    file_input = open(file, 'rb').read()
    rdu = requests.post(urlUp, headers={'X-Redmine-API-Key': key_redmine, "Content-Type": "application/octet-stream"},data=file_input)
    for child in ET.fromstring(rdu.text).findall('token'):
        print(child.tag, child.text)
    #Erzeugen der Commit XML

    xml_commit = """ <?xml version="1.0" encoding="utf-8" ?>
    <attachments>
      <folder_id>""" +str(folder_id)+"""</folder_id>
      <uploaded_file>
        <name>""" +file_name+"""</name>
        <title>""" +file_name+"""</title>
        <description>REST API</description>
        <version>3</version> <!-- It must be 3 (Custom version) -->
        <custom_version_major>1</custom_version_major> <!-- Major version -->
        <custom_version_minor>0</custom_version_minor>
        <comment>From API</comment>
        <!-- For an automatic version: -->
        <version/>
        <token>""" +child.text+"""</token>
      </uploaded_file>
    </attachments>"""
    print(xml_commit)
    url4 = project_url + 'dmsf/commit.xml'
    print(url4)
    rdc = requests.post(url4, headers={'X-Redmine-API-Key': key_redmine, "Content-Type": "application/xml"}, data=xml_commit)
    print(rdc)
    return rdc

#Prüfen ob Längen von Arrays 0 Heraufsetzen des Wertes
def mineins(laenge):
    if laenge == 0:
        return 1
    else:
        return laenge
        

def erzeuge_gdf_line(df_in):
    """
    Ermitteln der Luftliniendistanz zur nächsten Haltestelle
    und erzeugen eines Geodataframs
    Linestring aus dem shapely Modul
    setzt Fahrtnummern voraus, die bei der BSAG nicht vorhanden sind
    """
    from shapely.geometry import Point, Polygon, LineString
    import geopandas as gpd
    df = df_in.copy()
    felder3 = ['tag','dlid','buendel','externallinid' ,'fahrtnummer', 'index', 'stationnumber' ,'stationname','realtime', 'ab_soll', 'an_soll', 'ab_delay', 'an_delay', 'lat', 'lon']
    felder4 = ['externallinid' ,'fahrtnummer', 'index', 'stationnumber', 'stationname' ,'ab_soll', 'an_soll', 'ab_delay', 'an_delay', 'lat', 'lon']
    df_auf_pre = df[felder3].join(df[felder4].shift(-1), how='outer', rsuffix=('_n')).copy()
    df = df_auf_pre[df_auf_pre.fahrtnummer == df_auf_pre.fahrtnummer_n].copy()
    df['linestring'] = df.apply(lambda x: LineString([(x.lon, x.lat), (x.lon_n, x.lat_n)]), axis = 1) 
    gdf_line_4326                 = gpd.GeoDataFrame(df, geometry = 'linestring', crs = 4326) 
    gdf_line_32632                = gdf_line_4326.to_crs(epsg = 32632).copy() #Projizieren auf UTM32N 
    gdf_line_32632['laenge_ll']   = gdf_line_32632.apply(lambda x: int(x.linestring.length), axis = 1)
    gdf_line_32632['fahrzeit_s']  = gdf_line_32632.apply(lambda x: (x.an_soll_n - x.ab_soll).seconds, axis = 1)
    #Ermittlung unplausibler Geschwindigkeiten
    #Logik bei Entfernungen < 1000m sind auch 0 Minuten ok sonst bei 0 Minuten 999
    def v_plausi( ll , fz):
        if fz == 0 and ll > 1000:
            wert = 999
        elif fz == 0 and ll <= 1000:
            wert = 1
        else:
            wert = (ll / fz) * 3.6
        return wert
    gdf_line_32632['km_h_ll']     = gdf_line_32632.apply(lambda x: v_plausi(x.laenge_ll , x.fahrzeit_s) , axis = 1)
    
    #gdf_line_32632['km_h_ll']     = gdf_line_32632.apply(lambda x: ((x.laenge_ll / x.fahrzeit_s) * 3.6) if x.fahrzeit_s != 0 else 999, axis = 1)
    gdf_line_32632['delay_check'] = gdf_line_32632.apply(lambda x: x.an_delay_n - x.an_delay, axis = 1)    
    return [gdf_line_32632]  #Ausgabe der Geodataframes