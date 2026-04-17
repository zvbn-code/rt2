# %% [markdown]
# # Auswertung der Parquet Dateien aus dem Echtzeitarchiv V14
# - Start in /home/zvbn/python/rt2/run_rt2.sh jeweils 06:30

# %% [markdown]
# ## Import der Module und Setzen Parameter

# %%
import pandas as pd
import sys
from dotenv import load_dotenv
import os
import datetime as dt
import calendar as cal

import shutil

from dotenv import dotenv_values
import importlib
import redmine
from redmine import delete_upload_dmsf
import logging
import glob

import openpyxl
from openpyxl import load_workbook, Workbook
from openpyxl.styles import NamedStyle, Font
from openpyxl.cell import WriteOnlyCell
import duckdb
from openpyxl.formatting.rule import ColorScaleRule, CellIsRule, FormulaRule
import matplotlib.pyplot as plt

# %%
os.chdir("/home/zvbn/python/rt2")

# %%
log_file = "log/log_rt.txt"
logging.basicConfig(filename=log_file, 
                        level=logging.INFO,
                        style="{",
                        format="{asctime} [{levelname:8}] {message}",
                        datefmt="%d.%m.%Y %H:%M:%S")

load_dotenv()

# %%
sys.path.append('/home/zvbn/python/rt2')

# %%
from class_rt_duck import RtDuck

# %%
logging.info("Auswertung RT aus parquet gestartet")

# %%
config = dotenv_values(".env")
#config

# %%
pd.options.display.max_columns = 100

# %%
jetzt = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
gestern= (dt.date.today() - dt.timedelta(1)).strftime('%Y-%m-%d')
letzte07tage= (dt.date.today() - dt.timedelta(7)).strftime('%Y-%m-%d')
letzte14tage= (dt.date.today() - dt.timedelta(14)).strftime('%Y-%m-%d')
letzte21tage= (dt.date.today() - dt.timedelta(21)).strftime('%Y-%m-%d')

print(jetzt, letzte21tage)

# %% [markdown]
# ## Funktionen

# %%
def replace_german_special_characters(text) -> str:
    replacements = {
        'ä': 'ae',
        'ö': 'oe',
        'ü': 'ue',
        'Ä': 'Ae',
        'Ö': 'Oe',
        'Ü': 'Ue',
        'ß': 'ss'
    }
    
    for german_char, replacement in replacements.items():
        text = text.replace(german_char, replacement)
    
    return text

# %%
#für die Formatierung der Ausgabe in html
func_proz = lambda s: str(int((1-s) * 1000)/10) + '%' if str(int(s)) != '-1' else '-'
func_date = lambda s: s.dt.strftime('%m/%d/%Y')

# %% [markdown]
# ## CSS Styles

# %%
#Zellformatierung CSS
cell_hover = {  # for row hover use <tr> instead of <td>
    'selector': 'td:hover',
    'props': [('background-color', '#ffffb3')]
}
index_names = {
    'selector': '.index_name',
    'props': 'font-style: italic; color: darkgrey; font-weight:normal; font-family: sans-serif;'
}
headers = {
    'selector': 'th:not(.index_name)',
    'props': 'background-color: #FFFFFF; color: #000000; font-family: sans-serif;'
}

td = {'selector' : 'td', 'props': 'text-align:right; font-family: sans-serif'}

# %% [markdown]
# ## Testen der class

# %%
rt = RtDuck()
rt

# %%
#Schließen der Verbindung
#rt.verbindung_schliessen()

# %%
rt.create_table_fahrten(server = 'prod', interval = 42)

# %%
rt.cursor.sql("select datum, count(*) from fahrten group by datum order by datum").df().tail(5)

# %%
df = rt.cursor.sql("""pivot (select fnr, datum::date as datum, hasRealtime from fahrten 
              where lineid_short in ('de:VBN:251','de:VBN:252','de:VBN:258')
              and datum > (current_date - 28))
      
              on  datum
              using sum(hasRealtime)
              order by fnr
              """).df()
df

# %%
styled = (
    df
      .style
      .set_table_styles([
          {'selector': 'th', 'props': [('font-family', 'Arial, sans-serif'),
                                       ('font-size', '14px'),
                                       ('font-weight', 'normal')]},
          {'selector': 'td', 'props': [('font-family', 'Arial, sans-serif'),
                                       ('font-size', '13px'),
                                       ('text-align', 'right')]}
      ])
      .format(precision=0)
      .background_gradient(axis=None, vmin=0, vmax=2, cmap="RdYlBu")
)
html_path = '/var/www/rt_archiv/fahrten_echtzeit_251_252_258.html'
styled.to_html(html_path, index=False)

# %% [markdown]
# ## Statistik Ausfälle

# %%
df_ausfall = rt.cursor.sql("""select datum, datum::date::text as datum_text, vu, count(*) as anzahl_ges, 
                           count(*) filter (where cancelled_kum = true) as anzahl_ausfall, 
              anzahl_ausfall::float / count(*) * 100 as quote_ausfall
              from fahrten 
              where 
                -- cancelled_kum = true and 
              datum >= (current_date - interval '60 days')
              group by all
              order by anzahl_ausfall desc

              -- limit 5
              """).df()

df_ausfall


# %%
list_vu_oepnv = ['Bremer Straßenbahn AG', 'Verkehr und Wasser GmbH (VWG)', 
           'BREMERHAVEN BUS', 'Weser-Ems-Bus Betrieb Bremen', 'Delbus GmbH & Co. KG']
df_pivot_oepnv = df_ausfall.query("vu.isin(@list_vu_oepnv)").pivot(index='datum', columns='vu', values='quote_ausfall')

list_vu_spnv = [ 'NordWestBahn', 'metronom Eisenbahngesellschaft mbH', 'DB Regio AG Nord', 'Regionalverkehre Start Deutschland (Niedersachsen-Mitte)']
df_pivot_spnv = df_ausfall.query("vu.isin(@list_vu_spnv)").pivot(index='datum', columns='vu', values='quote_ausfall')

fig, ax = plt.subplots(2,1, figsize=(24,15))
df_pivot_oepnv.plot(kind='line', 
                title='Tägliche Ausfallquote der Fahrten in % je Verkehrsunternehmen ÖPNV', 
                #figsize=(10,5), 
                grid=True, ax=ax[0]).legend(bbox_to_anchor=(1.05, 1), loc='upper left')

df_pivot_spnv.plot(kind='line', 
                title='Tägliche Ausfallquote der Fahrten in % je Verkehrsunternehmen SPNV', 
                #figsize=(10,5), 
                grid=True, ax=ax[1]).legend(bbox_to_anchor=(1.05, 1), loc='upper left')

fig.text(0.23, 0.01, 'Datenquelle: Echtzeitdaten der VBN-Verkehrsunternehmen Hacon Echtzeit-Archiv', ha='center', fontsize=8)
fig.text(0.8, 0.01, f'Erstellt am: {jetzt} \n Auswertung ZVBN', ha='right', fontsize=8)
ax[0].set_ylabel('Ausfallquote in %')
#ax[0].set_ylim(0, 10)

ax[1].set_ylabel('Ausfallquote in %')
#ax[1].set_ylim(0, 20)

plt.tight_layout()
plt.savefig('/var/www/rt_archiv/ausfall/ausfall.pdf', dpi=300)

# %% [markdown]
# ### Liste der Betreiber in Echtzeit Feld deviceid

# %%
rt.cursor.sql("select distinct str_split(deviceid, '#')[3], str_split(deviceid, '#')[2] from verlauf where operday = current_date - 2")

# %% [markdown]
# ## Auswertung Echtzeit Anzahl Fahrten je Betreiber
# - für Statistik Ausfall Wartung

# %%
rt.cursor.sql("select distinct vu from fahrten order by vu").df()

# %%
device_id = 'IVU'
list_vu = ['Gebken Reisen GmbH', 'Gerdes Reisen', 'AM Bus', 'AllerBus', 'Delmenhorst-Harpstedter Eisenbahn GmbH',
          'Eisenbahnen und Verkehrsbetriebe Elbe-Weser GmbH', 'Hutfilters Reisedienst GmbH & Co.' , 'Verkehrsbetriebe Grafschaft Hoya GmbH',
          'Verkehrsbetriebe Oldenburger Land', 'Verkehrsbetriebe Wesermarsch GmbH', 'Bremer Straßenbahn AG']
interval = 40
sql = f"""
        select datum::date::text as datum, count(*) as anzahl_ges,
        count(*) filter (where hasRealtime  = true) as anzahl_echtzeit,
        count(*) filter (where cancelled_kum = true) as anzahl_ausfall,

        from fahrten 
        where 
        vu in {tuple(list_vu)} and
        datum >= (current_date - interval {interval} day) 
        
        group by all
       
        order by datum
        """

# %%
rt.cursor.sql(sql).df()

# %%
list_vu = [ 'Bremer Straßenbahn AG']
interval = 7
sql = f"""
        select *

        from fahrten 
        where 
        vu in {tuple(list_vu)} and
        datum >= (current_date - interval {interval} day) 
        
        group by all
       
        order by datum
        """

# %%
rt.cursor.sql(sql).df().to_excel('/var/www/rt_archiv/ausfall/ausfall_bsag.xlsx', index=False)

# %% [markdown]
# ## Auswerten besonders hoher Verspätungen
# - auch als Klasse abgebildet

# %%
rt.hohe_verspaetung('', 90, 7)

# %%
stat_hohe_verspaetung = rt.hohe_verspaetung('', 60, 7)[['journeyOperator', 'datum', 'fnr']].groupby(['journeyOperator', 'datum'])\
    .count().reset_index().sort_values(by=['fnr'], ascending=False).rename(columns={'fnr':'Anzahl Fahrten hohe Verspätung'})

# %%
df_ausfall.columns

# %%
list_vu = ['Verkehrsbetriebe Oldenburger Land', 'AM Bus']
rt.hohe_verspaetung('IVU', 90, 7).query("journeyOperator.isin(@list_vu)").sort_values(by=['datum', 'lineshortname', 'fnr'], ascending=True)

# %%
excel_file = '/var/www/rt_archiv/hohe_verspaetung_ivu_regio.xlsx'
excel_file_wil = '/var/www/rt_archiv/hohe_verspaetung_wilmering.xlsx'
sn01 = 'versp regio rbl'
sn02 = 'versp alle vbn'
sn03 = 'statistik versp'
sn04 = 'statistik ausfaelle'


with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
    # für die IVU Regio Mandanten
    rt.hohe_verspaetung('IVU', 60, 7).to_excel(writer, index=False, sheet_name=sn01)
    ws = writer.sheets[sn01]
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = 'A2'
    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 25

    # für alle Unternehmen im VBN
    rt.hohe_verspaetung('', 60, 7).to_excel(writer, index=False, sheet_name=sn02)
    ws = writer.sheets[sn02]
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = 'A2'
    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 25

    # Statistik hohe Verspätungen
    stat_hohe_verspaetung.to_excel(writer, index=False, sheet_name=sn03)
    ws = writer.sheets[sn03]
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = 'A2'
    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 25

    # Statistik Ausfälle
    df_ausfall[['datum_text', 'vu', 'anzahl_ges', 'anzahl_ausfall',
       'quote_ausfall']].to_excel(writer, index=False, sheet_name=sn04)
    ws = writer.sheets[sn04]
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = 'A2'
    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 25

with pd.ExcelWriter(excel_file_wil, engine='openpyxl') as writer:
    # für Wilmering AM Bus
    list_vu = ['Verkehrsbetriebe Oldenburger Land', 'AM Bus']
    rt.hohe_verspaetung('IVU', 90, 7).query("journeyOperator.isin(@list_vu)").sort_values(by=['datum', 'lineshortname', 'fnr'], ascending=True).to_excel(writer, index=False, sheet_name=sn01)
    ws = writer.sheets[sn01]
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = 'A2'
    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 25


# %%
rt.cursor.sql("""from fahrten 
              where datum > '2025-08-20' and clientid like '%IVU%'
              limit 5""")

# %%
rt.cursor.sql("from linien where linie like '6__' order by buendel, linie limit 30")

# %%
rt.cursor.sql("""select * from read_parquet('out/parquet/prod/matrix_spnv_2026_03*.parquet',  union_by_name = true, filename = true) 
 limit 5""").df()

# %%
rt.create_table_zusatz(server = 'prod', interval = 42)
rt.create_table_verlauf(server = 'prod', interval = 42)
rt.create_table_matrix(server = 'prod', interval = 42)

# %%
rt.cursor.sql("select operday, count(*) from read_parquet('out/parquet/prod/verlauf_2026_04_1*.parquet',  union_by_name = true, filename = true) group by operday order by operday limit 10").df()

# %%
rt.cursor.sql("select operday, count(*) from verlauf group by operday order by operday").df().tail(5)

# %%
rt.cursor.sql("""select * 
              from zusatz 
              where datum = '2026-04-03' 
              -- and fahrtstarttime::text like '%06:15%'
              -- and destination like '%Bremen%'
              limit 5""").df()

# %% [markdown]
# ## Pünktlichkeit je Bündel

# %%
rt.cursor.sql("""select lineshort, count(*) as anz_ges, count(*) filter (where max_del <= 5) as anz_del5 , 
              round(anz_del5 / anz_ges, 3) as anteil_punktlich from
              (select f.datum, f.fnr, f.lineshort, max(dep_del) as max_del
              from fahrten f
              join linien l on f.lineshort = l.linie
              join verlauf v on  f.datum = v.operday and f.fnr = v.fnr and f.lineid = v.ex_lineid
              where l.buendel = 'OHZ Ost'
              and hasrealtime = true
              and f.datum >= '2025-09-01' and f.datum < '2026-12-31'
              and v.dep_del <= 60 -- Ausschluss extremer Werte
              group by all)
              group by all
              order by lineshort""")

# %%
rt.cursor.sql("select lineid, lineid_short, * from fahrten where datum::date = '2026-04-14' and lineid like 'de:VBN:740%' order by fnr")

# %% [markdown]
# ## Erstellen einer Auswertung mit abweichender Clientid (gesamt, BSAG, VWG)

# %%
rt.cursor.sql("from fahrten order by datum desc limit 3")

# %%
df_line_clientid = rt.cursor.sql("""select distinct lineid_short, vu, clientid, 
                                 min(datum)::date::text as min_datum,
                                 max(datum)::date::text as max_datum,
                                 count(datum) as anzahl,
                                 count(*) filter (where reported_cancelled  = true) as anzahl_reported_cancelled,
                                 count(*) filter (where journey_cancelled  = true) as anzahl_journey_cancelled
                                 from fahrten 
                                 where datum >= (current_date - interval 7 day) 
                                 group by all
                                 order by  max_datum desc, lineid_short""").df()

# %%
df_line_clientid_bsag = rt.cursor.sql("""select datum::date::text as datum, lineid_short, fnr, vu, clientid, 
                                 
                                 
                                  reported_cancelled ,
                                  journey_cancelled 
                                 from fahrten 
                                 where datum >= (current_date - interval 7 day) 
                                and vu = 'Bremer Straßenbahn AG' and clientid not in ( 'BSAG', '-')
                                 
                                 order by  datum desc, lineid_short""").df()

# %%
df_line_clientid_vwg = rt.cursor.sql("""select datum::date::text as datum, lineid_short, fnr, vu, clientid, 
                                 
                                 
                                  reported_cancelled ,
                                  journey_cancelled 
                                 from fahrten 
                                 where datum >= (current_date - interval 7 day) 
                                and vu = 'Verkehr und Wasser GmbH (VWG)' and clientid not in ( 'vwg', '-')
                                 
                                 order by  datum desc, lineid_short""").df()

# %%
# Export DataFrame to Excel
excel_file = '/var/www/rt_archiv/line_clientid.xlsx'
sn01 = 'alle kombination'
sn02 = 'BSAG kombination'
sn03 = 'VWG kombination'
with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
    df_line_clientid.to_excel(writer, index=False, sheet_name=sn01)

    # Apply autofilter
    writer.book[sn01].auto_filter.ref = 'A:H'

    # Freeze the first row
    writer.book[sn01].freeze_panes = 'A2'

    #Anpasssung der Spaltenbreite
    writer.book[sn01].column_dimensions['A'].width = 20
    writer.book[sn01].column_dimensions['B'].width = 30
    writer.book[sn01].column_dimensions['C'].width = 20
    writer.book[sn01].column_dimensions['D'].width = 20
    writer.book[sn01].column_dimensions['E'].width = 20
    writer.book[sn01].column_dimensions['F'].width = 20
    writer.book[sn01].column_dimensions['G'].width = 20
    writer.book[sn01].column_dimensions['H'].width = 20

    #Fahrten BSAG mit abweichender ClientID
    if len(df_line_clientid_bsag) > 0:
        df_line_clientid_bsag.to_excel(writer, index=False, sheet_name=sn02)
        writer.book[sn02].auto_filter.ref = 'A:G'
        writer.book[sn02].freeze_panes = 'A2'
        for c in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
            writer.book[sn02].column_dimensions[c].width = 20

    #Fahrten VWG mit abweichender ClientID
    if len(df_line_clientid_vwg) > 0:
        df_line_clientid_vwg.to_excel(writer, index=False, sheet_name=sn03)
        writer.book[sn03].auto_filter.ref = 'A:G'
        writer.book[sn03].freeze_panes = 'A2'
        for c in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
            writer.book[sn03].column_dimensions[c].width = 20

# %%
rt.cursor.sql("describe fahrten").df()

# %%
df_linien_nach_client = rt.cursor.sql( """select f.lineid_short, datum::date as datum, buendel, vu, clientid,
                round((count (*) filter (realtimeHasEverBeenReported = true) / count(*) ) * 100, 1) as anteil_ez  
              from fahrten f 
                join linien l on f.lineid_short = l.dlid 
              
              where datum::date > (current_date - interval 7 day)
              group by all""").df()

# %% [markdown]
# ## Erstellen der Statistiken log_12_pivot und log_3_pivot sowie Ausfall

# %%
rt.cursor.sql("from fahrten")

# %%
def sql_ebenen(ebenen):
            sql = f""" 
              select datum::date::text as datum, buendel, 
                round((count (*) filter (realtimeHasEverBeenReported = true) / count(*) ) * 100, 1) as anteil_ez  
              from fahrten f 
                join linien l on f.lineid_short = l.dlid 
              where 
                f.datum > (current_date - interval 60 day)
              and ebene in {ebenen}
              group by all"""
            
            return sql


# %%
def sql_ebenen_ausfall(ebenen):
            sql = f""" 
              select datum::date::text as datum, buendel, 
                round((count (*) filter (cancelled_kum = true) / count(*) ) * 100, 1) as anteil_ez  
              from fahrten f 
                join linien l on f.lineid_short = l.dlid 
              where 
                f.datum > (current_date - interval 60 day)
              and ebene in {ebenen}
              group by all"""
            
            return sql


# %% [markdown]
# ### Auswertung nach Ausfall HIM und VDV

# %%
buendel = 'HB Tram'
sql = f""" 
select datum::date::text as datum, linie, buendel, 
count(*), 
round((count (*) filter (cancelled_kum = true) / count(*) ) * 100, 1) as anteil_ausfall_cum ,
round((count (*) filter (reported_cancelled = true) / count(*) ) * 100, 1) as anteil_ausfall_reported,
round((count (*) filter (journey_cancelled = true) / count(*) ) * 100, 1) as anteil_ausfall_journey
from fahrten f 
join linien l on f.lineid_short = l.dlid 
where 
f.datum = '2026-04-13'
and buendel = '{buendel}'
group by all
order by datum desc, buendel"""

# %%
rt.cursor.sql(sql).df()

# %% [markdown]
# ### HTML Stil

# %%
#Zellformatierung CSS
cell_hover = {  # for row hover use <tr> instead of <td>
    'selector': 'td:hover',
    'props': [('background-color', '#ffffb3')]
}
index_names = {
    'selector': '.index_name',
    'props': 'font-style: italic; color: darkgrey; font-weight:normal; font-family: sans-serif;'
}
headers = {
    'selector': 'th:not(.index_name)',
    'props': 'background-color: #FFFFFF; color: #000000; font-family: sans-serif;'
}

td = {'selector' : 'td', 'props': 'text-align:right; font-family: sans-serif'}

# %%
ebenen = ('1+', '1', '2', 'Stadt', 'Nacht')
df = rt.cursor.sql(sql_ebenen(ebenen=ebenen)).df()
df_pivot = df.pivot(index='datum', columns='buendel', values='anteil_ez')
df_pivot.sort_values('datum', ascending=False).style.background_gradient(cmap="RdYlGn", axis = None,  vmin=0.0, vmax=95)\
    .highlight_null(color='white').format(formatter = '{:.1f}%', precision=1, na_rep='-', thousands=" ", decimal= ',').set_table_styles([index_names, headers, td])\
        .to_html('/var/www/rt_archiv/log_12_pivot.html', encoding='LATIN1')

# %%
ebenen = ('1+', '1', '2', 'Stadt', 'Nacht', 'SPNV')
df = rt.cursor.sql(sql_ebenen_ausfall(ebenen=ebenen)).df()
df_pivot = df.pivot(index='datum', columns='buendel', values='anteil_ez')
df_pivot.sort_values('datum', ascending=False).style.background_gradient(cmap="RdYlGn_r", axis = None,  vmin=0.0, vmax=10)\
    .highlight_null(color='white').format(formatter = '{:.1f}%', precision=1, na_rep='-', thousands=" ", decimal= ',').set_table_styles([index_names, headers, td])\
        .to_html('/var/www/rt_archiv/ausfall_pivot.html', encoding='LATIN1')

df_pivot

# %%
ebenen = "('3')"
df = rt.cursor.sql(sql_ebenen(ebenen=ebenen)).df()
df_pivot = df.pivot(index='datum', columns='buendel', values='anteil_ez')
df_pivot.sort_values('datum', ascending=False).style.background_gradient(cmap="RdYlGn", axis = None,  vmin=0.0, vmax=95)\
    .highlight_null(color='white').format(formatter = '{:.1f}%', precision=1, na_rep='-', thousands=" ", decimal= ',').set_table_styles([index_names, headers, td])\
        .to_html('/var/www/rt_archiv/log_3_pivot.html', encoding='LATIN1')

# %%
rt.anzahl_fahrten_betreiber()

# %%
rt.anzahl_fahrten_betreiber().to_html('/var/www/rt_archiv/anzahl_fahrten_betreiber.html', encoding='LATIN1')

# %% [markdown]
# ## Ermitteln der Fahrten, die nur 0 Min senden bzw. im Verlauf nur 0 gespeichert wurde

# %%
rt.cursor.sql(""" select * from
                (select ex_lineid, fnr,min(operday) as start, max(operday) as ende ,count(*) as count from (
                    select * from 
                        (select operday, ex_lineid, fnr, avg(dep_del) as avg_del
                        from verlauf 
                        where dep_del is not null
                        and has_rt = true
                        group by all)
                    where avg_del = 0 and ex_lineid like 'de:VBN:6__:%' and operday > (current_date - interval 28 day)
                    order by ex_lineid)               
                
                group by all
                order by count desc)
              where count > 3
              """).df()

# %%
rt.cursor.sql("select * from verlauf where fnr = '1630018'").df().to_excel('out/verlauf_1630018.xlsx', index=False)

# %%
suffix = 'mitte'
#auswahl_linien = '680|660|N68'
auswahl_linien = '630|670|N63|N67'

df_auswahl_ohne_rt = rt.cursor.sql(f"""select * from
              (select lineshort, min(datum)::date as min_datum, max(datum)::date as max_datum, fnr, 
              count(* ) as anzahl, 
              count(* ) filter (hasRealtime  = false ) as anzahl_ohne_rt, 
              anzahl_ohne_rt / count(* ) as proz_ohne_rt
              from fahrten  
              where lineid  SIMILAR TO '.*({auswahl_linien}).*' 
              -- and hasRealtime  = false 
              and datum  >= (current_date() - interval 28 days)
              group by all
              )
              where anzahl_ohne_rt > 1
              order by proz_ohne_rt desc

              -- limit 10""").df()

df_zusatz = rt.cursor.sql(f"""select * from zusatz where lineid  SIMILAR TO 'de:VBN:.*({auswahl_linien}).*' """).df()

ohne_rt_xl = f"out/rt_ohne_realtime_{suffix}.xlsx"
sn01 = 'ohne_rt'
with pd.ExcelWriter(ohne_rt_xl, engine='openpyxl') as writer:
    df_auswahl_ohne_rt.to_excel(writer, sheet_name=sn01, index=False)
    worksheet = writer.sheets[sn01]
    worksheet.freeze_panes = 'a2'

    worksheet.column_dimensions['B'].width = 15
    worksheet.column_dimensions['C'].width = 15
    worksheet.auto_filter.ref = worksheet.dimensions

    # Format the 'Zeit' column as date
    for cell in worksheet['B']:  # Assuming 'Zeit' is in column D
        if cell.row == 1:  # Skip the header row
            continue
        cell.number_format = 'YYYY-MM-DD'

    # Format the 'Zeit' column as date
    for cell in worksheet['C']:  # Assuming 'Zeit' is in column D
        if cell.row == 1:  # Skip the header row
            continue
        cell.number_format = 'YYYY-MM-DD'

    # Format the 'Prozent' column as percentage
    for cell in worksheet['G']:  # Assuming 'Prozent' is in column D
        if cell.row == 1:  # Skip the header row
            continue
        cell.number_format = '0.0%'
 
df_zusatz

# %%
df_linien_quote_rt = rt.cursor.sql("""
            select * from
              ( select lineshort, min(datum)::date as min_datum, max(datum)::date as max_datum,  
                    count(* ) as anzahl, 
                    count(* ) filter (hasRealtime  = false ) as anzahl_ohne_rt, 
                    anzahl_ohne_rt / count(* ) as proz_ohne_rt
                from fahrten  
                where 
                -- and hasRealtime  = false 
                    datum  >= (current_date() - interval 28 days)
                group by all
              )
              where anzahl_ohne_rt > 1
              order by proz_ohne_rt desc

              -- limit 10""").df()

df_linien_quote_rt

# %%
df_zusatz

# %% [markdown]
# ### Auswertung Matrix nach Verlauf Zeitpunkt der Meldung

# %%
df_matrix = rt.cursor.sql("""select m.operatingDay::date, m.lineShortName, m.journeyId, v.index, 
                          m.stationName, m.scheduleDeparture,m.delay_minutes_arrival, m.delay_minutes_departure, m.timestamp, v.arr_del, v.dep_del
                from matrix m
                left join verlauf v on 
                          m.operatingDay = v.operday and 
                          m.lineShortName = v.lineshortname and 
                          m.journeyId = v.fnr and 
                          m.stationName = v.station_name
                where stop_cancelled = false
              and m.lineShortName = 'RS3'
              order by  m.operatingDay, m.externalLineId, m.journeyId, v.index,  m.timestamp 
              
              -- limit 20""").df()

# %%
auswahl_linien = '640|630|670|N68|N63|N67|680|660|330|340'
df_zusatz = rt.cursor.sql(f"""
                select lineid, fnr, min(datum::date) as datum_min,  max(datum::date) as datum_max, count(*) as anzahl,  vu 
                from zusatz 
                where                       

                    lineid SIMILAR TO 'de:VBN:.*({auswahl_linien}).*' and 
                    -- and vu like 'Reisedienst von Rahden%' 
                    datum::date >= (current_date - interval 3 day)
                group by all 
                order by lineid, fnr """).df()

df_zusatz

#rt.cursor.sql(f"""select * from zusatz where lineid  SIMILAR TO 'de:VBN:.*({auswahl_linien}).*' and datum::date >= (current_date - interval 30 day)""").df()

# %%
rt.cursor.sql("select min(datum )::date as min_date, max(datum)::date as amx_date, count(*) as anzahl from fahrten")

# %% [markdown]
# ### Häufung von Fahrten ohne Echtzeit

# %%
# rt.create_vw_buendel('OL Nord')
# df_fahrten_ohne_ez = rt.cursor.sql("""
              
#                 select datum::date as datum, ebene, lineshort , fnr, hasrealtime
               
#                 from vw_buendel 
#                 where datum >= (current_date - interval 30 day) and hasrealtime = false
#                 group by all
#                 order by ebene, lineshort, fnr
    
#               """).df()

# df_fahrten_ohne_ez_zusatz = df_fahrten_ohne_ez.merge(df_zusatz, left_on = ['datum', 'fnr'], right_on = ['datum', 'fnr'], how='left')
# df_fahrten_ohne_ez_zusatz.query("~vu.isnull()") 

# df_fahrten_ohne_ez_zusatz[['lineshort_x','datum','fnr']].groupby(['lineshort_x','fnr'], as_index=False)\
#     .agg(datum_min=('datum', 'min'), datum_max=('datum', 'max'), count=('datum', 'count')).sort_values('count', ascending=False)\
#     .to_excel('out/rt_fahrten_ohne_ez_zusatz.xlsx', index=False)

# %%
#rt.cursor.sql("from vw_buendel")

# %%
#df_fahrten_ohne_ez_zusatz.query("~vu.isnull()")

# %%
interval_auswertung = 21
df_fahrten_mit_nicht_vollstaendiger_echtzeit = rt.cursor.sql(f"""
            select * from 
                (select ebene, lineshort , fnr, count(*) as anz, count(*) filter (hasRealtime) as anz_rt, 
                    (anz - anz_rt) as f_ohne_rt ,round(anz_rt/anz,2) as quote,
                    max(datum::date) filter (hasRealtime) as letzte_lieferung_echtzeit
                from vw_buendel 
                where datum >= (current_date - interval {interval_auswertung} day)
                group by all
                order by ebene, lineshort, fnr)
            where f_ohne_rt > 1 and ebene in ('1+','1', '2') 
            order by f_ohne_rt desc                                                             
            """).df()

df_fahrten_mit_nicht_vollstaendiger_echtzeit

# %%
xl = 'out/nicht_vollstaendig.xlsx'
sn01 = '01 fahrten_rt_kl_100_roz'
sn02 = '02 zusatzfahrten'
sn03 = '03 ohne ez merge zusatz'

with pd.ExcelWriter(xl, engine='openpyxl') as writer: 
    df_fahrten_mit_nicht_vollstaendiger_echtzeit.to_excel(writer, index=False, sheet_name=sn01)
    writer.book[sn01].freeze_panes = 'A2'
    writer.book[sn01].auto_filter.ref='A:H'

    df_zusatz.to_excel(writer, index=False, sheet_name=sn02)
    writer.book[sn02].freeze_panes = 'A2'
    writer.book[sn02].auto_filter.ref='A:H'

    #df_fahrten_ohne_ez_zusatz.to_excel(writer, index=False, sheet_name=sn03)
    #writer.book[sn03].freeze_panes = 'A2'
    #writer.book[sn03].auto_filter.ref='A:H'


# %%
q = rt.cursor.sql("""
                   (select 
                    datum::date as datum, ebene, lineshort, lineid_short, count(*) anz,
                    count(*) filter (hasRealtime) anz_rt, round(anz_rt/ anz,2) anteil_rt, 
                    max(datum) filter (hasRealtime) letzte_lieferung
                    from vw_buendel 
                    where datum >= date_trunc('month', (date_trunc('month',current_date) - interval 1 day)::date)
                    and datum <= (date_trunc('month',current_date) - interval 1 day)::date
                  
                    group by all

                    order by datum::date)
                  """)
#q.filter("lineshort in ('S35', '350')") #mit filter einfache Abfragen

q

# %%
#Abfrage für den letzten Monat
q_pivot_lm = rt.cursor.sql("""
                    pivot (select 
                            datum::date as datum, ebene, lineshort, lineid_short, count(*) anz,
                            count(*) filter (hasRealtime) anz_rt, round(anz_rt/ anz,2) anteil_rt
                        from vw_buendel 
                        where datum >= date_trunc('month', (date_trunc('month',current_date) - interval 1 day)::date)
                            and datum <= (date_trunc('month',current_date) - interval 1 day)::date
                        group by all
                        )
                    on datum
                    using sum(anteil_rt)
                    group by lineshort, ebene
                    order by ebene, lineshort""")

q_pivot_lm.df().fillna('-')

# %% [markdown]
# ## Ausgabe je Bündel als html / xlsx

# %% [markdown]
# ### Erstellen der sortierten Bündelliste

# %%
list_buendel = sorted(rt.cursor.sql("select distinct buendel from linien where buendel not in ('nahsh')").df()['buendel'].to_list())

# %%
#Zellformatierung CSS
cell_hover = {  # for row hover use <tr> instead of <td>
    'selector': 'td:hover',
    'props': [('background-color', '#ffffb3')]
}
index_names = {
    'selector': '.index_name',
    'props': 'font-style: italic; color: darkgrey; font-weight:normal; font-family: sans-serif; font-size: 15px;'
}
headers = {
    'selector': 'th:not(.index_name)',
    'props': 'background-color: #FFFFFF; color: #000000; font-family: sans-serif; font-size: 15px;text-orientation: upright;'
}

td = {'selector' : 'td', 'props': 'text-align:right; font-family: sans-serif; font-size: 14px;'}

# %%
for b in list_buendel[8:9]:
    print(b, b.replace(' ', '_').lower(), replace_german_special_characters(b).replace(' ', '_').lower())

# %%
date_style = NamedStyle(name="date_style", number_format="YYYY-MM-DD")
eine_nachkomma = NamedStyle(name = 'eine_nachkomma', number_format= '#,##0.0')
zwei_nachkomma = NamedStyle(name = 'eine_nachkomma', number_format= '#,##0.00')

# %%
list_buendel

# %%
rt.create_vw_buendel('OL Nord')

# %%
rt.cursor.sql("select * from vw_buendel  ")

# %%
df = rt.cursor.sql("""pivot (select fnr, datum::date as datum, hasRealtime from vw_buendel 
                        where datum > (current_date - 42))
                        on  datum
                        using sum(hasRealtime)
                        order by fnr
""").df()
df

# %%
func_proz = lambda s: str(int((1-s) * 1000)/10) + '%' if str(int(s)) != '-1' else '-'
func_date = lambda s: s.dt.strftime('%m/%d/%Y')

interval_auswertung = 21

date_style = NamedStyle(name="date_style", number_format="YYYY-MM-DD")
eine_nachkomma = NamedStyle(name = 'eine_nachkomma', number_format= '#,##0.0')
zwei_nachkomma = NamedStyle(name = 'eine_nachkomma', number_format= '#,##0.00')

for b in list_buendel[0:500]:
    print(b, b.replace(' ', '_').lower(), replace_german_special_characters(b).replace(' ', '_').lower())

    rt.create_vw_buendel(b)
    rt.create_vw_buendel_verlauf(buendel=b)
    rt.cal_rel(36)
    #Abfrage für die letzten {interval_auswertung} Tage
    q_pivot_lm = rt.cursor.sql(f"""
                        pivot (select 
                                datum::date as datum, ebene, lineshort, lineid_short, count(*) anz,
                                count(*) filter (realtimeHasEverBeenReported ) anz_rt, round(anz_rt/ anz,2) anteil_rt
                            from vw_buendel 
                            where datum >= (current_date - interval {interval_auswertung} day)
                            group by all
                            )
                        on datum
                        using sum(anteil_rt)
                        group by lineshort, ebene
                        order by ebene, lineshort""")
    
    #Liste der Fahrten ohne Echtzeit die häufiger als 1 mal vorkommen
    df_fahrten_mit_nicht_vollstaendiger_echtzeit = rt.cursor.sql(f"""
                select * from 
                    (select ebene, lineshort , fnr, count(*) as anz, count(*) filter (hasRealtime) as anz_ez, 
                    (anz - anz_ez) as fahrten_ohne_ez ,round(anz_ez/anz,2) as quote,
                    max(datum::date) filter (realtimeHasEverBeenReported ) as letzte_lieferung_echtzeit
                    from vw_buendel 
                    where datum >= (current_date - interval {interval_auswertung} day)
                    group by all
                    order by ebene, lineshort, fnr)
                where fahrten_ohne_ez > 1 and ebene in ('1+','1', '2','Nacht') 
                    order by fahrten_ohne_ez desc                                                             
                
                """).df()
    
    df_fahrten_ohne_ez = rt.cursor.sql(f"""              
                select datum::date as datum, ebene, lineshort , fnr, hasrealtime               
                from vw_buendel 
                where datum >= (current_date - interval {interval_auswertung} day) and realtimeHasEverBeenReported  = false
                group by all
                order by ebene, lineshort, fnr
    
              """).df()
    
    df_fahrten_gesamt = rt.cursor.sql(f"""              
                select *               
                from vw_buendel 
                where datum >= (current_date - interval {interval_auswertung} day) 
                order by ebene, lineshort, fnr
    
              """).df()
    
    # Auswertung der Zusatzfahrten muss gesondert erfolgen 15.04.
    #html_zusatz_table = 'html/pre_zusatz.html'
    #df_fahrten_ohne_ez_zusatz = df_fahrten_ohne_ez.merge(df_zusatz, left_on = ['datum', 'fnr'], right_on = ['datum', 'fnr'], how='left')
    #df_fahrten_ohne_ez_zusatz.query("~vu.isnull()").to_html(html_zusatz_table, index=False)

    html_pre_table = 'html/pre_table.html'
    df_fahrten_mit_nicht_vollstaendiger_echtzeit.to_html(html_pre_table, index=False)

    html_pre_pivot = 'html/pre_pivot.html'
    q_pivot_lm.df().style.background_gradient(cmap="RdYlGn", axis = None,  vmin=0.5, vmax=1).highlight_null(color='white')\
        .format( precision=2, na_rep='-', thousands=" ")\
        .highlight_null(color='white')\
        .set_table_styles([index_names, headers, td])\
        .to_html(html_pre_pivot)
    
    # Save the HTML table to a file (optional)   
    with open(html_pre_pivot, 'r') as file:
        html_pre_pivot = file.read()
    
    # Load the HTML page template
    with open('html/template.html', 'r') as file:
        html_template = file.read()

    # Insert the HTML table into the template
    title = f"Echtzeitquote Bündel {b} je Linie erstellt: {dt.datetime.now().strftime('%d.%m.%Y %H:%M')}" 
    html_page = html_template.replace('{{ html_pivot }}', html_pre_pivot).replace('{{ html_title }}', title)

    if df_fahrten_mit_nicht_vollstaendiger_echtzeit.shape[0] > 0:
        html_page = html_page.replace('{{ html_table }}', df_fahrten_mit_nicht_vollstaendiger_echtzeit.to_html(index=False))
    else:
        html_page = html_page.replace('{{ html_table }}', "Keine Häufung Fahrten ohne Echtzeit")

    #if df_fahrten_ohne_ez_zusatz.query("~vu.isnull()").shape[0] > 0:
    #    html_page = html_page.replace('{{ html_table_zusatz }}', df_fahrten_ohne_ez_zusatz.query("~vu.isnull()").to_html(index=False))
    #else:
    #    html_page = html_page.replace('{{ html_table_zusatz }}', "Keine Zusatzfahrten mit gleicher Fahrtnummer")

    # Save the combined HTML page to a file
    html_combined = f"/var/www/rt_archiv/buendel/rt_{replace_german_special_characters(b).replace(' ', '_').lower()}.html"


    with open(html_combined, 'w') as file:
        file.write(html_page)

    #Ausgabe der wichtigen Ergebnisse als Excel
    xl = f"buendel_stat/{replace_german_special_characters(b).replace(' ', '_').lower()}_stat.xlsx"

    sn00 = '01 hilfe'
    sn01 = '02 statistik ebene'
    sn02 = '03 statistik pivot'
    sn03 = '04 fahrten gesamt'
    sn04 = '05 verlauf' #nicht im Stadtverkehr
    sn05 = '06 Echtzeit je Fahrt' #nicht im Stadtverkehr

    wb = Workbook(write_only=True)
    #Erstellen des Hilfeblattes an erster Position
    ws = wb.create_sheet(title=sn00, index=0)
     
    ws.append([f"Erstellt: {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}"])    
    ws.append(["Erläuterung der Werte in der Tabelle"])
    ws.append([f"Blatt {sn01} enthält die Echtzeitquote der Ebenen des Bündels {b} für die letzten {interval_auswertung} Tage"])
    ws.append([f"Blatt {sn02} enthält die Echtzeitquote der Linien des Bündels {b} für die letzten {interval_auswertung} Tage"])
    ws.append([f"Blatt {sn03} filterbare Liste der Fahrten {b} für die letzten {interval_auswertung} Tage"])
    ws.append([f"Blatt {sn05} enthält die Echtzeitdaten je Fahrt des Bündels {b} für die letzten {interval_auswertung} Tage"])

    # Create cell with styling
    # bold_font = Font(bold=True)
    # cell = WriteOnlyCell(ws, value="Header")
    # cell.font = bold_font
    # ws.append([cell])

    # Blatt 01 Vorfälle
    ws = wb.create_sheet(title=sn01, index=1)
    df_vorfaelle = rt.cursor.sql("from cal_rel").df().merge(rt.df_vorfaelle_echtzeit(36), on = 'datum', how='left')
    ws.append(df_vorfaelle.columns.tolist())
    for r in df_vorfaelle.itertuples(index=False):
        ws.append(r)
    ws.freeze_panes = 'A2'
    ws.auto_filter.ref='A:H'

    #Statistik Pivot
    ws = wb.create_sheet(title=sn02, index=2)
    ws.append(q_pivot_lm.df().columns.tolist())
    for r in q_pivot_lm.df().itertuples(index=False):
        ws.append(r)

    #Fahrten gesamt
    ws = wb.create_sheet(title=sn03, index=3)
    ws.append(df_fahrten_gesamt.columns.tolist())
    for r in df_fahrten_gesamt.itertuples(index=False):
        ws.append(r)
    ws.freeze_panes = 'A2'
    ws.auto_filter.ref='A:N'
    
    #Verlauf nicht im Stadtverkehr wegen Datenmenge
    if b not in ('HB Bus', 'HB Tram', 'BHV', 'DEL', 'OL Stadt'):
        ws = wb.create_sheet(title=sn04, index=4)
        df_verlauf = rt.cursor.sql("""from vw_buendel_verlauf""").df()
        ws.append(df_verlauf.columns.tolist())
        for r in df_verlauf.itertuples(index=False):
            ws.append(r)
    #         rt.cursor.sql("""from vw_buendel_verlauf""").df().to_excel(writer, index=False, sheet_name=sn04)
    
    wb.save(xl)
    # with pd.ExcelWriter(xl, engine='openpyxl') as writer:
    #     writer.book.add_named_style(date_style)

    #     #02 Statistik Ebene
    #     rt.cal_rel(36)
    #     df_vorfaelle = rt.cursor.sql("from cal_rel").df().merge(rt.df_vorfaelle_echtzeit(36), on = 'datum', how='left')
    #     df_vorfaelle.to_excel(writer, index=False, sheet_name=sn01)
    #     writer.book[sn01].freeze_panes = 'A2'
    #     writer.book[sn01].auto_filter.ref=f'A1:H{df_vorfaelle.shape[0]+1}'
    #     for row in writer.book[sn01].iter_rows(min_row=2, min_col=1, max_col=1):
    #             for cell in row:
    #                 cell.style = date_style

    #     for row in writer.book[sn01].iter_rows(min_row=2, min_col=7, max_col=7):
    #             for cell in row:
    #                 cell.style = zwei_nachkomma
    #     writer.book[sn01][f"H{df_vorfaelle.shape[0]+3}"] = f"=SUBTOTAL(9, H2:H{df_vorfaelle.shape[0]+1})"
    #     writer.book[sn01].column_dimensions['A'].width = 15
    #     # Add a three-color scale

    #     writer.book[sn01].conditional_formatting.add(f'G1:G{df_vorfaelle.shape[0]+1}',
    #                         ColorScaleRule(start_type='num', start_value=0.0, start_color='AA0000',
    #                         mid_type='num', mid_value=0.5, mid_color='FFFF00',
    #                         end_type='num', end_value=1.0, end_color='00AA00')
    #                          )

    #     #Statistik Pivot
    #     q_pivot_lm.df().to_excel(writer, index=True, sheet_name=sn02)
    #     writer.book[sn02].freeze_panes = 'A2'
    #     writer.book[sn02].auto_filter.ref='A:H'

    #     df_fahrten_gesamt.to_excel(writer, index=False, sheet_name=sn03)
    #     writer.book[sn03].freeze_panes = 'A2'
    #     writer.book[sn03].auto_filter.ref='A:N'
    #     writer.book[sn03].column_dimensions['A'].width = 15
    #     for row in writer.book[sn03].iter_rows(min_row=2, min_col=1, max_col=1):
    #             for cell in row:
    #                 cell.style = date_style

    #     #Verlauf nicht im Stadtverkehr wegen Datenmenge
    #     if b not in ('HB Bus', 'HB Tram', 'BHV', 'DEL', 'OL Stadt'):

    #         rt.cursor.sql("""from vw_buendel_verlauf""").df().to_excel(writer, index=False, sheet_name=sn04)
    #         writer.book[sn04].freeze_panes = 'A2'
    #         writer.book[sn04].auto_filter.ref='A:J'
    #         writer.book[sn04].column_dimensions['A'].width = 15
    #         writer.book[sn04].column_dimensions['F'].width = 30
    #         for row in writer.book[sn04].iter_rows(min_row=2, min_col=1, max_col=1):
    #             for cell in row:
    #                 cell.style = date_style

    #     #Echtzeit je Fahrt nicht im Stadtverkehr wegen Datenmenge
    #     if b not in ('HB Bus', 'HB Tram', 'BHV', 'DEL', 'OL Stadt'):
    #         df = rt.cursor.sql(f"""pivot (select fnr, datum::date as datum, hasRealtime from vw_buendel 
    #         where datum > (current_date - {interval_auswertung}))
    #         on  datum
    #         using sum(hasRealtime)
    #         order by fnr
    #         """).df()
            
    #     df.to_excel(writer, index=True, sheet_name=sn05)
    #     writer.book[sn05].freeze_panes = 'A2'
             

    # # Öffnen des Workbooks und Anwenden der Formatierung
    # wb = openpyxl.load_workbook(xl)

    # #Erstellen des Hilfeblattes an erster Position
    # wb.create_sheet(sn00, index=0)
    # sheet = wb[sn00]
    # sheet['A1'] = f"Erstellt: {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}"
    # sheet['A2'] =  "Erläuterung der Werte in der Tabelle"
    # sheet['A3'] = f"Blatt {sn01} enthält die Echtzeitquote der Ebenen des Bündels {b} für die letzten {interval_auswertung} Tage"
    # sheet['A4'] = f"Blatt {sn02} enthält die Echtzeitquote der Linien des Bündels {b} für die letzten {interval_auswertung} Tage"
    # sheet['A5'] = f"Blatt {sn03} filterbare Liste der Fahrten {b} für die letzten {interval_auswertung} Tage"
    # sheet['A6'] = f"Blatt {sn05} enthält die Echtzeitdaten je Fahrt des Bündels {b} für die letzten {interval_auswertung} Tage"

    # wb.save(xl)       

# %% [markdown]
# ### Übersichtstabelle Vorfälle letzte Monate

# %%
#Ausgabe der wichtigen Ergebnisse als Excel
xl = "buendel_stat/uebersicht_stat.xlsx"

sn01 = '01 vormonat'
sn02 = '02 akt monat'

with pd.ExcelWriter(xl, engine='openpyxl') as writer:
    writer.book.add_named_style(date_style)
    rt.stat_monat(1).to_excel(writer, index=False, sheet_name=sn01)
    writer.book[sn01].freeze_panes = 'A2'
    writer.book[sn01].auto_filter.ref='A:H'
    rt.stat_monat(0).to_excel(writer, index=False, sheet_name=sn02)
    writer.book[sn02].freeze_panes = 'A2'
    writer.book[sn02].auto_filter.ref='A:H'

    for row in writer.book[sn01].iter_rows(min_row=2, min_col=1, max_col=1):
                for cell in row:
                    cell.style = date_style

    writer.book[sn01].column_dimensions['A'].width = 15

    for row in writer.book[sn02].iter_rows(min_row=2, min_col=1, max_col=1):
            for cell in row:
                cell.style = date_style
    writer.book[sn02].column_dimensions['A'].width = 15

shutil.copyfile(xl, '/var/www/rt_archiv/uebersicht_stat.xlsx')

# %% [markdown]
# ### Upload nach Redmine

# %%
importlib.reload(redmine)
from redmine import delete_upload_dmsf

# %%
#2
list_excel = glob.glob('buendel_stat/*.xlsx')
list_redmine = pd.read_csv('input/folder_vms.csv', sep=';', quotechar="'")['buen'].to_list()
df_redmine  = pd.read_csv('input/folder_vms.csv', sep=';', quotechar="'")

for b in list_buendel:
    b_clean = replace_german_special_characters(b).replace(' ', '_').lower()
    folder_file_name = os.path.join('buendel_stat', f"{b_clean}_stat.xlsx") 
    file_name =  f"{b_clean}_stat.xlsx"

    shutil.copyfile(folder_file_name, f'/var/www/rt_archiv/{file_name}')

    if b_clean not in list_redmine:
        print(f"not in {b_clean}")
    
    else:
        print(f"{b} in {b_clean}")
        project_url = df_redmine.query(f"buen == '{b_clean}'").reset_index().at[0, 'project_url']
        folder_id = df_redmine.query(f"buen == '{b_clean}'").reset_index().at[0, 'folder_id']

        delete_upload_dmsf(project_url=project_url, folder_id=folder_id, file_name=file_name, folder_file_name=folder_file_name)
        logging.info(f"{folder_file_name} hochgeladen")


# %%
logging.info(f"Anzahl Fahrten gesamt {rt.anzahl_fahrten()}")

# %%
rt.verbindung_schliessen()

# %%



