""" Klasse zur Verarbeitung der Echtzeitdaten aus dem Hacon Echtzeitarchiv in DuckDB"""

import duckdb
from dotenv import dotenv_values
import seaborn as sns

config = dotenv_values(".env")

class RtDuck:
    """ Klasse zur Verarbeitung der Echtzeitdaten aus dem Hacon Echtzeitarchiv in DuckDB"""    
    #db_name=':memory:'
    db_name = 'db/rt_archiv.db' #als FileDB

    def __init__(self, db_name:str=db_name) -> None:
        """ Initialize the DuckDB connection und Verbindung zur DuckDB"""
        self.conn = duckdb.connect(database=db_name)
        self.cursor = self.conn.cursor()
        self.cursor.sql(f"""INSTALL postgres;
                            LOAD postgres;
                            ATTACH 'dbname=zvbn_postgis user={config['POSTGRES_USER']} 
                            host=127.0.0.1 password={config['POSTGRES_PW']}' AS db_dm (TYPE POSTGRES, READ_ONLY);
                            """
                        )
        self.cursor.sql("""create or replace table lin_buendel as 
                        select * from db_dm.basis.lin_buendel""")
        sql_lin = """
        Create OR replace table linien AS 
        SELECT nummer AS linie, buendel, ebene, dlid, id 
        FROM db_dm.basis.linien 
        WHERE buendel IS NOT NULL AND aktiv IS TRUE 
        ORDER BY buendel, ebene, nummer """
        self.cursor.sql(sql_lin)

    def create_table_fahrten(self, server:str) -> None:
        """ erstellt eine Tabelle fshrten aus den Parquet Files Fahrten fahrten_yyyy_mm_dd.parquet"""
        sql_create = f"""create or replace table fahrten as select * 
            from read_parquet('out/parquet/{server}/fahrten*.parquet',  union_by_name = true, filename = true)"""
        self.cursor.execute(sql_create)
        #self.cursor("update fahrten ")
        self.cursor.sql("alter table fahrten add column if not exists lineid_short VARCHAR")
        self.cursor.sql("""update fahrten 
                set lineid_short = concat_ws(':', split_part(lineid,':', 1), split_part(lineid,':', 2), split_part(lineid,':', 3))""")
        self.cursor.sql("""select distinct lineid,
                concat_ws(':', split_part(lineid,':', 1), split_part(lineid,':', 2), split_part(lineid,':', 3)) 
                from fahrten""")

        print("Table 'fahrten' created.")

    def create_table_zusatz(self, server:str) -> None:
        """ erstellt eine Tabelle zusatz aus den Parquet Files Fahrten zusatz_yyyy_mm_dd.parquet"""
        sql_create = f"create or replace table zusatz as select * from read_parquet('out/parquet/{server}/zusatz*.parquet',  union_by_name = true, filename = true)"
        self.cursor.execute(sql_create)
        print("Table 'zusatz' created.")

    def create_table_verlauf(self, server:str) -> None:
        """ erstellt eine Tabelle zusatz aus den Parquet Files Fahrten verlauf_yyyy_mm_dd.parquet"""
        sql_create = f"""create or replace table verlauf as select * 
            from read_parquet('out/parquet/{server}/verlauf*.parquet',  union_by_name = true, filename = true)"""
        self.cursor.execute(sql_create)
        self.cursor.sql("alter table verlauf add column if not exists lineid_short VARCHAR")
        self.cursor.sql("""update verlauf set lineid_short = concat_ws(':', split_part(ex_lineid,':', 1), split_part(ex_lineid,':', 2), split_part(ex_lineid,':', 3))""")
        print("Table 'verlauf' created.")

    def create_table_matrix(self, server:str) -> None:
        """ erstellt eine Tabelle matrix aus den Parquet Files Fahrten matrix_yyyy_mm_dd.parquet"""
        sql_create = f"create or replace table matrix as select * from read_parquet('out/parquet/{server}/matrix*.parquet',  union_by_name = true, filename = true)"
        self.cursor.execute(sql_create)
        print("Table 'matrix' created.")

    def create_vw_buendel(self, buendel:str) -> None:
        """ erstellt oder ersetzt Sicht/View auf ein Linienbündel Fahrten mit dem Namen vw_buendel"""
        sql_buendel = f"""create or replace view vw_buendel as
                                (select f.datum, l.ebene, f.vu, f.fnr, f.fahrtstartstationname, 
                                f.fahrtendstationname, f.lineshort, f.lineid_short, f.hasrealtime, 
                                f.journey_cancelled, f.reported_cancelled, f.ts_reported_cancelled, f.realtimeHasEverBeenReported         
                                from fahrten f                                         
                                left outer join linien l on f.lineid_short = l.dlid 
                                where buendel like '%{buendel}%') 
                                """
        self.cursor.execute(sql_buendel)        

    def create_vw_buendel_verlauf(self, buendel:str) -> None:
        """ erstellt oder ersetzt Sicht/View auf ein Linienbündel Fahrten mit dem Namen vw_buendel"""
        sql = f"""create or replace view vw_buendel_verlauf as
                                (select v.operday, l.ebene, v.fnr, v.index, v.station_nr, 
                                v.station_name, v.arr_del, v.dep_del,
                                v.reported_cancelled, v.canc from verlauf v
                                left join linien l on l.dlid = v.lineid_short 
                                where l.buendel = '{buendel}' 
                                    and v.operday::date >= (current_date - interval '35 days')::date
                                order by v.operday asc, v.fnr, v.index)
              
                                """
        self.cursor.execute(sql)

    def anzahl_fahrten(self) -> int:
        """ Ermittelt die Gesamtzahl der Fahrten in der Tabelle fahrten"""        
        return self.cursor.sql("from fahrten").shape[0]
    
    def anzahl_fahrten_betreiber(self):
        """ Anzahl der Fahrten pro Betreiber mit Anteil an Echtzeitdaten und farblicher Hinterlegung
        Farbpallette hue 0 = rot 120 = grün  https://en.wikipedia.org/wiki/Hue#/media/File:HueScale.svg""
        """
        return self.cursor.sql("""select vu,
                    count(vu) as count_ges, 
                    count(vu) filter (realtimeHasEverBeenReported = true) as count_rt, 
                    count(vu) filter (datum::date = (current_date - interval 1 day)) as heute_minus_1_ges,
                    count(vu) filter (datum::date = (current_date - interval 1 day) and realtimeHasEverBeenReported = true) as heute_minus_1_rt,
                    round((heute_minus_1_rt / heute_minus_1_ges) * 100, 2) anteil_heute_minus_1, 
              
                    count(vu) filter (datum::date = (current_date - interval 2 day)) as heute_minus_2_ges,
                    count(vu) filter (datum::date = (current_date - interval 2 day) and realtimeHasEverBeenReported = true) as heute_minus_2_rt,
                    round((heute_minus_2_rt / heute_minus_2_ges) * 100, 2) anteil_heute_minus_2,
                    count(vu) filter (datum::date = (current_date - interval 3 day)) as heute_minus_3_ges,
                    count(vu) filter (datum::date = (current_date - interval 3 day) and realtimeHasEverBeenReported = true) as heute_minus_3_rt,
                    round((heute_minus_3_rt / heute_minus_3_ges) * 100, 2) anteil_heute_minus_3

              from fahrten 
              group by vu order 
              by count_ges desc""").df().style.format({
        'anteil_heute_minus_1': '{:.1f}%',
        'anteil_heute_minus_2': '{:.1f}%',
        'anteil_heute_minus_3': '{:.1f}%'
        }).background_gradient(cmap=sns.diverging_palette(h_neg=0, h_pos=120, as_cmap=True), subset=['anteil_heute_minus_1', 'anteil_heute_minus_2', 'anteil_heute_minus_3'])
    
    def verbindung_schliessen(self) -> None:
        """ Schließen der Duckdb DB Verbindung"""
        self.conn.close()
        print("Verbindung zur DB geschlossen")

    def df_vorfaelle_echtzeit(self) -> None:
        """ Ermittelt die Quoten Echtzeitdaten und Vorfaelle"""

        sql = """
            select *,
              case 
              when quote >= 0.85 and ebene_group = 'ebene_1_2' then 0
              when quote < 0.85 and ebene_group = 'ebene_1_2' then ((0.95 - quote) * 10)::int
              else 0
              end as vorfaelle
              
              from 
              (
                select datum, ebene_group, sum(anz)::int as anz, sum(anz_rt)::int as anz_rt
                , sum(anz_rt)::float / sum(anz)::float as quote
        
                from
                
                (
                select datum, 
                ebene, 
                        
                    CASE 
                        WHEN ebene IN ('1+', '1', '2') THEN 'ebene_1_2'
                        WHEN ebene IN ('3') THEN 'ebene_3'
                        ELSE 'andere'
                    END AS ebene_group,
                    
                    count(*) as anz, 
                    count(*) filter (realtimeHasEverBeenReported ) as anz_rt
                    from vw_buendel
                    where datum >= (current_date - interval 35 day)
                    group by all
                    order by datum, ebene
                )
                
                where ebene_group in ('ebene_1_2', 'ebene_3')
                group by all
                order by datum, ebene_group)

        """

        return self.cursor.sql(sql).df()