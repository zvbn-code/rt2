import duckdb
from dotenv import load_dotenv, dotenv_values

config = dotenv_values(".env")

class rt_duck:    
    #db_name=':memory:'
    db_name = 'db/rt_archiv.db'
    
    def __init__(self, db_name=db_name):
        # Initialize the DuckDB connection
        self.conn = duckdb.connect(database=db_name)
        self.cursor = self.conn.cursor()
        self.cursor.sql(f"""INSTALL postgres;
                            LOAD postgres;
                            ATTACH 'dbname=zvbn_postgis user={config['POSTGRES_USER']} host=127.0.0.1 password={config['POSTGRES_PW']}' AS db_dm (TYPE POSTGRES, READ_ONLY);"""
                        )
        self.cursor.sql("create or replace table lin_buendel as select * from db_dm.basis.lin_buendel")
        sql_lin = """
        Create or replace table linien as 
        SELECT nummer AS linie, buendel, ebene, dlid, id 
        FROM db_dm.basis.linien 
        WHERE buendel IS NOT NULL AND aktiv IS TRUE 
        ORDER BY buendel, ebene, nummer """
        self.cursor.sql(sql_lin)

    def create_table_fahrten(self, server):
        """ erstellt eine Tabelle fshrten aus den Parquet Files Fahrten fahrten_yyyy_mm_dd.parquet"""
        sql_create = f"create or replace table fahrten as select * from read_parquet('out/parquet/{server}/fahrten*.parquet',  union_by_name = true, filename = true)"
        self.cursor.execute(sql_create)
        #self.cursor("update fahrten ")
        self.cursor.sql("alter table fahrten add column if not exists lineid_short VARCHAR")
        self.cursor.sql("""update fahrten 
                set lineid_short = concat_ws(':', split_part(lineid,':', 1), split_part(lineid,':', 2), split_part(lineid,':', 3))""")
        self.cursor.sql("""select distinct lineid, 
                concat_ws(':', split_part(lineid,':', 1), split_part(lineid,':', 2), split_part(lineid,':', 3)) 
                from fahrten""")

        print("Table 'fahrten' created.")

    def create_table_zusatz(self, server):
        """ erstellt eine Tabelle zusatz aus den Parquet Files Fahrten zusatz_yyyy_mm_dd.parquet"""
        sql_create = f"create or replace table zusatz as select * from read_parquet('out/parquet/{server}/zusatz*.parquet',  union_by_name = true, filename = true)"
        self.cursor.execute(sql_create)
        print("Table 'zusatz' created.")

    def create_table_verlauf(self, server):
        """ erstellt eine Tabelle zusatz aus den Parquet Files Fahrten verlauf_yyyy_mm_dd.parquet"""
        sql_create = f"create or replace table verlauf as select * from read_parquet('out/parquet/{server}/verlauf*.parquet',  union_by_name = true, filename = true)"
        self.cursor.execute(sql_create)
        print("Table 'verlauf' created.")

    def create_table_matrix(self, server):
        """ erstellt eine Tabelle matrix aus den Parquet Files Fahrten matrix_yyyy_mm_dd.parquet"""
        sql_create = f"create or replace table matrix as select * from read_parquet('out/parquet/{server}/matrix*.parquet',  union_by_name = true, filename = true)"
        self.cursor.execute(sql_create)
        print("Table 'matrix' created.")

    def create_vw_buendel(self, buendel):
        """ erstellt oder ersetzt Sicht auf ein Linienbündel Fahrten mit dem Namen vw_buendel"""
        sql_buendel = f"""create or replace view vw_buendel as
                                (select f.datum, l.buendel, l.ebene, f.vu, f.fnr, f.fahrtstartstationname, f.fahrtendstationname, f.lineshort, f.lineid_short, f.hasrealtime, 
                                    f.journey_cancelled, f.reported_cancelled, f.ts_reported_cancelled, f.realtimeHasEverBeenReported         
                                from fahrten f                                         
                                left outer join linien l on f.lineid_short = l.dlid 
                                where buendel like '%{buendel}%') 
                                """
        self.cursor.execute(sql_buendel)        

    def anzahl_fahrten(self):        
        return self.cursor.sql("from fahrten").shape[0]
    
    def anzahl_fahrten_betreiber(self):
        return self.cursor.sql("select vu, count(vu) as count from fahrten group by vu order by count desc").df()
    
    def verbindung_schließen(self):
        """ Schließen der Duckdb DB Verbindung"""
        self.conn.close()
        print("Verbindung zur DB geschlossen")