# Umstellung Auswertung RT Archiv
- auf Version 15 z.B. mit jemals Echtzeit geliefert
- Auswerten aller Fahrten eines Tages und Schreiben nach Parquet
- Gesamtauswertung über diese Dateien auch um Traffiq zu sparen

## /home/zvbn/python/rt2/rt_api2_05_duckdb.ipynb
- Abfrage der Daten für den gesamten VBN für einen Tag
- Ablage der Ergebnisse in out/parquet

## /home/zvbn/python/rt2/rt_auswertung_parquet.ipynb
- Auswertung analog der bisherigen Auswertung mit Erstellung der Echtzeitquoten