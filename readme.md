# Umstellung Auswertung RT Archiv
- auf Version 15 z.B. mit jemals Echtzeit gelieferten Fahrten
- Auswerten aller Fahrten eines Tages und Schreiben nach Parquet
- Gesamtauswertung über diese Dateien auch um Traffic zu sparen

## /home/zvbn/python/rt2/rt_api2_05_duckdb.ipynb
- Abfrage der Daten für den gesamten VBN für einen Tag
- Aufbereitung xml und Umwandlung in Dataframe
- Ablage der Ergebnisse in out/parquet

## /home/zvbn/python/rt2/rt_auswertung_parquet.ipynb
- Auswertung analog der bisherigen Auswertung mit Erstellung der Echtzeitquoten

## /home/zvbn/python/rt2/rt_auswertung_parquet.ipynb
- Auswertung der Quoten Echtzeit je Linie 
- Häufung von Fahrten ohne Echtzeit
- Abgleich mit Zusatzfahrten