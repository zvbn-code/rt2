# Import und Auswertung RT Archiv über API
- auf Version 15 z.B. mit jemals Echtzeit gelieferten Fahrten
- Auswerten aller Fahrten eines Tages und Schreiben nach Parquet
- Gesamtauswertung über diese Dateien auch um Traffic zu sparen

## Skripte zum Import und zur Verabeitung


### class_rt_duck.py
- Auslagerung von Funktionen und Klassen aus den Skripten

### rt_api_import.ipynb
- Abfrage der Daten für den gesamten VBN für einen Tag
- Aufbereitung xml und Umwandlung in Dataframe
- Ablage der Ergebnisse in out/parquet

### rt_api_import_matrix.ipynb
- Import inkl. der wischenzeitlich gemeldetem Werte (entspricht der Matrixansicht im Echtzeitarchiv)

### rt_einzelauswertung.ipynb
- verschiedene Sonderauswertungen z.B. für den Qualitätsbericht des VBN

### rt_auswertung_parquet.ipynb
- Auswertung analog der bisherigen Auswertung mit Erstellung der Echtzeitquoten
- Auswertung der Quoten Echtzeit je Linie 
- Häufung von Fahrten ohne Echtzeit
- Abgleich mit Zusatzfahrten

### run_rt2.sh
- Skript zur Automatisierung

## Ergebnisdateien

### Übersichtsdarstellung alle Bündel im ZVBN
- Hauptlinien (Ebene 1 und 2, Stadtverkehr gemäß NVP) 
- https://daten.zvbn.de/rt_archiv/log_12_pivot.html 

### Einzeldarstellung je Bündel und Linie
- https://daten.zvbn.de/rt_archiv/buendel/

### Auswertung Matrixdarstellung (Zwischenergebnisse) des SPNV
- https://daten.zvbn.de/rt_matrix/

### Ausfall je Bündel
- https://daten.zvbn.de/rt_archiv/ausfall_pivot.html

### Detaillierte Ergebnisdateien im Excel-Format, die den Unternehmen zur Verfügung gestellt werden
- https://daten.zvbn.de/rt_archiv/ (z.B. dh_nordost_stat.xlsx)

## Beispieldateien aufbereitete Rohdaten
Über die API werden die Daten als xml geliefert. Für die weitere Aufbereitung werden diese als Parquet-Dateien tagesscharf abgelegt.
Im Ordner parquet_beispiel finden sich 
- fahrten*.parquet Auflistung aller Merkmale je Fahrt
- verlauf*.parquet Auflistung der Verspätungen (letzte Prognosemeldung) im Fahrtverlauf 
- matrix_spnv*.parquet Auflistung auch der zwischenzeitlichen Echtzeitmeldungen für den SPNV
- zusatz*.parquet Auflistung von Zusatzfahrten für die Qualitätskontrolle

Die Auswertung dieser Parquet-Dateien kann z.B. mit [DuckDB](https://duckdb.org/docs/current/data/parquet/overview) erfolgen, welches einfach auf den Ordner mit den Dateien zugreift

``SELECT *, filename
FROM read_parquet('test/*.parquet');``