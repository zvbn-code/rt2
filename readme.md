# Import und Auswertung RT Archiv über API
- auf Version 15 z.B. mit jemals Echtzeit gelieferten Fahrten
- Auswerten aller Fahrten eines Tages und Schreiben nach Parquet
- Gesamtauswertung über diese Dateien auch um Traffic zu sparen

## Skripte zum Import und zur Verabeitung

### rt_api_import.ipynb
- Abfrage der Daten für den gesamten VBN für einen Tag
- Aufbereitung xml und Umwandlung in Dataframe
- Ablage der Ergebnisse in out/parquet

### rt_api_import_matrix.ipynb


### rt_einzelauswertung.ipynb

### rt_auswertung_parquet.ipynb
- Auswertung analog der bisherigen Auswertung mit Erstellung der Echtzeitquoten
- Auswertung der Quoten Echtzeit je Linie 
- Häufung von Fahrten ohne Echtzeit
- Abgleich mit Zusatzfahrten

## Ergebnisdateien

### Übersichtsdarstellung alle Bündel im ZVBN
- Hauptlinien (Ebene 1 und 2, Stadtverkehr gemäß NVP) 
- https://daten.zvbn.de/rt_archiv/log_12_pivot.html 

### Einzeldarstellung je Bündel und Linie
- https://daten.zvbn.de/rt_archiv/buendel/

### Auswertung Matrixdarstellung (Zwischenergebnisse) des SPNV
- https://daten.zvbn.de/rt_matrix/