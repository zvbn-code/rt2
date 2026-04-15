#!/bin/bash
# Abfragen aller Daten im VBN und schreiben in ein parquet file

heute=$(date +%Y-%m-%d)
gestern=$(date -d @$(( $(date +"%s") - 86400)) +"%Y-%m-%d")

echo $heute $gestern

source /home/zvbn/python/rt2/.venv/bin/activate
#/home/zvbn/python/rt2/.venv/bin/jupyter nbconvert --to notebook --execute /home/zvbn/python/rt2/rt_api2_prod_duckdb.ipynb --allow-errors
#/home/zvbn/python/rt2/.venv/bin/jupyter nbconvert --output-dir='/home/zvbn/python/rt2/log' --to notebook --execute /home/zvbn/python/rt2/rt_api_import.ipynb --allow-errors
python /home/zvbn/python/rt2/rt_api_import.py $gestern
sleep 1m # Wartezeit bis die Daten vollständig geschrieben sind

#/home/zvbn/python/rt2/.venv/bin/jupyter nbconvert --output-dir='/home/zvbn/python/rt2/log' --to notebook --execute /home/zvbn/python/rt2/rt_api_import_matrix.ipynb --allow-errors
python /home/zvbn/python/rt2/rt_api_import_matrix.py $gestern
sleep 1m # Wartezeit bis die Daten vollständig geschrieben sind
#/home/zvbn/python/rt2/.venv/bin/jupyter nbconvert --output-dir='/home/zvbn/python/rt2/log' --to notebook --execute /home/zvbn/python/rt2/rt_auswertung_parquet.ipynb --allow-errors
#Ausführen als Python anstatt Notebook, da es hier zu Problemen mit der Ausführung kommt, wenn die Datenmenge zu groß ist. Es wird dann zu viel Speicher benötigt und die Ausführung bricht ab.
python /home/zvbn/python/rt2/rt_auswertung_parquet.py
