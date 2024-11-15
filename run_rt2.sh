#!/bin/bash
# Abfragen aller Daten im VBN und schreiben in ein parquet file

source /home/zvbn/python/rt2/.venv/bin/activate
#/home/zvbn/python/rt2/.venv/bin/jupyter nbconvert --to notebook --execute /home/zvbn/python/rt2/rt_api2_prod_duckdb.ipynb --allow-errors
/home/zvbn/python/rt2/.venv/bin/jupyter nbconvert --output-dir='/home/zvbn/python/rt2/log' --to notebook --execute /home/zvbn/python/rt2/rt_api_import.ipynb --allow-errors
/home/zvbn/python/rt2/.venv/bin/jupyter nbconvert --output-dir='/home/zvbn/python/rt2/log' --to notebook --execute /home/zvbn/python/rt2/rt_api_import_matrix.ipynb --allow-errors
/home/zvbn/python/rt2/.venv/bin/jupyter nbconvert --output-dir='/home/zvbn/python/rt2/log' --to notebook --execute /home/zvbn/python/rt2/rt_auswertung_parquet.ipynb --allow-errors