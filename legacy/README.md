# Discogs Conversion Tools
Script Python per convertire i dump Discogs (JSON + XML) in Parquet leggibili da Trino/Hive.

Contiene:
- convert_labels_fix.py  
- clean_labels_ref_clean.py  
- parse_discogs_artists_dump.py  
- parse_collection_json.py  

Esegue le conversioni dei tuoi dump in `/tmp/hive-data`, pronti per essere importati nel catalogo Hive.
