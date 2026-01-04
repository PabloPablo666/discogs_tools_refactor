# Discogs Hive Restore Guide

Questa guida descrive come ripristinare l’ambiente completo (Hive + Trino + script Python) da zero.

## Passaggi principali
1. **Reinstalla Python e le librerie** (`pandas`, `fastparquet`, `lxml`, `tqdm`).
2. **Estrai gli script** da `discogs_tools_backup_*.zip`.
3. **Ripristina i dati Hive** con `tar xzf discogs_hive_data_backup_*.tgz -C /`.
4. **Riavvia Docker** e verifica che i container siano attivi.
5. **Ricrea i cataloghi in Trino** se necessario (schema `hive.discogs`, tabelle `artists_ref_ready` e `labels_ref_ready`).
6. **Rilancia la collezione JSON** con `parse_collection_json.py` se serve aggiornare le release.

Tutti i dati Parquet si trovano in `/tmp/hive-data`, pronti per essere mappati nel metastore Hive.
