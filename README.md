# Reto 2 — Observación de telemetría (BDA 26-27)

Equipo de 4 personas: una fuente por integrante. Responsable de JSONL: [pendiente de nombre].

## Fuentes verificadas (25 nov 2026)

- PostgreSQL REST: hadoop-master1.tartangalh.eus:3000 — CAÍDA (ConnectionRefused).
- Kafka: hadoop-master1.tartangalh.eus:9092 — topic ethazi_telemetria, 3 particiones.
- CSV en HDFS: /ethazi/fuentes/csv — 80 ficheros, edificios E001-E003.
- JSONL en HDFS: /ethazi/fuentes/json — 80 ficheros, dispositivos S001-S005.

Usar siempre el FQDN: "hadoop-master1" no resuelve. El análisis detallado está en
ANALISIS_FUENTES.md, que no se versiona.

## Convenciones

UTC · ids canónicos · Bronze byte-a-byte · Parquet+Snappy · partición dt por fecha de evento ·
los originales permanecen en HDFS sin modificar.

## Estructura

- src/ = código real (fuente de verdad); ingest/ lleva un .py por fuente.
- notebooks/ = vacío por ahora. Se creará un .ipynb por fuente/capa cuando se confirme
  el entorno (Zeppelin o Jupyter). La lógica real vive en src/*.py; el notebook solo
  importa y demuestra. Limpiar los outputs antes de commit.
- dags/ = Airflow; sql/ = DDL/HQL de Hive; configs/ = umbrales y crosswalk de métricas.
- data/ = espejo local del lake (bronze, silver, gold), ignorado salvo samples/.
  El lake auténtico está en HDFS: /ethazi/bronze, /ethazi/silver, /ethazi/gold.
- docs/ = memoria acumulativa, arquitectura y evidencias de cada hito.

## Estado

Repo local y GitHub público. Projects y Airflow pendientes. GitHub con incidencias el 25 nov.

## Pendiente

API REST del puerto 3000, permisos HDFS de escritura, leer un evento real de Kafka
y obtener el usuario de GitHub del profesor para darle acceso al Project.
