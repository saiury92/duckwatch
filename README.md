# Duckwatch

It uses DuckDB to create a view of the data. It watches a folder for new CSV files and converts them to Parquet files. It then creates a view of the data in DuckDB.

## Usage

1. **Build + start the watcher**

```bash
docker-compose up -d --build
```

2. **Watch logs**

```bash
docker-compose logs -f duckwatch
```

3. **Query the database**

```bash
docker compose run --rm duckdb-cli ./mydb.duckdb

SELECT * FROM all_data;

SELECT region, SUM(sales) AS total_sales FROM all_data GROUP BY region;

.quit
```
