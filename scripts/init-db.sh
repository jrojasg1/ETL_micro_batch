#!/bin/bash
set -e

# Create the 'taxes' table
psql -U $POSTGRES_USER -d $POSTGRES_DB <<-EOSQL
  CREATE TABLE IF NOT EXISTS taxes (
    id TEXT PRIMARY KEY,
    registro_id TEXT,
    anio INT,
    destino TEXT,
    estrato INT,
    consumo NUMERIC(10,2),
    tax_calculada NUMERIC(10,2),
    tarifa_sobre_consumo NUMERIC(4,3),
    minimo NUMERIC(12,1),
    maximo FLOAT
  );
EOSQL

# Create the 'tracking_table' table
psql -U $POSTGRES_USER -d $POSTGRES_DB <<-EOSQL
  CREATE TABLE IF NOT EXISTS tracking_table (
    id SERIAL PRIMARY KEY,
    table_name TEXT,
    count_records INT,
    sum_calculated_tax DOUBLE PRECISION,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
EOSQL
