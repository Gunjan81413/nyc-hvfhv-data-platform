# System Architecture

## High-Level Architecture

NYC TLC HVFHV
     |
     v
Raw Data Storage
     |
     v
Bronze Layer
     |
     v
Silver Layer
     |
     v
Dimensional Data Model
     |
     v
Gold Layer
     |
     v
Power BI

## Medallion Architecture

### Bronze

Contains raw source data with minimal transformation.

### Silver

Contains cleaned, validated, standardized, and enriched data.

### Gold

Contains business-ready fact and dimension tables optimized
for analytical workloads.

## Incremental Processing

January 2026 will initially be used as the historical development
dataset.

Subsequent monthly datasets will be processed incrementally through
the same pipeline.

## Data Source

NYC Taxi & Limousine Commission High Volume For-Hire Vehicle data.