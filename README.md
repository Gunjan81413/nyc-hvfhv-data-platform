# NYC HVFHV Data Platform

An end-to-end data engineering project built using real NYC High Volume
For-Hire Vehicle (HVFHV) trip data.

## Project Objective

Build a scalable analytical data platform that ingests, processes,
models, and analyzes NYC HVFHV trip data using modern data engineering
practices.

The project focuses on:

- Batch and incremental data ingestion
- Medallion architecture
- PySpark transformations
- Data quality
- Dimensional data modeling
- Delta Lake
- Analytical workloads
- Business intelligence

## Architecture

NYC TLC HVFHV Data
        ↓
    Raw Storage
        ↓
     Bronze
        ↓
     Silver
        ↓
  Dimensional Model
        ↓
      Gold
        ↓
    Power BI

## Technology Stack

- Python
- PySpark
- Databricks
- Delta Lake
- AWS S3
- SQL
- Power BI
- Git & GitHub

## Dataset

The project uses real NYC Taxi & Limousine Commission
High Volume For-Hire Vehicle trip data.

Source:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Project Status

🚧 Under Development

### Planned Stages

- [ ] Project setup
- [ ] Raw data ingestion
- [ ] Bronze layer
- [ ] Silver layer
- [ ] Data quality checks
- [ ] Dimensional data model
- [ ] Gold layer
- [ ] Incremental ingestion
- [ ] Performance optimization
- [ ] Power BI dashboard
- [ ] Documentation