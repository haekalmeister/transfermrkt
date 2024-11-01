# Transfermrkt Data Pipeline

My first end-to-end data engineering project! 🎉 
This project extracts football transfer market data and creates a data pipeline using Airflow, Snowflake, and dbt.

## What I Built

I built a data pipeline that:
- Downloads dataset from Kaggle that updated regularly
- Loads it into Snowflake data warehouse
- Transforms raw data into analytical tables
- Creates useful data marts for analysis

## Tech Stack Used
- Python (for scripting and data processing)
- Apache Airflow (for orchestration)
- Snowflake (data warehouse)
- dbt (data transformation)
- Kaggle API (data source)

## Features I Implemented

### Data Marts:
1. **Player Current Club Tracking**
   - Finds where players currently play
   - Handles tricky cases like players moving outside Europe
   - Flags when data might be outdated

2. **Player Performance Stats**
   - Season-by-season stats
   - Goals, assists, cards
   - Age and position analysis
   - Performance by competition

3. **Team Performance Analysis**
   - Win/loss/draw stats
   - Goals scored and conceded
   - Team nationality info
   - Points per season

## What I Learned
- How to build a proper data pipeline
- Working with Airflow DAGs
- dbt modeling and transformations
- Data warehouse concepts
- Git version control
- Managing environment variables

## What's Next
Things I want to add:
- Different staging source
- Data visualization
- Containerization
