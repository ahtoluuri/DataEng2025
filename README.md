# DataEng2025
Here you can find the instructions on how to run the Data Engineering 2025 project for team 11

## Environment variables
Copy `.env.example` to `.env` and fill in your own values if needed (default values are provided):
```bash
cp .env.example .env
```

## Run
Start the necessary services
```bash
docker compose up -d
```
Create the tables
```bash
docker exec -it clickhouse-server clickhouse-client --multiquery --queries-file=/sql/01_create_db_and_tables.sql
```

[Clickhouse](http://localhost:8123)

[Airflow](http://localhost:8080)

## Troubleshooting
If Airflow can't be accessed, this might help:
```bash
mkdir logs dags plugins
sudo chown 50000:0 logs dags plugins
docker compose up airflow-init
```
## Airflow DAGs
DAG citibike_monthly_ingest:

<img width="1155" height="153" alt="image" src="https://github.com/user-attachments/assets/10ed9bcf-58cf-43f2-abd6-62ae1b5ce3e2" />

DAG weather_monthly_ingest:

<img width="647" height="153" alt="image" src="https://github.com/user-attachments/assets/b11d71ff-46ed-4db1-8c8f-0a780b7de3a2" />

## Results of analytical queries (limited to 10 rows)

How do weather conditions impact daily and hourly ridership volumes?

**Daily**

| full_date   | apparent_temp | precipitation_category | wind_category | total_trips |
|-------------|----------------|------------------------|---------------|-------------|
| 2025-08-31  | Warm           | No Rain                | Breezy        | 846         |
| 2025-08-31  | Warm           | No Rain                | Calm          | 1           |
| 2025-09-01  | Mild           | No Rain                | Breezy        | 31,011      |
| 2025-09-01  | Mild           | No Rain                | Calm          | 20,970      |
| 2025-09-01  | Warm           | No Rain                | Breezy        | 33,662      |
| 2025-09-01  | Warm           | No Rain                | Calm          | 1,758       |
| 2025-09-01  | Warm           | No Rain                | Windy         | 54,460      |
| 2025-09-02  | Mild           | Light Rain             | Breezy        | 2,193       |
| 2025-09-02  | Mild           | No Rain                | Breezy        | 31,823      |
| 2025-09-02  | Mild           | No Rain                | Calm          | 26,398      |

**Hourly**

| full_date   | hour | apparent_temp | precipitation_category | wind_category | hourly_trips |
|-------------|------|----------------|------------------------|---------------|--------------|
| 2025-08-31  | 14   | Warm           | No Rain                | Breezy        | 2            |
| 2025-08-31  | 16   | Warm           | No Rain                | Breezy        | 1            |
| 2025-08-31  | 17   | Warm           | No Rain                | Breezy        | 5            |
| 2025-08-31  | 18   | Warm           | No Rain                | Breezy        | 5            |
| 2025-08-31  | 19   | Warm           | No Rain                | Breezy        | 5            |
| 2025-08-31  | 20   | Warm           | No Rain                | Calm          | 1            |
| 2025-08-31  | 21   | Warm           | No Rain                | Breezy        | 9            |
| 2025-08-31  | 22   | Warm           | No Rain                | Breezy        | 26           |
| 2025-08-31  | 23   | Warm           | No Rain                | Breezy        | 793          |
| 2025-09-01  | 0    | Warm           | No Rain                | Breezy        | 2,841        |


**What is the average trip duration under different precipitation categories?**

| precipitation_category | avg_trip_duration_minutes |
|------------------------|---------------------------|
| No Rain                | 13.29                     |
| Light Rain             | 12.53                     |
| Heavy Rain             | 11.57                     |


**Are certain stations more popular as starting points during adverse weather (e.g., heavy rain) compared to dry hours?**

| station_name         | precipitation_category | trip_count |
|----------------------|------------------------|------------|
| 1 Ave & E 110 St     | No Rain                | 2,021      |
| 1 Ave & E 110 St     | Heavy Rain             | 68         |
| 1 Ave & E 118 St     | No Rain                | 1,589      |
| 1 Ave & E 118 St     | Heavy Rain             | 76         |
| 1 Ave & E 16 St      | No Rain                | 6,483      |
| 1 Ave & E 16 St      | Heavy Rain             | 261        |
| 1 Ave & E 18 St      | No Rain                | 6,218      |
| 1 Ave & E 18 St      | Heavy Rain             | 251        |
| 1 Ave & E 30 St      | No Rain                | 4,163      |
| 1 Ave & E 30 St      | Heavy Rain             | 109        |


**Do casual riders and annual members exhibit different ridership patterns in response to precipitation changes?**

| rider_type | precipitation_category | total_trips | avg_duration_minutes |
|------------|------------------------|-------------|----------------------|
| casual     | No Rain                | 773,275     | 18.9                 |
| casual     | Light Rain             | 202,193     | 17.0                 |
| casual     | Heavy Rain             | 20,317      | 15.7                 |
| member     | No Rain                | 3,218,330   | 11.9                 |
| member     | Light Rain             | 944,249     | 11.6                 |
| member     | Heavy Rain             | 114,345     | 10.8                 |


**What is the peak ridership hour, and how does this peak shift based on season and apparent temperature?**

**Peak ridership hour per season (current dataset includes only the last day of summer):**

| season | peak_hour | total_trips |
|--------|-----------|-------------|
| Fall   | 17        | 507,075     |
| Summer | 23        | 793         |


**Peak ridership hour by apparent temperature category:**

| apparent_temp | peak_hour | total_trips |
|---------------|-----------|-------------|
| Hot           | 18        | 63,984      |
| Mild          | 8         | 236,007     |
| Warm          | 17        | 452,201     |
