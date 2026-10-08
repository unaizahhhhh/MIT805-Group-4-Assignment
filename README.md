# MIT 805 Big Data Project: Weather Impact on US Flight Delays

**Course:** MIT 805 Big Data, Semester Project 2026 (Part 2: PySpark / MapReduce)
**Group:** Group 4 (`<u23056534>`, `<u20426799>`)

## Overview

This project uses PySpark to analyse how precipitation affects flight delays across US domestic flights from 2016 to 2024. The processing is MapReduce-style: row-level transformation (map), key-based grouping (shuffle), and aggregation (reduce). It runs on Google Colab with PySpark's DataFrame and RDD APIs.

Flight delays are costly for airlines, airports and passengers, and weather is one of the main drivers. This project quantifies that effect and shows how it varies by season, carrier and route.

## Objectives

1. Quantify the difference in delay rates and times between dry and wet (precipitation) conditions.
2. Determine the influence of seasonality on delay rates.
3. Determine the effect of wet conditions on delays by carrier and route.
4. Identify the routes with the highest delay rates and times.

## Dataset

- **Name:** Multi-modal Flight Delay Dataset (MFDD / Aeolus), `Flight_Tab` tabular component
- **Source:** Kaggle, `flnny123/mfddmulti-modal-flight-delay-dataset`
- **License / terms:** Apache License 2.0 (as stated on the Kaggle dataset page)
- **Sizes:** raw dataset ≈ 42 GB; working dataset (`Flight_Tab` CSV) ≈ 15 GB; processing dataset (2016–2024 snappy parquet, ≈ 2.4 GB on disk, larger once decompressed in Spark memory)
- **Format used:** parquet, split into one folder per year (`data/flight_with_weather_<YEAR>/`)
- **Key fields:** flight date, carrier, origin/destination, departure/arrival delay, and origin/destination temperature, precipitation and wind speed

See `data/README.md` for instructions on obtaining the original data.

## Method

| Stage | MapReduce concept | Where |
|---|---|---|
| Cleaning and feature derivation (`DELAYED_15`, `SEVERE_DELAY_60`, `PRECIPITATION_CATEGORY`, `ROUTE`, `SEASON`) | Map / transformation | `src/transform.py` |
| `groupBy` on carrier, route, season, month, year | Shuffle / grouping | `src/aggregate.py` |
| `agg` (counts, averages, delay percentages, dry-vs-wet comparison) | Reduce / aggregation | `src/aggregate.py` |
| Per-carrier average delay via `map` and `reduceByKey` | Explicit low-level RDD MapReduce | `src/aggregate.py` |
| Execution plan and Spark UI / DAG evidence | Distributed execution evidence | `notebooks/`, `figures/` |

A flight counts as delayed if it arrives 15 or more minutes late. Precipitation is flagged when origin or destination precipitation is above zero.

## Repository Structure

```
project/
├── README.md
├── requirements.txt
├── data/            # yearly parquet folders + data/README.md (source and download instructions)
├── notebooks/       # Colab notebook(s) running the full pipeline
├── src/             # transform.py, aggregate.py
├── output/          # saved aggregation results
├── figures/         # charts and Spark execution screenshots
└── report/          # final PDF report
```

## Reproducing the Analysis

1. Open the notebook in `notebooks/` in Google Colab (`<notebook filename>`).
2. Install dependencies: `pip install -r requirements.txt` (PySpark, pandas, matplotlib, plotly).
3. Clone this repository so the data folder is available:
```python
   !git clone https://github.com/<username>/MIT805-Group-4-Assignment.git
```
4. Start a Spark session, then load all years as one DataFrame:
```python
   paths = [f"MIT805-Group-4-Assignment/data/flight_with_weather_{y}/" for y in range(2016, 2025)]
   df_all = spark.read.parquet(*paths)
```
5. Add `src/` to the path and run the pipeline:
```python
   import sys
   sys.path.append("MIT805-Group-4-Assignment/src")
   from transform import clean_and_derive
   from aggregate import *
   df_clean = clean_and_derive(df_all)
```
6. Run the remaining notebook cells to produce the aggregations, figures and execution plans.

Full-dataset actions (counts, grouped aggregations) can take several minutes on Colab's free tier.

## Limitations

- Results are descriptive associations, not causal effects. Traffic volume and seasonal congestion are not controlled for.
- Weather is recorded at airport level at a point in time, not along the full flight path.
- Carrier codes change over the study period (for example, Virgin America (VX) merged into Alaska in 2018), so some carriers have shorter observation windows.

## Data and Ethics

Only publicly available data is used, and it contains no personally identifiable information. The original dataset remains the property of its creators; refer to the Kaggle page for licence terms.

## Video Demonstration
[Watch the MIT805 project demonstration video](https://drive.google.com/file/d/1JT0lK4--a-IQPt5gG6cTWE_xyPbXExhK/view?usp=drive_link)
