# Data

**This folder contains only the processed, compressed (snappy) parquet files used in the analysis.** The original raw dataset and the working `Flight_Tab` CSV are **not** included in this repository. Use the instructions below to obtain them from the source.

## Original dataset

- **Name:** Multi-modal Flight Delay Dataset (MFDD / Aeolus), `Flight_Tab` tabular component
- **Source:** Kaggle, https://www.kaggle.com/datasets/flnny123/mfddmulti-modal-flight-delay-dataset
- **Licence / terms of use:** <licence from the Kaggle page, or "none stated; used for academic purposes only">
- **Contents:** US domestic flight records joined with origin and destination weather. The full dataset is also published with flight-chain and flight-network graph modalities; only the tabular `Flight_Tab` data is used here.
- **Collection period used:** 2016 to 2024
- **Last updated / published:** <date shown on the Kaggle page>

## Dataset sizes

| Stage | Size | In this repo? |
|---|---|---|
| Raw dataset (full original download) | ≈ 42 GB | No |
| Working dataset (`Flight_Tab` CSV) | ≈ 15 GB | No |
| Processing dataset (2016–2024 snappy parquet read by PySpark) | ≈ 2.4 GB on disk (compressed); larger once decompressed in Spark memory | Yes (compressed parquet only) |

## Folder layout

Only compressed parquet part files are stored here, one folder per year:

```
data/
├── README.md
├── flight_with_weather_2016/
│   ├── part-00000-....snappy.parquet
│   ├── ...
│   └── _SUCCESS
├── flight_with_weather_2017/
├── ...
└── flight_with_weather_2024/
```

No CSV or other raw files are stored in this folder. Spark reads all years as one DataFrame:

```python
paths = [f"data/flight_with_weather_{y}/" for y in range(2016, 2025)]
df_all = spark.read.parquet(*paths)
```

## How the processed data was produced

1. Download the dataset from Kaggle (for example with `kagglehub`):
```python
   import kagglehub
   path = kagglehub.dataset_download("flnny123/mfddmulti-modal-flight-delay-dataset")
```
2. Locate the `Flight_Tab` CSV in the downloaded files.
3. In Google Colab with PySpark, read the CSV and write it back out as snappy parquet, one folder per year:
```python
   df = spark.read.option("header", True).option("inferSchema", True).csv("<path to Flight_Tab csv>")
   df.write.option("compression", "snappy").parquet("flight_with_weather_<YEAR>/")  # run per year, filtering on FL_DATE
```
4. Place the resulting `flight_with_weather_<YEAR>/` folders in this `data/` directory.

## Schema (34 columns)

| Group | Columns |
|---|---|
| Flight identifiers | `FL_DATE`, `OP_CARRIER`, `OP_CARRIER_FL_NUM`, `ORIGIN`, `DEST`, `ORIGIN_INDEX`, `DEST_INDEX` |
| Scheduled / actual times | `CRS_DEP_TIME`, `DEP_TIME`, `WHEELS_OFF`, `WHEELS_ON`, `CRS_ARR_TIME`, `ARR_TIME` |
| Delays and durations | `DEP_DELAY`, `ARR_DELAY`, `TAXI_OUT`, `TAXI_IN`, `CRS_ELAPSED_TIME`, `ACTUAL_ELAPSED_TIME`, `AIR_TIME` |
| Calendar | `MONTH`, `DAY_OF_MONTH`, `DAY_OF_WEEK` |
| Origin weather | `O_TEMP`, `O_PRCP`, `O_WSPD` |
| Destination weather | `D_TEMP`, `D_PRCP`, `D_WSPD` |
| Airport coordinates | `O_LATITUDE`, `O_LONGITUDE`, `D_LATITUDE`, `D_LONGITUDE` |
| Other | `FLIGHTS` (constant 1, a row counter) |

## Data quality notes

- Only the weather columns contain missing values (about 0.16% of rows in 2016); all other columns were complete.
- Delay values are right-skewed, with extremes from about -204 to over 2,000 minutes.
- `FLIGHTS` is always 1 and is not used in the analysis.

## Privacy and licensing

The data contains no personally identifiable information. Only the compressed parquet files derived from the public Kaggle dataset are stored here; the original raw dataset and CSV are not redistributed. Refer to the Kaggle page for the original licence terms.
