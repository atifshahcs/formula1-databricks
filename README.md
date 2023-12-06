# formula1-databricks
Formula1 projects consist of Formula 1 racing data from the 1950s till date, the dataset includes data about drivers, circuits, races,  countries, and races won by the drivers. The Formula1 data is collected from [Ergast API](https://ergast.com/mrd/db/)

This project includes the data bricks notebooks and consists of the following folder.
## setup
- This folder consists of the notebooks that are used to connect the Azure data lake and extract the data in the form of CSV, JSON, and parquet.
- This also includes mount and unmount data.
## demo
- This folder consists of some demos for, filtering, joins, and aggregations using Spark.

## include
- The Include folder consists of notebooks that are required to include other basic functionality to other notebooks
  
## ingestion
- Include notebook read/write data from/to Azure data lake in the form of CSV, JSON, and  parquet.

## transformation
- The notebooks extract, transform, load
  - The data is first extracted from Azure Data Lake.
  - The transformation is done via Spark, which includes joins, filtering, and aggregation.
  - The transformed data is loaded to Azure Azure Data Lake.

