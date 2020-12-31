## Data Lake Project
### Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, process them using Spark, and load the data back into S3 as a set of dimensional tables. This will allow the analytics team to continue finding insights in what songs their users are listening to.

### Project Description
In this project, I'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project,I will to load data from S3, process the data into analytics tables using Spark, and load them back into S3. I'll deploy this Spark process on a cluster using AWS.

### Project Files
*data* - where input and ouput(from running the etl on local spark) song and log data is located. 

*dl.cfg* - configuration file where to store AWS access key and secret key.

*etl.py* - python file to run the etl process consuming input data from S3 location and writing the transformed data back to S3 location. In order to run this script, you will need to set up your own S3 bucket and change the output_data to that address. 

*etl_local.py* - python file to run the etl process consuming input data from local zip data and writing the transformed data back to data/output-data directory.

*etl_with_local_data.ipynb* - Jupiter notebook where to run each step one by one and see the results for runing the etl process locally. 

*etl_with_S3.ipynb* - Jupyter notebook where to run each step one by one and see the results for runing the etl process on S3.

*README.md* - where information about the project and  steps are provided.



### ETL Pipeline
#### Read data from S3

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

The script reads song_data and load_data from S3.

#### Process data using spark

Transforms them to create five different tables listed below :

*Fact Table*

*songplays* - records in log data associated with song plays i.e. records with page NextSong

songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

*Dimension Tables*

*users* - users in the app Fields - user_id, first_name, last_name, gender, level

*songs* - songs in music database Fields - song_id, title, artist_id, year, duration

*artists* - artists in music database Fields - artist_id, name, location, lattitude, longitude

*time* - timestamps of records in songplays broken down into specific units 
*Fields* - start_time, hour, day, week, month, year, weekday

#### Load it back to S3

Writes them to partitioned parquet files in table directories on S3.# Data-Lake-on-S3
# Data-Lake-on-S3
