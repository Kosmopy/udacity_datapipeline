# udacity_datapipeline


##Summary: In this project, I create an ETL Pipeline using Airflow to copy and transform data from S3 into AWS Redshift tables.

##Data: Song and log data are retrieved from the S3 bucket s3://udacity-dend.

##Schema: The tables are designed in a star schema with the following fact and dimension tables:

Fact table "songplays" with playid, start_time, user_id, level, songid, artistid, sessionid, location, user_agent.

Dimension tables
-"users" with userid, first_name, last_name, gender, level
-"songs" with songid, title, artist_id, year, duration
-"artists" with artistid, name, location, lattitude, longitude
-"time" with start_time, hour, day, week, month, year, weekday

##Requirements
The ETL Pipeline in this project is managed in Airflow. The following is necessary to set up Airflow:
1. Creation of IAM User in AWS
2. Creation of redshift cluster that is can be publicly accessed
3. Connection between Airflow and AWS by adding awsuser and redshift connections in Airflow
