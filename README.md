# Create a config.py 
Add the following fields for SQL Server using Windows credentials
* DATABASE
* SERVER

Add the following fields for MySQL Server
* USER
* PASSWORD
* IP_ADDRESS
* MYSQL_DATABASE

# Improvements
The tables are currently generated using the data labels from redcap, it would be more efficient to use the raw data and create a star schema with the labels.