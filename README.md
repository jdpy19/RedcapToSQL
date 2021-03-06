## Dependencies
```
[[source]]
name = "pypi"
url = "https://pypi.org/simple"
verify_ssl = true

[dev-packages]
pylint = "*"

[packages]
[packages]
requests = "*"
sqlalchemy = "*"
pandas = "*"
mysqlclient = "*"
pyodbc = "*"

[requires]
python_version = "3.7"
```

## Create a config.py 
Required fields include the api token, engine type and respective credentials.
```
ENGINE_TYPE = "mysql" | "sqlserver"
API_TOKEN = ""
```

Add the following fields for SQL Server using Windows credentials
```
DATABASE = ""
SERVER = ""
```

Add the following fields for MySQL Server
```
USER = ""
IP_ADDRESS ="" 
MYSQL_DATABASE =""
```

## Modify db.py for survey specific tables
Tables should be created by inheriting the class Base (Declarative_Base from SQLAlchemy), defining the table name and adding survey specific columns. Examples in code are Enrollment and Survey.

## Improvements
The tables are currently generated using the data labels from redcap, it would be more efficient to use the raw data and create a star schema with the labels.