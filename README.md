# ETL pipeline to Collect,transform and load data to a Postgres Docker Container

in this project ,I extracted data from Avito cars website using Selenium web driver , performed the processing in Python to clean and transform data ,and finally load data to a Postgres Docker container using airflow work
flow manager.

## Prerequisites

* Python version 3.*
* Selenium web driver
* Pandas
* numpy
* psycopg2
* Apache Airflow
* Docker

## installation and preparation:
### running airflow
open terminal go to airflow directory then run:
![image](https://user-images.githubusercontent.com/89319105/216825333-69f71a47-dbdc-4327-8ee9-7482dbf7d808.png)
then go to localhost:8080
you will get this window:
![image](https://user-images.githubusercontent.com/89319105/216825422-618755e5-918b-4f34-bc13-7f2fc9f73767.png)
### running Postgres container
go to postgres directory and type this command:
```
**docker-compose up**
```
then go to localhost:8081
![image](https://user-images.githubusercontent.com/89319105/216825899-01011ba8-4d9a-4fc9-a3d1-7ad7ec46329b.png)
![image](https://user-images.githubusercontent.com/89319105/216825960-72128a89-5e20-4065-a4ef-a8d9ff3a8125.png)

####pipeline deployement 
![image](https://user-images.githubusercontent.com/89319105/216826433-b46d9a91-09f6-427a-b797-f93144327fa5.png)

