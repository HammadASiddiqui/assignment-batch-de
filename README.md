# Batch ETL SOLUTION
batch ETL approach for the solution.
## Requirements 
1. Install docker

## STEPS TO RUN
1. clone the project
2. open terminal
3. run the follwing command in the cloned folder directory
```
docker-compose up
```

## About the solution
The docker-compose command basically lays the whole docker based infrastructure required for the program to be executed:

For this approach the following components are created by the ```docker-compose```:

1. Python based docker image
2. Postgres DB

### Explanation 
1. First, the docker spawns the containers for the python based and postgres DB (also creating the data tables required for the ETL). 
2. Then the python script gets executed, it invokes the twitter API and extracts the data from the get request.
3. The batch is configured to get the weekly extract of tweets starting from the scheduled time.
4. Then the extracted data after application of some transformations gets ingested in the relevant PostgresDB's Tables. 
5. This completes the ETL.

### How to query the DB for viewing ETL results

Execute the following commands on the terminal

```docker container ps```
view the DB containers CONTAINER ID and copy it 

```docker exec -it <copied CONTAINER ID> bash  ```
this will route your terminal to the postgres DB instance

```psql postgres://username:secret@localhost:5432/database```
this command will connect to your Database

## Design Diagram

Please find attached the design diagram for the solution.

![batch_de](https://user-images.githubusercontent.com/15999137/180247530-f3dc4a6b-5904-472c-8f19-32624d2a27b8.jpeg)


