# Distributed Weather Data Aggregation System

## Purpose
This project implements a distributed weather system with three main components:
1. `Aggregation Server`: Collections weather station data from Content servers, stores it in JSON files, and handles concurrent requests.
2. `Content Server`: Reads weather station data from a local file and pushes the entries to the Aggregations Server using PUT requests
3. `GET client`: Queries the Aggregation Server for weather data (all stations or a specific one) using GET requests.

All 3 are syncrhonized using lamport clocks using SYNC requests

## Project Structure
```commandline
C:.
│   pom.xml (dependencies)│
├───src
│   │   README.md
│   ├───main
│   │   ├───java
│   │   │   ├───AggregationServer  (Aggregation server entrypoint and connection handler)
│   │   │   │       AggregationServer.java
│   │   │   │
│   │   │   ├───ContentServer (Content server that sends weather data)
│   │   │   │       baseExample_doubleEntry.txt (Example files for testing)
│   │   │   │       baseExample_singleEntry.txt
│   │   │   │       ContentServer.java
│   │   │   │
│   │   │   ├───GETclient (Client for querying weather data)
│   │   │   │       GETClient.java
│   │   │   │
│   │   │   └───shared (resources shared by the 3 servers)
│   │   │           FileHandler.java
│   │   │           HTTPParser.java
│   │   │           HTTPRequest.java
│   │   │           HTTPResponse.java
│   │   │           IpPort.java
│   │   │           JSONParser.java
│   │   │           Retryable.java
│   │   │           RetryUtils.java
│   │   │           UrlManager.java
│   │   │
│   │   └───resources
│   └───test
│       └───java
│           │   ContentServerTest.java 
│           │   FileHandlerTest.java
│           │   GETClientTest.java
│           │   HTTPParserTest.java
│           │   IntegrationTest.java
│           │   UrlManagerTest.java
│           │
│           └───AggregationServer
│                   AggregationServerTest.java
```
## Dependencies / Requirements
- JAVA 11+ 
- JUNIT 5 for testing
- GSON for parsing

## Build tools

You can use either:
- IntelliJ idea (recommended)
- Maven for dependecies

## Running the servers

### Aggregation Server
Set the working directory to `src\main\java\AggregationServer`
Run using `java AggregationServer -p <port>`

### Content Server
Set the working directory to `src\main\java\ContentServer`
Run using ` java ContentServer -url {server url} -f {local weather filename}`

### GET Client
Set the working directory to `src\main\java\GETclient`
Run using `java GETclient -url {server url} [-sid {station ID}]`

## Running tests
Run all tests

### If using IntelliJ:

Right-click test/ folder → Run All Tests.

### If using Maven:

mvn test

## Notes

- Files are stored as {stationID}.json. Each contains:
  - Metadata (Lamport clock, update count, last host/port, timestamp).
  
  - Weather data body.

- Expired files (based on age or update count) are garbage-collected.

- Retry logic uses exponential backoff with jitter (see RetryUtils).

- Lamport clocks are incremented and synchronized on every request.

