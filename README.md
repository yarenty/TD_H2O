# TalkingData Mobile User Demographics on H2O

Build a model predicting users’ demographic characteristics based on their app usage, geolocation, and mobile device properties. 

Doing so will help millions of developers and brand advertisers around the world pursue data-driven marketing efforts which are relevant to their users and catered to their preferences.


## Dependencies
Sparkling Water 1.6.5 which integrates:
  - Spark 1.6.1
  - H2O 3.0 Shannon
  - Data from keggle competition: https://www.kaggle.com/c/talkingdata-mobile-user-demographics/data
  - directiories in /opt/data/TalkingData/:
      - /input  - input csv files
      - /model  - model data build from input files
      - /output - and output submission file


## Status

- Step 1: Data preparation


## TODO
- Data munging - raw
- Machine Learning


## Project structure
 
```
├─ gradle/        - Gradle definition files
├─ src/           - Source code
│  ├─ main/       - Main implementation code 
│  │  ├─ scala/
│  ├─ test/       - Test code
│  │  ├─ scala/
├─ build.gradle   - Build file for this project
├─ gradlew        - Gradle wrapper 
```



## Project building

For building, please, use provided `gradlew` command:
```
./gradlew build
```

### Run
For running an application:
```
./gradlew run
```

## Running tests

To run tests, please, run:
```
./gradlew test
```

# Checking code style

To check codestyle:
```
./gradlew scalaStyle
```

## Creating and Running Spark Application

Create application assembly which can be directly submitted to Spark cluster:
```
./gradlew shadowJar
```
The command creates jar file `build/libs/td.jar` containing all necessary classes to run application on top of Spark cluster.

Submit application to Spark cluster (in this case, local cluster is used):
```
export MASTER='local-cluster[3,2,1024]'
$SPARK_HOME/bin/spark-submit --class com.yarenty.td.MLProcessor build/libs/ddi.jar
```




