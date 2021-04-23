# DatabaseAccess

Can be used to log invocations in MongoDB and update a MariaDB based on the logs.

To use it, simply build and include the `databaseAccess.jar` file in your project.

## Build

````
gradle shadowJar
````

## mongoDatabase.properties

The file `mongoDatabase.properties` has to be placed in the root folder of the enactment-engine.

The structure is as follows (with example values):
````
host=10.0.0.62
port=27017
database=AFCL
collection=logs
username=user
password=pw
````

## mariaDatabase.properties

The file `mariaDatabase.properties` has to be placed in the root folder of the enactment-engine.

The structure is as follows (with example values):
````
host=10.0.0.62
port=3307
database=afcl
username=user
password=pw
````
 
