# Automated Tests

## Pre-reqs

The following is required in order to run the automated tests:

 1. Gradle must be installed on the system in order to run the automated tests. 
 See [Installing Gradle](https://docs.gradle.org/current/userguide/installation.html).
 2. A local Redis server must be installed and running
 
## Setup

 1. Clone the streamsx.dps respository
 2. Modify the ```junit.properties``` file to point to the address of 
 your Redis server as well as the path to the DPS toolkit.
 
## Running the Tests

1. Run the following command from the streamsx.dps/test directory:
  
  ``` gradle cleanTest build ```
