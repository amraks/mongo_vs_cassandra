/***************************************************************
README:
Project: B669 Project 1
Group Members: Harsh Savla (hsavla@indiana.edu), Sandeep Taduri
***************************************************************/

1. Project requires Maven.
2. Unzip the .zip file.
3. Find the folder b669_p1
4. Import the project into eclipse as a maven project.
5. To build: mvn clean install
6. Run as normal java application.

For Cassandra:
1. There are 2 super column families and 1 standard column family which need to be created through CLI before inserting data into cassandra. 
   The schema for these has been described in the report.

2. The cluster name, column family names, configuration will need to be same as defined in the CassandraHelper class.

For MongoDB:
1. Although not require, it is advisable to create the collections defined in MongoDBHelper class before inserting the data.