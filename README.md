An attempt to reimplement   Hortonworks' popular Spark tutorial on IOT data from HVAC devices,
using Apache Flink's **DataSet**.

Currently it uses

* JDK 1.7.0_79

* Scala 2.11.7

* Apache Flink 1.0


The input data are made available through two data files, namely

* ./SensorFiles/building.csv

* ./SensorFiles/HVAC.csv


These files are downloaded from the link given at the tutorial's site: 
http://s3.amazonaws.com/hw-sandbox/tutorial14/SensorFiles.zip

The tutorial makes use of HIVE scripts as well as Microsoft Excel's Power feature, all
running within Hortonwork's sandbox.

Here, I am using Apache Flink's local mode of running, there by creating all output files 
in the local filesystem (EXT4, on my Ubuntu laptop). 

To prepare the World Geographical Map view, I have used Google Spreadsheet's *Insert->Chart->Map* feature.

**TBD**

Hortonwork's tutorial also demonstrates use of Apache Zeppelin to create visualization. I am yet to 
add that to this project.

All comments and critiques are welcome.