Topic: Apache Spark
Architecture: Single Node Cluster
Programming Language: Java with Maven
Problem: Count the muliples of a given number (this is the same excersice presented for Hadoop and Pig, but using Spark)
Input: A txt file that contains a list of numbers
Output: The result of counting the multiples

Example:

Given a list of the numbers 2,5,8,10,13,15,16
and the target number 2
Then the application should return that the count of multiples of 2 are 4.
Because there are 4 multiples of 2 (2, 8, 10, 16)

An example of the list is also provided
The jar file to run is: multiple-of-0.0.1-SNAPSHOT.jar
The params needed are: jarfile numbersFile target

To run the program, just go to the spark/bin folder and type:
spark-submit --class multipleof.Main --master local[2] {yourPathToTheJarFile}\multiple-of-0.0.1-SNAPSHOT.jar {yourPathToTheNumbersFile}/numbers.txt 2

(where 2 is the target)

You should see the following output
	The count of multiples of 2 is: 304

Pre-requisites:
- Having hadoop, spark, java (jdk) and maven installed.

Example: 

C:\spark\bin>spark-submit --class multipleof.Main --master local[2] D:\ztmp\eclipse-workspace\multiple-of\target\multiple-of-0.0.1-SNAPSHOT.jar C:/numbers.txt 8

 ======= MULTIPLES OF 8 - SPARK IMPLEMENTATION =======

Reading file C:/numbers.txt ...
The file contains: 608 numbers

The count of multiples of 8 is: 76


