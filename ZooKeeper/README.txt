Topic: Zookeeper
Programming Language: Java with Maven
Problem: Implement three distributed co-ordination protocols using Zookeeper:
	1 - Create a ZNode
	2 - Update the data of a ZNode
	3 - Delete a ZNode

These simple protocols were coded in Java, and they can be found in the source code folder of this excersice
Also a main class which implements the protocols can be found. Its name's ProtocolsExcersiceMain

To run the program:

(Only once)
1 - Download the source code
2 - Go to the folder where the pom.xml file is located
3 - Run the command: mvn clean install

(Everytime you want to run the program)
4 - Run the command: mvn exec:java -Dexec.mainClass="com.bigdata.ProtocolsExcersiceMain"

You will see in the console the three protocols running

Pre-requisites:
- Having zookeeper installed and running
- Having maven and java (jdk) installed