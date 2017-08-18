Topic: Spark Streaming
Architecture: Single Node Cluster
Programming Language: Java with Maven
Problem: Get the text of the tweets that were created using the #BigData hashtag (in real time)

To run the program:

(Only once)
1 - Download the source code
2 - Go to the folder where the pom.xml file is located
3 - Run the command: mvn clean install

(Everytime you want to run the program)
4 - Run the command: mvn exec:java -Dexec.mainClass="twitters.Main"

You will see in the console output the tweets that were created with the #BigData hashtag in real time.

Pre-requisites:
- Having maven and java (jdk) installed