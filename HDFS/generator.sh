#!/bin/bash
# written by Maximiliano Agustin Mascheroni

# Verifying that Hadoop is running
echo $'\n'"===== Starting HDFS Numbers Generator ====="
echo "==========================================="$'\n'
jps="$(jps)"
if [[ ${jps} == *" NameNode"* ]] && [[ ${jps} == *"JobTracker"* ]] && 
[[ ${jps} == *"DataNode"* ]] && [[ ${jps} == *"TaskTracker"* ]] &&
[[ ${jps} == *"SecondaryNameNode"* ]]; then
    echo "INFO: Hadoop Daemons are running properly"$'\n'"Running Script...."$'\n'
else
    echo "ERROR: Hadoop Daemons are not running"$'\n'"Stopping Script...."$'\n'
    exit
fi

# Verifying that partial numbers file does not exist in the local system
nls="$(ls)"
if [[ ${nls} == *"numbers"* ]]; then
    echo "ERROR: numbers file already exist in your local system"$'\n'"Stopping Script...."$'\n'
    exit
fi

# Verifying that numbersInput file does not exist in your HDFS
ls="$(hadoop fs -ls)"
if [[ ${ls} == *"numbersInput"* ]]; then
    echo "ERROR: numbersInput file already exist in HDFS"$'\n'"Stopping Script...."$'\n'
    exit
fi

# Creating 700 random numbers
echo "Creating random numbers...."
fileInMemory="1"
for i in {1..700}; do
    line=$(( $RANDOM % 650 ))
    fileInMemory="${fileInMemory}"$'\n'"${line}"
done
echo "${fileInMemory}" >> numbers

# Creating a numbersInput file with the generated numbers in HDFS
echo $'\n'"Creating numbersInput file in your HDFS"
hadoop fs -copyFromLocal numbers numbersInput

# Verifying that numbersInput file was created successfully in HDFS
newls="$(hadoop fs -ls)"
if [[ ${newls} == *"numbersInput"* ]]; then
    echo "SUCCESS: numbersInput file was generated successfully"$'\n'"Run 'hadoop fs -ls' to see it"$'\n'
else
    echo "ERROR: numbersInput was not genereated in your HDFS"$'\n'
    echo "Removing temporal numbers file...."$'\n'"Stopping Script...."$'\n'
    exit
fi

#Cleaning up
rm numbers
