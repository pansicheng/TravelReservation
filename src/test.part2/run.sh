rm -r data/* 
rm -r results/*

export CLASSPATH=.:gnujaxp.jar
javac RunTests.java
java -DrmiPort=1099 RunTests MASTER.xml
