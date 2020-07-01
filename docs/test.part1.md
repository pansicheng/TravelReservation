Note
---
```sh
cd src/lockmgr
make
cd src/transaction
make clean
make
rmiregistry -J-classpath -J.. 1099 &
cd src/test.part1
export CLASSPATH=.:gnujaxp.jar
/usr/bin/javac RunTests.java
java -DrmiPort=1099 RunTests MASTER.xml

lsof -i:1099
```

[CS223 Project 1](https://www.ics.uci.edu/~cs223/projects/projects1.html)
---
```sh
# Download the tarball.
# Put the tarball in a directory you've created for this class, say, ~/cs223
cd ~/cs223
gunzip project1.tar.gz
tar xvf project1.tar
cd project/lockmgr
make
copy your submissions directory to ./submissions
cd ../transaction
# Think of a random number between 2000 and 5000, say, 2100, and change the
# value of RMIREGPORT in first line of Makefile from 1099 to this number. This
# is the port number where your rmiresgistry will be listening. Giving it a
# random number will ensure that your rmiresgistry doesn't conflict with
# somebody else's.
cd ../test.part1
cp -r ../../submissions/* ../transaction
rm -r ../transaction/Client.java
cp -f Client.java ../transaction/
cd ../transaction
make clean
make server
make client
rmiregistry -J-classpath -J.. 2100 &
cd ../test.part1
setenv CLASSPATH .:gnujaxp.jar
/usr/bin/javac RunTests.java
java -DrmiPort=2100 RunTests MASTER.xml
```
