lsof -ti:1099 | xargs kill -9
make clean
make
rmiregistry -J-classpath -J.. 1099 &
