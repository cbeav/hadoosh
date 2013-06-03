#! /bin/bash
javac -g HadooSh.java -cp "lib/*"
jar cvf HadooSh.jar `ls *.class` lib/
