find src/ -name *.java > source.txt

if [ ! -d "bin" ]
then
mkdir bin/
fi

javac @source.txt -cp lib/hibernate-memcached.jar:lib/org.springframework.beans-3.1.1.RELEASE.jar:lib/slf4j-api-1.6.4.jar:. -d bin/

echo "jar cvf xmemcached.jar *" > bin/makejar.sh
chmod +x bin/makejar.sh
