VERSION=$1
mvn clean compile package
docker build -t covid19-stats:$VERSION .
docker tag covid19-stats:$VERSION xsreality/covid19-stats:$VERSION
docker push xsreality/covid19-stats:$VERSION
