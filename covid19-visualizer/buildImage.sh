VERSION=$1
mvn clean compile package
docker build -t covid19-visualizer:$VERSION .
docker tag covid19-visualizer:$VERSION xsreality/covid19-visualizer:$VERSION
docker push xsreality/covid19-visualizer:$VERSION
