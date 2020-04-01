VERSION=$1
mvn clean compile package
docker build -t covid19-patient-analyzer:$VERSION .
docker tag covid19-patient-analyzer:$VERSION xsreality/covid19-patient-analyzer:$VERSION
docker push xsreality/covid19-patient-analyzer:$VERSION
