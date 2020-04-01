VERSION=$1
mvn clean compile package
docker build -t covid19-telegram-app:$VERSION .
docker tag covid19-telegram-app:$VERSION xsreality/covid19-telegram-app:$VERSION
docker push xsreality/covid19-telegram-app:$VERSION
