FROM openjdk:8-jre-slim
COPY ./target/covid19-visualizer-1.0-SNAPSHOT.jar /usr/app/
WORKDIR /usr/app
ENTRYPOINT ["java", "-jar", "covid19-visualizer-1.0-SNAPSHOT.jar"]