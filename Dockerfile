FROM openjdk:11
LABEL maintainer="Alexandre Franca"
ADD build/libs/library-consumer-0.0.1-SNAPSHOT.jar library-events-consumer.jar
ENTRYPOINT ["java", "-jar", "library-events-consumer.jar"]