FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY common/src common/src
COPY order-service/pom.xml order-service/pom.xml
COPY payment-service/pom.xml payment-service/pom.xml
COPY fulfillment-service/pom.xml fulfillment-service/pom.xml
COPY notification-service/pom.xml notification-service/pom.xml
RUN mvn dependency:go-offline -pl notification-service -am -q
COPY notification-service/src notification-service/src
RUN mvn package -pl notification-service -am -DskipTests -q

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /app/notification-service/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
