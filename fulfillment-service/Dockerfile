FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY common/src common/src
COPY fulfillment-service/pom.xml fulfillment-service/pom.xml
COPY order-service/pom.xml order-service/pom.xml
COPY payment-service/pom.xml payment-service/pom.xml
RUN mvn dependency:go-offline -pl fulfillment-service -am -q
COPY fulfillment-service/src fulfillment-service/src
RUN mvn package -pl fulfillment-service -am -DskipTests -q

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /app/fulfillment-service/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
