FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY common/src common/src
COPY order-service/pom.xml order-service/pom.xml
COPY payment-service/pom.xml payment-service/pom.xml
COPY fulfillment-service/pom.xml fulfillment-service/pom.xml
RUN mvn dependency:go-offline -pl order-service -am -q
COPY order-service/src order-service/src
RUN mvn package -pl order-service -am -DskipTests -q

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/order-service/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
