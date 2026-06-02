FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY common/src common/src
COPY payment-service/pom.xml payment-service/pom.xml
# Sibling pom must exist so Maven can read the reactor without order-service source
COPY order-service/pom.xml order-service/pom.xml
RUN mvn dependency:go-offline -pl payment-service -am -q
COPY payment-service/src payment-service/src
RUN mvn package -pl payment-service -am -DskipTests -q

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/payment-service/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
