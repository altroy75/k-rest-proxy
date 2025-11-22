FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM registry.access.redhat.com/ubi9/openjdk-21
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
COPY src/main/resources/keystore.p12 /app/keystore.p12
ENTRYPOINT ["java", "-jar", "app.jar"]
