FROM eclipse-temurin:23-jre-alpine

WORKDIR /app

# Copy the Spring Boot fat JAR built by Maven
COPY target/DataMigrationFramework-*.jar app.jar

# Actuator port (9390) and optional JMX debug port
EXPOSE 9390

ENTRYPOINT ["java", "-jar", "app.jar"]
