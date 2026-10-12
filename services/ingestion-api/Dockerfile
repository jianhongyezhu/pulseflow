FROM maven:3.9.9-eclipse-temurin-21 AS build

WORKDIR /workspace

COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:21-jre-alpine AS runtime

WORKDIR /app

RUN addgroup -S -g 10001 pulseflow \
    && adduser -S -D -H -u 10001 -G pulseflow pulseflow

COPY --from=build --chown=10001:10001 \
    /workspace/target/pulseflow-ingestion-0.0.1-SNAPSHOT.jar \
    /app/pulseflow-ingestion.jar

USER 10001:10001

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/pulseflow-ingestion.jar"]
