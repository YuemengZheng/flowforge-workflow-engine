# Multi-stage: Maven resolves and builds, the runtime image carries only a JRE
# and the jar. A build image would triple the size for nothing.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

# Dependencies first, so editing a source file does not re-resolve the world.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app

# curl only for the container healthcheck: the JRE image has no HTTP client, and
# a healthcheck that cannot reach the app is not a healthcheck.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10002 flowforge
COPY --from=build /build/target/history-*.jar app.jar
USER flowforge

EXPOSE 8090
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
