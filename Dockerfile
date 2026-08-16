FROM eclipse-temurin:17-jdk AS build
WORKDIR /usr/src/app
COPY gradle gradle
COPY gradle.properties gradle.properties
COPY gradlew gradlew
COPY settings.gradle settings.gradle
COPY build.gradle build.gradle
COPY api ./api
COPY clients/java ./clients/java
RUN ./gradlew --no-daemon :api:shadowJar

FROM eclipse-temurin:17-jre
WORKDIR /usr/src/app
COPY --from=build /usr/src/app/api/build/libs/marquez-*.jar /usr/src/app/marquez.jar
COPY LICENSE /usr/src/app/LICENSE
COPY marquez.dev.yml /usr/src/app/marquez.dev.yml
COPY docker/entrypoint.sh /usr/src/app/entrypoint.sh
EXPOSE 5000 5001
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
