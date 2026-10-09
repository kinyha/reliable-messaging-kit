FROM eclipse-temurin:21-jdk-jammy@sha256:8878012b286ef00032346bfbdd55b10e9f5bf923430e3a85c7c3d6e6db6f4605 AS builder
WORKDIR /builder
COPY demo-stand/payment-service/build/libs/payment-service-0.1.0-SNAPSHOT.jar application.jar
RUN java -Djarmode=tools -jar application.jar extract --layers --destination extracted

FROM eclipse-temurin:21-jdk-jammy@sha256:8878012b286ef00032346bfbdd55b10e9f5bf923430e3a85c7c3d6e6db6f4605
WORKDIR /application
COPY --from=builder /builder/extracted/dependencies/ ./
COPY --from=builder /builder/extracted/spring-boot-loader/ ./
COPY --from=builder /builder/extracted/snapshot-dependencies/ ./
COPY --from=builder /builder/extracted/application/ ./
ENTRYPOINT ["java", "-jar", "application.jar"]
