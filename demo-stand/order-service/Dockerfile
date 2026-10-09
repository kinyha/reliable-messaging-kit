FROM eclipse-temurin:21-jdk-jammy@sha256:8878012b286ef00032346bfbdd55b10e9f5bf923430e3a85c7c3d6e6db6f4605 AS builder
WORKDIR /builder
COPY demo-stand/order-service/build/libs/order-service-0.1.0-SNAPSHOT.jar application.jar
RUN java -Djarmode=tools -jar application.jar extract --layers --destination extracted

FROM eclipse-temurin:21-jdk-jammy@sha256:8878012b286ef00032346bfbdd55b10e9f5bf923430e3a85c7c3d6e6db6f4605
ARG TARGETARCH
ARG PROFILER_VERSION=4.5
RUN apt-get update && apt-get install -y --no-install-recommends curl procps && rm -rf /var/lib/apt/lists/* \
    && case "$TARGETARCH" in arm64) arch=arm64;; amd64) arch=x64;; *) exit 1;; esac \
    && mkdir -p /opt/async-profiler \
    && curl -fsSL "https://github.com/async-profiler/async-profiler/releases/download/v${PROFILER_VERSION}/async-profiler-${PROFILER_VERSION}-linux-${arch}.tar.gz" \
       | tar -xz --strip-components=1 -C /opt/async-profiler
WORKDIR /application
COPY --from=builder /builder/extracted/dependencies/ ./
COPY --from=builder /builder/extracted/spring-boot-loader/ ./
COPY --from=builder /builder/extracted/snapshot-dependencies/ ./
COPY --from=builder /builder/extracted/application/ ./
ENTRYPOINT ["java", "-jar", "application.jar"]
