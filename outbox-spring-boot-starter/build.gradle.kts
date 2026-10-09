plugins {
    `java-library`
    `maven-publish`
}

dependencies {
    api(platform(libs.spring.boot.bom))
    api("org.springframework.boot:spring-boot-autoconfigure")
    api("org.springframework:spring-jdbc")
    api("org.springframework.kafka:spring-kafka")

    implementation("org.springframework.boot:spring-boot-starter-aop")
    compileOnly("io.micrometer:micrometer-tracing")

    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("io.micrometer:micrometer-core")
    implementation("org.flywaydb:flyway-core")
    runtimeOnly("org.flywaydb:flyway-database-postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.testcontainers:toxiproxy")
    testImplementation("org.awaitility:awaitility")
    testImplementation("io.micrometer:micrometer-tracing")
    testRuntimeOnly("org.postgresql:postgresql")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("starter") {
            from(components["java"])
        }
    }
}
