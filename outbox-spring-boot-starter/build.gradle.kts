plugins {
    `java-library`
}

dependencies {
    api(platform(libs.spring.boot.bom))
    api("org.springframework.boot:spring-boot-autoconfigure")
    api("org.springframework:spring-jdbc")
    api("org.springframework.kafka:spring-kafka")

    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("io.micrometer:micrometer-core")
    implementation("org.flywaydb:flyway-core")
    runtimeOnly("org.flywaydb:flyway-database-postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
    withSourcesJar()
}
