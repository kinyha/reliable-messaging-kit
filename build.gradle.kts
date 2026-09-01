plugins {
    alias(libs.plugins.spring.boot) apply false
}

allprojects {
    group = "dev.reliablemessaging"
    version = "0.1.0-SNAPSHOT"
}

subprojects {
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }
}
