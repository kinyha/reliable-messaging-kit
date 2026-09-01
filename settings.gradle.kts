pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

rootProject.name = "reliable-messaging-kit"

include(
    "outbox-spring-boot-starter",
    "demo-stand:order-service",
    "demo-stand:payment-service",
)
