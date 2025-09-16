plugins {
    id("java")
    id("java-library")
    alias(libs.plugins.springboot)
    alias(libs.plugins.depend)
    alias(libs.plugins.kt)
    alias(libs.plugins.ktSpring)
}


group = "com.liquid"
version = "0.1"


dependencies {
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

tasks.getByName<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = false
}