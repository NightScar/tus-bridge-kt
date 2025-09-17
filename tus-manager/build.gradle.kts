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
    api(libs.bundles.coroutines)
    api(libs.bundles.exposedLibs)
    api(libs.bundles.arrowLibs)
    api(libs.klog)
    implementation("org.springframework.boot:spring-boot-starter-web")
    api("com.fasterxml.jackson.module:jackson-module-kotlin")
    api("com.fasterxml.jackson.module:jackson-module-jsonSchema-jakarta")
    // minio
    implementation(libs.minio)

    testCompileOnly("org.junit.jupiter:junit-jupiter")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("com.h2database:h2")
    testImplementation(kotlin("test"))
    testImplementation(libs.coroutinesTest)
}

tasks.getByName<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = false
}
tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}