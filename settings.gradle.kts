pluginManagement {
    repositories {
        maven {
            name = "TencentPublic"
            url = uri("https://mirrors.cloud.tencent.com/nexus/repository/maven-public")
        }
        maven {
            name = "AliyunPlugin"
            url = uri("https://maven.aliyun.com/repository/gradle-plugin")
        }
        gradlePluginPortal()
        mavenCentral()
        mavenLocal()
    }
}
plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

dependencyResolutionManagement {
    @Suppress("UnstableApiUsage")
    repositories {
        maven {
            name = "TencentPublic"
            url = uri("https://mirrors.cloud.tencent.com/nexus/repository/maven-public")
        }
        maven {
            name = "AliyunPublic"
            url = uri("https://maven.aliyun.com/repository/public")
        }

        mavenCentral()
        maven { url = uri("https://repo.spring.io/milestone") }
        maven { url = uri("https://repo.spring.io/snapshot") }
    }
}
rootProject.name = "tus-bridge-kt"
