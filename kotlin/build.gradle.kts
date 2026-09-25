plugins {
    kotlin("jvm") version "1.9.24"
    `java-library`
}

group = "io.dpmp"
version = "0.1.2"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
}

// 示例源码集（与主库分离，但参与编译以确保不腐烂）
sourceSets {
    create("examples") {
        kotlin.srcDir("examples")
        compileClasspath += sourceSets["main"].output
        runtimeClasspath += sourceSets["main"].output
    }
}

java {
    withSourcesJar()
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        jvmTarget = "17"
    }
}

tasks.test {
    useJUnitPlatform()
}

// build 时顺带编译示例
tasks.named("check") {
    dependsOn("compileExamplesKotlin")
}
