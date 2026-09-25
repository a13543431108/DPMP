plugins {
    kotlin("jvm") version "1.9.24"
    `java-library`
    `maven-publish`
    signing
}

group = "io.github.a13543431108"
version = "0.1.3"

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
    withJavadocJar()
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

tasks.named("check") {
    dependsOn("compileExamplesKotlin")
}

// Dokka 未引入，用 sourcesJar 占位以满足 Maven Central 要求
tasks.named<Jar>("javadocJar") {
    from(tasks.named("sourcesJar").map { (it as Jar).archiveFile })
}

// ---- 签名凭据（从 gitignore 的 secret.properties 读取） ----
val secretProps: Map<String, String> = run {
    val f = rootProject.file("secret.properties")
    if (!f.exists()) emptyMap()
    else f.readLines()
        .filter { it.contains("=") && !it.trim().startsWith("#") }
        .associate {
            val i = it.indexOf('=')
            it.substring(0, i).trim() to it.substring(i + 1).trim()
        }
}
val signingKeyArmored: String? = secretProps["signingKeyFile"]
    ?.let { runCatching { rootProject.file(it).readText() }.getOrNull() }
val signingPassword: String? = secretProps["signingPassword"]

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("DPMP")
                description.set("Dual-Punch Multi-Path Protocol —— P2P 连接与传输底层库（Kotlin/JVM 实现，与 Python 端协议逐字节一致）")
                url.set("https://github.com/a13543431108/DPMP")
                licenses {
                    license {
                        name.set("Apache License 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                developers {
                    developer {
                        id.set("a13543431108")
                        name.set("a13543431108")
                        email.set("a1354343110804@outlook.com")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/a13543431108/DPMP.git")
                    developerConnection.set("scm:git:https://github.com/a13543431108/DPMP.git")
                    url.set("https://github.com/a13543431108/DPMP")
                }
            }
        }
    }
    repositories {
        // 本地暂存目录，之后打包成 bundle 上传到 Central Portal
        maven {
            name = "staging"
            url = uri(layout.buildDirectory.dir("staging-deploy"))
        }
    }
}

signing {
    if (signingKeyArmored != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKeyArmored, signingPassword)
        sign(publishing.publications["maven"])
    }
}
