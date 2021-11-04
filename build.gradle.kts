plugins {
  `maven-publish`
  java
  signing
  id("io.github.gradle-nexus.publish-plugin") version "1.0.0"
}

repositories {
  mavenCentral()
  mavenLocal()
}

subprojects {
  apply(plugin = "java")
  apply(plugin = "signing")
  apply(plugin = "maven-publish")
}

allprojects {
  java.sourceCompatibility = JavaVersion.VERSION_11
  java.targetCompatibility = JavaVersion.VERSION_11

  repositories {
    mavenCentral()
    mavenLocal()
  }

  dependencies {
    val flinkVersion = "1.12.4"
    implementation("com.google.code.gson:gson:2.8.6")
    implementation("com.google.guava:guava:30.1-jre")

    testImplementation(platform("org.junit:junit-bom:5.7.1"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testImplementation("org.mockito:mockito-junit-jupiter:3.7.7")
    testImplementation("net.jqwik:jqwik:1.3.10")
    testImplementation("com.h2database:h2:1.4.200")
    testImplementation("org.apache.flink:flink-table-planner-blink_2.11:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients_2.11:$flinkVersion")

    implementation("org.apache.flink:flink-table-planner-blink_2.11:$flinkVersion")

    // Specialize log4j
    implementation("org.apache.logging.log4j:log4j-core:2.12.1")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.12.1")
  }

  tasks.test {
    useJUnitPlatform()
  }
}

val ossrhUsername: String by project
val ossrhPassword: String by project

nexusPublishing {
  repositories {
    sonatype {
      nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
      snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
      username.set(ossrhUsername)
      password.set(ossrhPassword)
    }
  }
}
