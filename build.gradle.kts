plugins {
  `maven-publish`
  java
  signing
  id("io.github.gradle-nexus.publish-plugin") version "1.0.0"
  id("pmd")
  id("com.github.spotbugs") version "5.0.6"
}

repositories {
  mavenCentral()
  mavenLocal()
}

subprojects {
  apply(plugin = "java")
  apply(plugin = "signing")
  apply(plugin = "maven-publish")
//  apply(plugin = "pmd")
//  apply(plugin = "com.github.spotbugs")
}

allprojects {
  java.sourceCompatibility = JavaVersion.VERSION_11
  java.targetCompatibility = JavaVersion.VERSION_11

  repositories {
    mavenCentral()
    mavenLocal()
  }

  dependencies {
    val flinkVersion = "1.12.7"
    val scalaVersion = "2.11"
    implementation("com.google.code.gson:gson:2.8.6")
    implementation("com.google.guava:guava:30.1-jre")

    testImplementation(platform("org.junit:junit-bom:5.7.1"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testImplementation("org.mockito:mockito-junit-jupiter:3.7.7")
    testImplementation("net.jqwik:jqwik:1.3.10")
    testImplementation("com.h2database:h2:1.4.200")
    testImplementation("org.apache.flink:flink-table-planner-blink_$scalaVersion:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients_$scalaVersion:$flinkVersion")

    compileOnly("org.apache.flink:flink-table-planner-blink_$scalaVersion:$flinkVersion")

    // Specialize log4j
    implementation("org.apache.logging.log4j:log4j-core:2.12.1")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.12.1")
  }

  tasks.test {
    useJUnitPlatform()
  }
}

var ossrhUsername = System.getenv("OSSRH_USERNAME")
if (ossrhUsername == null && project.hasProperty("ossrhUsername")) {
  ossrhUsername = project.property("ossrhUsername").toString()
}
var ossrhPassword = System.getenv("OSSRH_PASSWORD")
if (ossrhPassword == null && project.hasProperty("ossrhPassword")) {
  ossrhPassword = project.property("ossrhPassword").toString()
}

nexusPublishing {
  repositories {
    sonatype {
      nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
      snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
      username.set(ossrhUsername.toString())
      password.set(ossrhPassword.toString())
    }
  }
}
