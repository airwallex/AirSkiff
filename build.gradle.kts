plugins {
  `maven-publish`
  java
  signing
  id("io.github.gradle-nexus.publish-plugin") version "1.0.0"
//  id("pmd")
//  id("com.github.spotbugs") version "5.0.6"
}

repositories {
  mavenCentral()
  mavenLocal()
}

subprojects {
  apply(plugin = "java")
  apply(plugin = "signing")
  apply(plugin = "maven-publish")

  // Configure publishing for all subprojects
  configure<PublishingExtension> {
    repositories {
      maven {
        name = "OSSRH"
        url = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
        credentials {
          username = System.getenv("OSSRH_USER_TOKEN_USERNAME") ?: findProperty("ossrhUsername")?.toString() ?: ""
          password = System.getenv("OSSRH_USER_TOKEN_PASSWORD") ?: findProperty("ossrhPassword")?.toString() ?: ""
        }
      }
    }
  }
}

allprojects {
  java.sourceCompatibility = JavaVersion.VERSION_11
  java.targetCompatibility = JavaVersion.VERSION_11

  repositories {
    mavenCentral()
    mavenLocal()
  }

  dependencies {
    val flinkVersion = "1.15.3"
    implementation("com.google.code.gson", "gson", "2.9.0")
    implementation("com.google.guava", "guava", "31.1-jre")

    testImplementation(platform("org.junit:junit-bom:5.8.2"))
    testImplementation("org.junit.jupiter", "junit-jupiter", "5.8.2")
    testImplementation("org.mockito", "mockito-junit-jupiter", "4.6.0")
    testImplementation("net.jqwik", "jqwik", "1.6.5")
    testImplementation("com.h2database", "h2", "2.1.212")
    testImplementation("org.apache.flink", "flink-table-planner_2.12", flinkVersion)
    testImplementation("org.apache.flink", "flink-clients", flinkVersion)

    compileOnly("org.apache.flink", "flink-table-planner_2.12", flinkVersion)

    // Specialize log4j
//    implementation("org.apache.logging.log4j", "log4j-core", "2.17.2")
//    implementation("org.apache.logging.log4j", "log4j-slf4j-impl", "2.17.2")
  }

  tasks.test {
    useJUnitPlatform()
  }
}

// Configure Nexus publishing with token-based authentication
nexusPublishing {
  repositories {
    sonatype {
      nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
      snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
      username.set(System.getenv("OSSRH_USER_TOKEN_USERNAME") ?: findProperty("ossrhUsername")?.toString() ?: "")
      password.set(System.getenv("OSSRH_USER_TOKEN_PASSWORD") ?: findProperty("ossrhPassword")?.toString() ?: "")
    }
  }
}

// Token-based publishing credentials for root project
publishing {
    repositories {
        maven {
            name = "OSSRH"
            url = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
            credentials {
                username = System.getenv("OSSRH_USER_TOKEN_USERNAME") ?: findProperty("ossrhUsername")?.toString() ?: ""
                password = System.getenv("OSSRH_USER_TOKEN_PASSWORD") ?: findProperty("ossrhPassword")?.toString() ?: ""
            }
        }
    }
}
