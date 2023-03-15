/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java library project to get you started.
 * For more details take a look at the 'Building Java & JVM projects' chapter in the Gradle
 * User Manual available at https://docs.gradle.org/6.7.1/userguide/building_java_projects.html
 */

plugins {
  java
}

repositories {
  mavenLocal()
  mavenCentral()
}
description = "core"

tasks.jar {
  from(sourceSets["test"].output)
}

val instrumentedJars by configurations.creating {
  isCanBeConsumed = true
  isCanBeResolved = false
}

java {
  withSourcesJar()
  withJavadocJar()
}

//pmd {
//  ruleSetFiles = project.files("lint/pmd-rules.xml")
//  ruleSets = emptyList()
//}

dependencies {
  val flinkVersion = "1.15.3"
  val sparkVersion = "3.3.0"
  implementation("org.apache.flink", "flink-avro", flinkVersion)
  implementation("org.apache.flink", "flink-connector-kafka", flinkVersion)
  testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion:tests")
  implementation("org.apache.flink", "flink-runtime", flinkVersion)
  testImplementation("org.apache.flink", "flink-test-utils", flinkVersion)
  implementation("com.google.cloud", "google-cloud-storage", "2.11.3")
  implementation("org.apache.spark:spark-core_2.12:${sparkVersion}")
  implementation("org.apache.spark:spark-sql_2.12:${sparkVersion}")
}

tasks.test {
  useJUnitPlatform()
}

publishing {
  publications {
    create<MavenPublication>("mavenJava") {
      groupId = project.group.toString()
      artifactId = project.name
      version = project.version.toString()
      from(components["java"])
      val gpgKeyId = System.getenv("GPG_KEY_ID")
      val gpgPassword = System.getenv("GPG_PASSWORD")
      val gpgSecret = System.getenv("GPG_SECRET")

      pom {
        name.set(project.name)
        description.set("A stream library for batch and realtime data processing")
        url.set("https://github.com/airwallex/AirSkiff")

        // either read signing credentials from environment variable or from the file
        if (gpgSecret != null || project.hasProperty("signing.keyId")) {
          signing {
            if (gpgSecret != null) {
              useInMemoryPgpKeys(gpgKeyId.toString(), gpgSecret.toString(), gpgPassword.toString())
            }
            sign(publishing.publications)
            sign(configurations.archives.get())
          }
        }

        licenses {
          license {
            name.set("The MIT License")
            url.set("https://opensource.org/licenses/MIT")
          }
        }
        developers {
          developer {
            id.set("tzhu-airwallex")
            name.set("Tianshi Zhu")
            email.set("tzhu@airwallex.com")
          }
        }
        scm {
          connection.set("scm:svn:https://github.com/airwallex/AirSkiff.git")
          developerConnection.set("scm:svn:https://github.com/airwallex/AirSkiff.git")
          url.set("https://github.com/airwallex/AirSkiff.git")
        }
      }
    }
  }
}
