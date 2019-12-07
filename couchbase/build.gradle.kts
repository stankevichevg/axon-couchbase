import java.math.BigDecimal.valueOf

plugins {
    java
    eclipse
    idea
    id("io.freefair.lombok") version Versions.lombok
    jacoco
}

dependencies {
    compile("org.axonframework", "axon-eventsourcing", Versions.axon)
    compile("com.couchbase.client", "java-client", Versions.couchbaseClient)
    compile("org.apache.commons", "commons-text", "1.8")
    compile("com.fasterxml.jackson.core", "jackson-annotations", Versions.jackson)

    testImplementation("org.axonframework", "axon-eventsourcing", Versions.axon, classifier = "tests")
    testImplementation("com.fasterxml.jackson.core", "jackson-databind", Versions.jackson)
    testImplementation("com.fasterxml.jackson.datatype", "jackson-datatype-jsr310", Versions.jackson)
    testImplementation("org.springframework", "spring-tx", Versions.spring)
    testImplementation("org.mockito", "mockito-core", Versions.mockito)

    testImplementation("org.assertj", "assertj-core", Versions.assertj)
    testImplementation("org.awaitility", "awaitility", "3.0.0")

    testImplementation("org.junit.jupiter", "junit-jupiter-api", Versions.junit5)
    testImplementation("org.junit.jupiter", "junit-jupiter-params", Versions.junit5)
    testImplementation("org.junit.jupiter", "junit-jupiter-engine", Versions.junit5)
    testImplementation("org.awaitility", "awaitility", "3.0.0")
    
    testImplementation("org.testcontainers", "couchbase", Versions.testCouchbase)
}

configurations {
    testCompile.get().extendsFrom(compileOnly.get())
}

tasks {
    test {
        useJUnitPlatform()
        useJUnit { }
        testLogging {
            events("passed", "skipped", "failed")
        }
        finalizedBy(jacocoTestReport, jacocoTestCoverageVerification)
    }
    checkstyleMain.configure { enabled = false }
    checkstyleTest.configure { enabled = false }
    jacocoTestReport {
        reports {
            xml.isEnabled = false
            csv.isEnabled = false
            html.isEnabled = true
            html.destination = file("$buildDir/reports/coverage")
        }
    }
    jacocoTestCoverageVerification {
        violationRules {
            rule {
                limit {
                    minimum = valueOf(0.9)
                }
            }
        }
    }
}