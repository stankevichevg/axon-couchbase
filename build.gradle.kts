import java.math.BigDecimal.valueOf

plugins {
    java
    idea
    jacoco
    checkstyle
}

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    group = "org.axonframework.extensions.couchbase"
    version = "0.0.1-SNAPSHOT"

    apply<JacocoPlugin>()
    apply<CheckstylePlugin>()

    jacoco {
        toolVersion = Versions.jacoco
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

val jacocoAggregateMerge by tasks.creating(JacocoMerge::class) {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    executionData(
        project(":couchbase").buildDir.absolutePath + "/jacoco/test.exec"
    )
    dependsOn(
        ":couchbase:test"
    )
}

@Suppress("UnstableApiUsage")
val jacocoAggregateReport by tasks.creating(JacocoReport::class) {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    executionData(jacocoAggregateMerge.destinationFile)
    reports {
        xml.isEnabled = true
    }
    additionalClassDirs(files(subprojects.flatMap { project ->
        listOf("java", "kotlin").map { project.buildDir.path + "/classes/$it/main" }
    }))
    additionalSourceDirs(files(subprojects.flatMap { project ->
        listOf("java", "kotlin").map { project.file("src/main/$it").absolutePath }
    }))
    dependsOn(jacocoAggregateMerge)
}

tasks {
    jacocoTestCoverageVerification {
        executionData.setFrom(jacocoAggregateMerge.destinationFile)
        violationRules {
            rule {
                limit {
                    minimum = valueOf(0.9)
                }
            }
        }
        additionalClassDirs(files(subprojects.flatMap { project ->
            listOf("java", "kotlin").map { project.buildDir.path + "/classes/$it/main" }
        }))
        additionalSourceDirs(files(subprojects.flatMap { project ->
            listOf("java", "kotlin").map { project.file("src/main/$it").absolutePath }
        }))
        dependsOn(jacocoAggregateReport)
    }
    check {
        finalizedBy(jacocoTestCoverageVerification)
    }
}