mvn install apache-rat:check -Drat.numUnapprovedLicenses=600 -Pjanusgraph-release -Dgpg.skip=true -DskipTests=true

cp janusgraph-core/target/janusgraph-core-0.5.0-SNAPSHOT.jar ~/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/lib/
