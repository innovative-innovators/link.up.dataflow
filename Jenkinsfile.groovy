node {
    def mvnHome
    def JAVA_HOME
    def tempDir = "/VWMC/TempWorkDir"
    def currentProject = "link.up.dataflow"
    def project
    def topic
    def bucket
    def neo4j_url
    def neo4j_username
    def neo4j_password

    stage('Preparation') { // for display purposes

        JAVA_HOME = tool name: 'JDK1.8-152', type: 'jdk'
        mvnHome = tool name: 'Maven-3.5.2', type: 'maven'
    }

    stage('Checkout from Github') {

        sh("rm -rf ${tempDir}/${currentProject}")
        sh("mkdir ${tempDir}/${currentProject}")

        dir("${tempDir}/${currentProject}") {
            git 'https://github.com/innovative-innovators/link.up.dataflow.git'
        }
    }

    stage('Build') {
        dir("${tempDir}/${currentProject}") {
            sh("${mvnHome}/bin/mvn clean compile")
        }
    }

    stage('Input Required Info') {

        def inputParams = input(message: 'Required Info',
                parameters: [
                        [$class: 'StringParameterDefinition', defaultValue: '', description: '', name: 'Project Name'],
                        [$class: 'StringParameterDefinition', defaultValue: '', description: '', name: 'Topic Name'],
                        [$class: 'StringParameterDefinition', defaultValue: '', description: '', name: 'Bucket'],
                        [$class: 'StringParameterDefinition', defaultValue: '', description: '', name: 'Neo4J URL'],
                        [$class: 'StringParameterDefinition', defaultValue: '', description: '', name: 'Neo4J Username'],
                        [$class: 'hudson.model.PasswordParameterDefinition', defaultValue: '', description: '', name: 'Neo4J Password']
                ])

        project = inputParams['Project Name']
        topic = inputParams['Topic Name']
        bucket = inputParams['Bucket']
        neo4j_url = inputParams['Neo4J URL']
        neo4j_username = inputParams['Neo4J Username']
        neo4j_password = inputParams['Neo4J Password']

        /**
         * Hide Password output.
         */
        wrap([$class: 'MaskPasswordsBuildWrapper',
                varPasswordPairs: [[password: "${neo4j_password}", var: 'password']]
        ]) {
            sh("echo \"Project is : ${project}\"")
            sh("echo \"Topic is : ${topic}\"")
            sh("echo \"Bucket is : ${bucket}\"")
            sh("echo \"Neo4J URL is : ${neo4j_url}\"")
            sh("echo \"Neo4J UserName is : ${neo4j_username}\"")
            sh("echo \"Neo4J Password is : ${neo4j_password}\"")
        }

    }

    stage("Activate LinkUp-Dataflow") {
        dir("${tempDir}/${currentProject}") {
            sh("${mvnHome}bin/mvn clean compile -e exec:java -Dexec.mainClass=\"link.up.dataflow.Transformer\" -Dexec.args=\"--project=${project} --topic=${topic} --bucket=${bucket} --runner=DataflowRunner --stagingLocation=gs://${bucket}/pubsub.demo/StagingLogs/ --tempLocation=gs://${bucket}/pubsub.demo/StagingLogs/ --neo4jUrl=${neo4j_url} --userName=${neo4j_username} --password=${neo4j_password}\"")
        }
    }
}