pipeline {
  agent any
  stages {
    stage('Build and test') {
      steps{
        withMaven {
          sh 'mvn clean deploy -f invesdwin-context-integration-parent/pom.xml -T4'
        }  
      }
    }
  }
}