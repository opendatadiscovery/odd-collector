@Library('jenkins-libs') _

pipeline {
    agent { label 'jenkins-agent' }

    options{
      ansiColor('xterm')
      buildDiscarder(
        logRotator(
          artifactDaysToKeepStr: '10',
          artifactNumToKeepStr: '10',
          daysToKeepStr: '5',
          numToKeepStr: '5'
        )
      )
      disableConcurrentBuilds()
      timeout(time: 20, unit: 'MINUTES')
    }

    environment {
      ECR_URL = getEcrUrl()
      ECR_REGION = "$ECR_URL".find(/\w{2}\-\w{4}\-\d{1}/)
      IMAGE_REPO = "$ECR_URL/$IMAGE_NAME"
      IMAGE_NAME = "odd-collector"
      GIT_SHA8 = "${GIT_COMMIT[0..7]}"
      JENKINS_ENVIRONMENT = "${System.getenv('JENKINS_ENVIRONMENT') ?: 'none'}"
    }

    stages {

      stage('ECR auth'){
        steps{
          sh """
            set +x
            \$(aws ecr get-login --no-include-email --region ${ECR_REGION})
          """
        }
      }

      stage('Build image') {
        steps {
          echo 'Building image'
          sh "docker build -t ${ECR_URL}/${IMAGE_NAME}:${GIT_SHA8} ."
        }
      }

      stage('Pushing image') {
        steps {
          sh """
            docker push ${ECR_URL}/${IMAGE_NAME}:${GIT_SHA8}
            docker rmi -f ${ECR_URL}/${IMAGE_NAME}:${GIT_SHA8} >/dev/null 2>&1
          """
        }
      }
    }

    post {
      always {
        dockerImages('remove', ["${ECR_URL}/${IMAGE_NAME}:${GIT_SHA8}"])
      }
    }
}
