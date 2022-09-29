@Library('jenkins-libs') _

dockerPythonPipeline {
  app_name           = 'odd-collector'
  build_deploy_image = 'docker build -t ${ECR_URL}/${IMAGE_NAME}:${TAG} .'
  docker_label_agent = 'jenkins-agent'

  container_names_images = 'odd-collector'
}
