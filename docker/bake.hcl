variable "FENV_EXT_DOCKER_TAG" {
  default = "latest"
}

target "fenv" {
  dockerfile = "./docker/Dockerfile"
  tags       = ["fdb-mutex:${FENV_EXT_DOCKER_TAG}"]
  args = {
    FENV_DOCKER_TAG = FENV_DOCKER_TAG
  }
}
