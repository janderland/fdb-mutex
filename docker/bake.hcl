variable "FENV_DOCKER_TAG" {
  default = "latest"
}

variable "FENV_EXT_DOCKER_TAG" {
  default = "latest"
}

variable "FENV_EXT_IMAGE_NAME" {
  default = "fdb-mutex"
}

target "fenv" {
  dockerfile = "./docker/Dockerfile"
  tags       = ["${FENV_EXT_IMAGE_NAME}:${FENV_EXT_DOCKER_TAG}"]
  contexts = {
    # Tell bake that "fenv:TAG" in FROM statements comes from fenv-base target
    "fenv:${FENV_DOCKER_TAG}" = "target:fenv-base"
  }
  args = {
    FENV_DOCKER_TAG = FENV_DOCKER_TAG
  }
}
