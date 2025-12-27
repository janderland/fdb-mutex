variable "FENV_DOCKER_TAG" {
  default = "latest"
}

variable "FENV_EXT_DOCKER_TAG" {
  default = "latest"
}

target "fenv-ext" {
  dockerfile = "./docker/Dockerfile"
  tags       = ["fenv-ext:${FENV_EXT_DOCKER_TAG}"]
  contexts = {
    # Tell bake that "fenv:TAG" in FROM statements comes from fenv target
    "fenv:${FENV_DOCKER_TAG}" = "target:fenv"
  }
  args = {
    FENV_DOCKER_TAG = FENV_DOCKER_TAG
  }
}
