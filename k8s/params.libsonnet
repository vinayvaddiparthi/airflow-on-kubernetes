{
    app: std.extVar("APP_NAME"),
    env: std.extVar("CI_ENVIRONMENT_SLUG"),
    namespace: std.extVar("KUBE_NAMESPACE"),
    image: {
        repo: std.extVar("DOCKER_REPOSITORY"),
        tag: std.extVar("DOCKER_IMAGE_TAG"),
    },

    webserverSecretKey: std.extVar("WEBSERVER_SECRET_KEY"),
    fernetKey: std.extVar("FERNET_KEY"),

    smtp: {
      username: std.extVar("AWS_SES_USERNAME"),
      password: std.extVar("AWS_SES_PASSWORD"),
    },
}
