{
    app: "airflow",
    env: std.extVar("CI_ENVIRONMENT_SLUG"),

    image: {
        repo: std.extVar("DOCKER_REPOSITORY"),
        tag: std.extVar("DOCKER_IMAGE_TAG"),
    },

    webserverSecretKey: std.extVar("WEBSERVER_SECRET_KEY"),
    fernetKey: std.extVar("FERNET_KEY"),
}
