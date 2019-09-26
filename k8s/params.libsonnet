{
    image: std.extVar("DOCKER_REPOSITORY") + ":bitbucket-" + std.extVar("BITBUCKET_BUILD_NUMBER"),
    env: std.extVar("BITBUCKET_DEPLOYMENT_ENVIRONMENT"),
    webserverSecretKey: std.extVar("WEBSERVER_SECRET_KEY"),
    fernetKey: std.extVar("FERNET_KEY"),
}