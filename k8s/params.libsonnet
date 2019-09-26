{
    env: std.extVar("BITBUCKET_DEPLOYMENT_ENVIRONMENT"),

    image: {
        repo: std.extVar("DOCKER_REPOSITORY"),
        tag: "bitbucket-" + std.extVar("BITBUCKET_BUILD_NUMBER"),
    },
    
    webserverSecretKey: std.extVar("WEBSERVER_SECRET_KEY"),
    fernetKey: std.extVar("FERNET_KEY"),
}
