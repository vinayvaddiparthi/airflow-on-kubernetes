{
    app: std.extVar("APP_NAME"),
    env: std.extVar("CI_ENVIRONMENT_SLUG"),
    namespace: std.extVar("KUBE_NAMESPACE"),
    image: {
        repo: std.extVar("DOCKER_REPOSITORY"),
        tag: std.extVar("DOCKER_IMAGE_TAG"),
    },

    webserver: {
      host: "airflow.tcdata.co",
      path: "/" + $.env,

      oidc: {
        issuer: "https://iam-production.tcdata.co/auth/realms/ztt-internal",
        clientId: std.extVar("OIDC_CLIENT_ID"),
        clientSecret: std.extVar("OIDC_CLIENT_SECRET"),
      },
      secretKey: std.extVar("WEBSERVER_SECRET_KEY"),
    },

    fernetKey: std.extVar("FERNET_KEY"),

    smtp: {
      username: std.extVar("AWS_SES_USERNAME"),
      password: std.extVar("AWS_SES_PASSWORD"),
    },
}
