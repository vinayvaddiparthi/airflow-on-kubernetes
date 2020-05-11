local params = import "../params.libsonnet";

{
  apiVersion: "v1",
  kind: "Secret",
  metadata: {
    name: params.app + "-" + params.env + "-" + $.metadata.labels.component,
    namespace: "airflow",
    labels: {
      app: params.app,
      env: params.env,
      component: "webserver",
    },
  },
  data: {
    secretKey: std.base64(params.webserver.secretKey),
    "client_secret.json": std.base64(
    std.toString(
      {
        web: {
          "client_id": params.webserver.oidc.clientId,
          "client_secret": params.webserver.oidc.clientSecret,
          "auth_uri": params.webserver.oidc.issuer + "/protocol/openid-connect/auth",
          "token_uri": params.webserver.oidc.issuer + "/protocol/openid-connect/token",
          "userinfo_uri": params.webserver.oidc.issuer + "/protocol/openid-connect/userinfo",
          "issuer": params.webserver.oidc.issuer,
          "redirect_uris": params.webserver.host + params.webserver.path + "/oidc_callback",
        },
      },
    )),
  },
}
