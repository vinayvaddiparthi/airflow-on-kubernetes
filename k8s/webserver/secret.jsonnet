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
    "client_secret.json": std.base64(
    std.toString(
      {
        web: {
          "client_id": "airflow-production",
          "client_secret": "c48d6f58-5098-43f0-833f-430c42edfceb",
          "auth_uri": "https://iam-production.tcdata.co/auth/realms/ztt-internal/protocol/openid-connect/auth",
          "token_uri": "https://iam-production.tcdata.co/auth/realms/ztt-internal/protocol/openid-connect/token",
          "userinfo_uri": "https://iam-production.tcdata.co/auth/realms/ztt-internal/protocol/openid-connect/userinfo",
          "issuer": "https://iam-production.tcdata.co/auth/realms/ztt-internal",
          "redirect_uris": "https://airflow.tcdata.co/review-oidc-auth-dew5ck/oidc_callback",
        },
      },
    )),
  },
}
