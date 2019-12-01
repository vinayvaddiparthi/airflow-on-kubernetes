local params = import "params.libsonnet";

{
  apiVersion: "v1",
  kind: "Secret",
  metadata: {
    name: params.app + "-" + params.env,
    namespace: "airflow",
    labels: {
      app: "airflow",
      env: params.env,
    },
  },
  data: {
    AIRFLOW__CORE__FERNET_KEY: std.base64(params.fernetKey),
    AIRFLOW__WEBSERVER__SECRET_KEY: std.base64(params.webserverSecretKey),
    AIRFLOW__SMTP__SMTP_USER: std.base64(params.smtp.username),
    AIRFLOW__SMTP__SMTP_PASSWORD: std.base64(params.smtp.password),
  },
}
