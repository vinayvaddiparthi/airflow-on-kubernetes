local params = import "params.libsonnet";

{
  apiVersion: "v1",
  kind: "Secret",
  metadata: {
    name: "airflow-production",
    namespace: "airflow",
    labels: {
      app: "airflow",
      env: params.env,
    },
  },
  data: {
    AIRFLOW__CORE__FERNET_KEY: std.base64(params.fernetKey),
    AIRFLOW__WEBSERVER__SECRET_KEY: std.base64(params.webserverSecretKey),
  },
}