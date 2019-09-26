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
    AIRFLOW__CORE__FERNET_KEY: params.fernetKey,
    AIRFLOW__WEBSERVER__SECRET_KEY: params.webserverSecretKey,
  },
}