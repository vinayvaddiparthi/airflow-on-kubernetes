local params = import "../params.libsonnet";

{
  apiVersion: "v1",
  kind: "Service",
  metadata: {
    name: "airflow-production-webserver",
    namespace: "airflow",
    labels: {
      app: "airflow",
      env: params.env,
      component: "webserver",
    },
  },
  spec: {
    selector: {
      app: "airflow",
      env: params.env,
      component: "webserver",
    },
    type: "ClusterIP",
    ports: [
      {
        protocol: "TCP",
        port: 8080
      },
    ],
  },
}