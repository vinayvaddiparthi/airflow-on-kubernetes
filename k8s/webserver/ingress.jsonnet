local params = import "../params.libsonnet";

{
  apiVersion: "extensions/v1beta1",
  kind: "Ingress",
  metadata: {
    name: "airflow-production-webserver",
    namespace: "airflow",
    labels: {
      app: "airflow",
      env: params.env,
      component: "webserver",
    },
    annotations: {
      "kubernetes.io/ingress.class": "traefik",
    },
  },
  spec: {
    rules: [
      {
        host: "airflow.tcdata.co",
        http: {
          paths: [
            {
              path: "/",
              backend: {
                serviceName: "airflow-production-webserver",
                servicePort: 8080
              },
            },
          ],
        },
      },
    ],
  },
}
