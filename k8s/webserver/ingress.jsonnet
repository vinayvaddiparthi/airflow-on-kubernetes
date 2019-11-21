local params = import "../params.libsonnet";

{
  apiVersion: "extensions/v1beta1",
  kind: "Ingress",
  metadata: {
    name: params.app + "-" + params.env + "-" + "webserver",
    namespace: "airflow",
    labels: {
      app: params.app,
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
              path: "/" + params.env,
              backend: {
                serviceName: params.app + "-" + params.env + "-" + "webserver",
                servicePort: 8080
              },
            },
          ],
        },
      },
    ],
  },
}
