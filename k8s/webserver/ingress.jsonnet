local params = import "../params.libsonnet";

{
  apiVersion: "networking.k8s.io/v1",
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
        host: params.webserver.host,
        http: {
          paths: [
            {
              path: params.webserver.path,
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
