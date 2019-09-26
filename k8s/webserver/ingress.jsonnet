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
      "kubernetes.io/ingress.class": "alb",
      "alb.ingress.kubernetes.io/certificate-arn": "arn:aws:acm:ca-central-1:810110616880:certificate/b14b320c-2578-4ea6-ba7b-b3beb3576b4f",
      "alb.ingress.kubernetes.io/inbound-cidrs": "184.95.225.32/27",
      "alb.ingress.kubernetes.io/scheme": "internet-facing",
      "alb.ingress.kubernetes.io/target-type": "ip",
      "alb.ingress.kubernetes.io/healthcheck-path": "/api/experimental/test",
    },
  },
  spec: {
    rules: [
      {
        host: "airflow.tcdata.co",
        http: {
          paths: [
            {
              path: "/*",
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
