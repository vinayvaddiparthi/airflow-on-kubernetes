local params = import "../params.libsonnet";

{
  apiVersion: "v1",
  kind: "Service",
  metadata: {
    name: params.app + "-" + params.env + "-" + "webserver",
    namespace: "airflow",
    labels: {
      app: params.app,
      env: params.env,
      component: "webserver",
    },
  },
  spec: {
    selector: {
      app: params.app,
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