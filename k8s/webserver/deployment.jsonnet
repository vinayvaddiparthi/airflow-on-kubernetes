local params = import "../params.libsonnet";

{
  apiVersion: "apps/v1",
  kind: "Deployment",
  metadata: {
    name: params.app + "-" + params.env + "-" + "webserver",
    labels: {
      app: params.app,
      env: params.env,
      component: "webserver",
    },
    namespace: "airflow",
  },
  spec: {
    replicas: 2,
    selector: {
      matchLabels: {
        app: params.app,
        env: params.env,
        component: "webserver",
      },
    },
    template: {
      metadata: {
        labels: {
          app: params.app,
          env: params.env,
          component: "webserver",
        },
      },
      spec: {
        containers: [
          {
            name: "airflow",
            image: params.image.repo + ":" + params.image.tag,
            command: ["airflow", "webserver"],
            readinessProbe: {
              periodSeconds: 10,
              httpGet: {
                path: "/api/experimental/test",
                port: 8080,
              },
            },
            livenessProbe: {
              initialDelaySeconds: 60,
              httpGet: {
                path: "/api/experimental/test",
                port: 8080,
              },
            },
            envFrom: [
              {
                secretRef: {
                  name: params.app + "-" + params.env + "-" + "postgres"
                },
              },
              {
                secretRef: {
                  name: params.app + "-" + params.env + "-" + "s3"
                },
              },
              {
                secretRef: {
                  name: params.app + "-" + params.env
                },
              },
              {
                configMapRef: {
                  name: params.app + "-" + params.env + "-" + "env"
                },
              },
            ],
            ports: [
              { containerPort: 8080 },
            ],
            volumeMounts: [
              {
                name: "config-volume",
                mountPath: "/usr/local/airflow/airflow.cfg",
                subPath: "airflow.cfg",
              },
              {
                name: "config-volume",
                mountPath: "/usr/local/airflow/webserver_config.py",
                subPath: "webserver_config.py",
              }
            ],
          },
        ],
        volumes: [
          {
            name: "config-volume",
            configMap: {
              name: params.app + "-" + params.env,
            },
          },
        ],
      },
    }
  }
}
