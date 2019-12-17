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
    strategy: {
      type: "RollingUpdate",
      rollingUpdate: {
        maxUnavailable: 1,
      },
    },
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
            resources: {
              requests: {
                cpu: "250m",
                memory: "768Mi",
              },
            },
            readinessProbe: {
              periodSeconds: 10,
              httpGet: {
                path: "/" + params.env + "/api/experimental/test",
                port: 8080,
              },
            },
            livenessProbe: {
              initialDelaySeconds: 60,
              httpGet: {
                path: "/" + params.env + "/api/experimental/test",
                port: 8080,
              },
            },
            envFrom: [
              {
                secretRef: {
                  name: params.app + "-" + params.env + "-" + "postgres-auth"
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
            ],
            env: [
              {
                name: "AIRFLOW_CONN_S3_LOGS",
                value: "s3://$(BUCKET_NAME)",
              },
              {
                name: "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
                value: "postgresql+psycopg2://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@" + params.app + "-" + params.env + "-" + "postgres" + "/postgres"
              },
              {
                name: "AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER",
                value: "s3://$(BUCKET_NAME)/logs",
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
