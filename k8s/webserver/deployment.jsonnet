local params = import "../params.libsonnet";

{
  apiVersion: "apps/v1",
  kind: "Deployment",
  metadata: {
    name: "airflow-production-webserver",
    labels: {
      app: "airflow",
      env: params.env,
      component: "webserver",
    },
    namespace: "airflow",
  },
  spec: {
    replicas: 2,
    selector: {
      matchLabels: {
        app: "airflow",
        env: params.env,
        component: "webserver",
      },
    },
    template: {
      metadata: {
        labels: {
          app: "airflow",
          env: params.env,
          component: "webserver",
        },
      },
      spec: {
        containers: [
          {
            name: "airflow",
            image: params.image,
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
                  name: "airflow-production-postgres"
                },
                prefix: "AIRFLOWDB_"
              },
              {
                secretRef: {
                  name: "airflow-production"
                },
              },
            ],
            env: [
              {
                name: "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
                value: "postgresql+psycopg2://$(AIRFLOWDB_MASTER_USERNAME):$(AIRFLOWDB_MASTER_PASSWORD)@$(AIRFLOWDB_ENDPOINT_ADDRESS):$(AIRFLOWDB_PORT)/$(AIRFLOWDB_DB_NAME)"
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
              name: "airflow-production",
            },
          },
        ],
      },
    }
  }
}
