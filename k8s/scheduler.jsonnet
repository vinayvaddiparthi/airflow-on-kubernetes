local params = import "params.libsonnet";

{
  apiVersion: "apps/v1",
  kind: "Deployment",
  metadata: {
    name: "airflow-production-scheduler",
    labels: {
      app: "airflow",
      env: params.env,
      component: "scheduler",
    },
    namespace: "airflow",
  },
  spec: {
    replicas: 1,
    strategy: {
      type: "Recreate",
    },
    selector: {
      matchLabels: {
        app: "airflow",
        env: params.env,
        component: "scheduler",
      },
    },
    template: {
      metadata: {
        labels: {
          app: "airflow",
          env: params.env,
          component: "scheduler",
        },
      },
      spec: {
        serviceAccountName: "airflow-scheduler",
        containers: [
          {
            name: "airflow",
            image: params.image.repo + ":" + params.image.tag,
            command: ["airflow", "scheduler"],
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
            volumeMounts: [
              {
                name: "config-volume",
                mountPath: "/usr/local/airflow/airflow.cfg",
                subPath: "airflow.cfg",
              },
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