local params = import "params.libsonnet";

{
  apiVersion: "apps/v1",
  kind: "Deployment",
  metadata: {
    name: params.app + "-" + params.env + "-" + "scheduler",
    labels: {
      app: params.app,
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
          app: params.app,
          env: params.env,
          component: "scheduler",
        },
      },
      spec: {
        serviceAccountName: params.app + "-" + "scheduler",
        containers: [
          {
            name: "airflow",
            image: params.image.repo + ":" + params.image.tag,
            command: ["airflow", "scheduler"],
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
                value: "s3://$(S3_AWS_ACCESS_KEY_ID):$(S3_AWS_SECRET_ACCESS_KEY)@$(BUCKET_NAME)"
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
              name: params.app + "-" + params.env,
            },
          },
        ],
      },
    }
  }
}