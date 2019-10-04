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