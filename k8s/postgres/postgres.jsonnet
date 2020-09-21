local params = import "../params.libsonnet";

{
  apiVersion: "kubedb.com/v1alpha1",
  kind: "Postgres",
  metadata: {
    name: params.app + "-" + params.env + "-" + $.metadata.labels.component,
    namespace: params.namespace,
    labels: {
      app: params.app,
      env: params.env,
      component: "postgres",
    },
  },
  spec: {
    version: "9.6-v2",
    replicas: if params.env == "production" then 3 else 1,
    standbyMode: "Warm",
    storageType: if params.env == "production" then "Durable" else "Ephemeral",
    storage: if params.env == "production" then {
      storageClassName: "gp2-encrypted",
      accessModes: [ "ReadWriteOnce" ],
      resources: {
        requests: { storage: "20Gi" },
      },
    } else null,
    archiver: {
      storage: {
        storageSecretName: params.app + "-" + params.env + "-" + $.metadata.labels.component + "-" + "bk-auth",
        s3: {
          bucket: params.postgres.backups.bucket,
        },
      },
    },
    podTemplate: {
      spec: {
        resources: {
          requests: {
            cpu: "250m",
            memory: "512Mi",
          },
          limits: {
            cpu: "250m",
            memory: "512Mi",
          },
        },
      },
    },
  }
}
