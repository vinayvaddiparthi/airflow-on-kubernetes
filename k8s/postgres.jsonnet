local params = import "params.libsonnet";

{
  apiVersion: "kubedb.com/v1alpha1",
  kind: "Postgres",
  metadata: {
    name: params.app + "-" + params.env + "-" + "postgres",
    namespace: params.namespace,
    labels: {
      app: params.app,
      env: params.env,
      component: "postgres",
    },
  },
  spec: {
    version: "9.6-v2",
    replicas: 3,
    standbyMode: "Warm",
    storageType: "Durable",
    storage: {
      storageClassName: "gp2-encrypted",
      accessModes: [ "ReadWriteOnce" ],
      resources: {
        requests: { storage: "20Gi" },
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
