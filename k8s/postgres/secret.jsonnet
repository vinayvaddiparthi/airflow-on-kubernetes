local params = import "../params.libsonnet";

{
  apiVersion: "v1",
  kind: "Secret",
  metadata: {
    name: params.app + "-" + params.env + "-" + $.metadata.labels.component + "-" + "bk-auth",
    namespace: params.namespace,
    labels: {
      app: params.app,
      env: params.env,
      component: "postgres",
    },
  },
  data: {
    "AWS_ACCESS_KEY_ID": std.base64(params.postgres.backups.awsAccessKeyId),
    "AWS_SECRET_ACCESS_KEY": std.base64(params.postgres.backups.awsSecretAccessKey),
  },
}
