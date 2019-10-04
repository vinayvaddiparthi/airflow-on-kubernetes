local params = import "../params.libsonnet";

{
  "apiVersion": "servicecatalog.k8s.io/v1beta1",
  "kind": "ServiceInstance",
  "metadata": {
    "name": params.app + "-" + params.env + "-" + "postgres",
    "namespace": "airflow",
    "labels": {
      "app": params.app,
      "env": params.env,
      "component": "postgres"
    }
  },
  "spec": {
    "clusterServiceClassExternalName": "rdspostgresql",
    "clusterServicePlanExternalName": "production",
    "parameters": {
      "AccessCidr": params.postgres.accessCidr,
      "DBInstanceClass": params.postgres.instanceType,
    }
  }
}
