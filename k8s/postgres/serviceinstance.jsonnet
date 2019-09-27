local params = import "../params.libsonnet";

{
  "apiVersion": "servicecatalog.k8s.io/v1beta1",
  "kind": "ServiceInstance",
  "metadata": {
    "name": params.app + "-" + params.env + "-" + "postgres",
    "namespace": "airflow",
    "labels": {
      "app": "airflow",
      "env": params.env,
      "component": "db"
    }
  },
  "spec": {
    "clusterServiceClassExternalName": "rdspostgresql",
    "clusterServicePlanExternalName": "production",
    "parameters": {
      "AccessCidr": params.postgres.AccessCidr,
      "DBInstanceClass": params.postgres.instanceType,
    }
  }
}