local params = import "../params.libsonnet";

{
  "apiVersion": "servicecatalog.k8s.io/v1beta1",
  "kind": "ServiceInstance",
  "metadata": {
    "name": "airflow-production-postgres",
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
      "AccessCidr": "172.20.0.0/16",
      "DBInstanceClass": "db.t2.small"
    }
  }
}