local params = import "../params.libsonnet";

{
  "apiVersion": "servicecatalog.k8s.io/v1beta1",
  "kind": "ServiceBinding",
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
    "instanceRef": {
      "name": "airflow-production-postgres"
    }
  }
}