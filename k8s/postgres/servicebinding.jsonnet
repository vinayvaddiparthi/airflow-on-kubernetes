local params = import "../params.libsonnet";

{
  "apiVersion": "servicecatalog.k8s.io/v1beta1",
  "kind": "ServiceBinding",
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
    "instanceRef": {
      "name": params.app + "-" + params.env + "-" + "postgres",
    }
  }
}
