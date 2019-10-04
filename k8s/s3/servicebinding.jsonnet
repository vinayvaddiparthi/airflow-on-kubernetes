local params = import "../params.libsonnet";

{
  "apiVersion": "servicecatalog.k8s.io/v1beta1",
  "kind": "ServiceBinding",
  "metadata": {
    "name": params.app + "-" + params.env + "-" + "s3",
    "namespace": "airflow",
    "labels": {
      "app": params.app,
      "env": params.env,
      "component": "s3"
    }
  },
  "spec": {
    "instanceRef": {
      "name": params.app + "-" + params.env + "-" + "s3",
    }
  }
}
