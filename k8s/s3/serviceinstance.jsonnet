local params = import "../params.libsonnet";

{
  "apiVersion": "servicecatalog.k8s.io/v1beta1",
  "kind": "ServiceInstance",
  "metadata": {
    "name": params.app + "-" + params.env + "-" + "s3",
    "labels": {
        "app": params.app,
        "env": params.env,
        "component": "s3"
    },
  },
  "spec": {
    "clusterServiceClassExternalName": "s3",
    "clusterServicePlanExternalName":
      if params.env == "production" then "production"
      else "custom",
    "parameters": if params.env != "production" then { PreventDeletion: "False" },
  }
}
