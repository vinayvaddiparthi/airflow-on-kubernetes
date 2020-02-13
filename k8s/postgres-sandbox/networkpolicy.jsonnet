local params = import "../params.libsonnet";

{
  apiVersion: "crd.projectcalico.org/v1",
  kind: "NetworkPolicy",
  metadata: {
    name: params.app + "-" + params.env + "-" + "postgres-sb",
    namespace: params.namespace,
    labels: {
      app: params.app,
      env: params.env,
      component: "postgres-sb",
    },
  },
  spec: {
    selector: "kubedb.com\\/name == '" + params.app + "-" + params.env + "-" + "postgres-sb" + "'",
    ingress: [
      {
        action: "Allow",
        protocol: "TCP",
        source: {
          selector: "app == '" + params.app + "' && env == '" + params.env + "'",
        },
        destination: {
          ports: [
            5432,
          ]
        }
      },
      {
        action: "Allow",
        source: {
          namespaceSelector: "name == 'kube-system'",
          selector: "all()",
        },
      },
      {
        action: "Allow",
        protocol: "TCP",
        source: {
          selector: "kubedb.com\\/name == '" + params.app + "-" + params.env + "-" + "postgres-sb" + "'",
        },
        destination: {
          ports: [
            5432,
          ]
        }
      }
    ]
  }
}
