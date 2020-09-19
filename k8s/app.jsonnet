local k = import "ksonnet.beta.4/k.libsonnet";

local webserver = [
  import "webserver/deployment.jsonnet",
  import "webserver/service.jsonnet",
  import "webserver/ingress.jsonnet",
  import "webserver/secret.jsonnet"
];

local scheduler = [import "scheduler.jsonnet"];
local postgres = [
  import "postgres/postgres.jsonnet",
  import "postgres/networkpolicy.jsonnet"
];
local secret = [import "secret.jsonnet"];
local config = [import "config.jsonnet"];
local s3 = [import "s3/serviceinstance.jsonnet", import "s3/servicebinding.jsonnet"];

k.core.v1.list.new(scheduler + webserver + postgres + config + secret + s3)
