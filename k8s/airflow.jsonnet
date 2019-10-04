local k = import "ksonnet.beta.4/k.libsonnet";

local webserver = [import "webserver/deployment.jsonnet", import "webserver/service.jsonnet", import "webserver/ingress.jsonnet"];
local scheduler = [import "scheduler.jsonnet"];
local postgres = [import "postgres/serviceinstance.jsonnet", import "postgres/servicebinding.jsonnet"];
local secret = [import "secret.jsonnet"];
local config = import "config.jsonnet";
local s3 = [import "s3.jsonnet"];

k.core.v1.list.new(scheduler + webserver + postgres + config + secret + s3)