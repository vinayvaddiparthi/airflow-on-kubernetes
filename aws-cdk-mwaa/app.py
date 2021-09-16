#!/usr/bin/env python3
import io
from aws_cdk import core

from mwaa_cdk_stacks.mwaa_cdk_backend import MwaaCdkStackBackend
from mwaa_cdk_stacks.mwaa_cdk_env import MwaaCdkStackEnv

env_CA=core.Environment(region="us-east-1", account="623810217472")
mwaa_props = {'dagss3location': 'tc-mwaa-development','mwaa_env' : 'tc-mwaa-development'}

app = core.App()

mwaa_backend = MwaaCdkStackBackend(
    scope=app,
    id="MWAA-Backend",
    env=env_CA,
    mwaa_props=mwaa_props
)

mwaa_env = MwaaCdkStackEnv(
    scope=app,
    id="MWAA-Environment",
    vpc=mwaa_backend.vpc,
    env=env_CA,
    mwaa_props=mwaa_props
)

app.synth()
