#!/usr/bin/env python3
import os

import aws_cdk as cdk

from kc_glue_stremaing_etl.kc_glue_stremaing_etl_stack import KcGlueStremaingEtlStack


app = cdk.App()
project_name = app.node.try_get_context("ProjectName")
logging = app.node.try_get_context("Logging")
logging = True if logging != 'false' else False
env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION'))
KcGlueStremaingEtlStack(app, "KcGlueStremaingEtlStack", env, project_name, logging)
app.synth()
