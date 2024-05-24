from troposphere import Template, GetAtt, Ref
from troposphere.awslambda import Permission
from troposphere.events import Rule, Target, RetryPolicy
from troposphere.serverless import SERVERLESS_TRANSFORM

from dynamodb import add_trdscn_table
from loader_binance_lambda import add_loader_binance_lambda
from loader_fmp_lambda import add_loader_fmp_lambda


def add_lambda_schedule(function, schedule, title, desc):
    rule = template.add_resource(Rule(
        f"{title}LambdaScheduleRule",
        Description=desc,
        State="ENABLED",
        ScheduleExpression=schedule,
        Targets=[Target(
            Arn=GetAtt(function, 'Arn'),
            Id="TargetRuleScheduleLambda",
            RetryPolicy=RetryPolicy(
                MaximumRetryAttempts=2
            )
        )]
    ))
    template.add_resource(Permission(
        f"{title}LambdaInvokePermission",
        FunctionName=Ref(function),
        Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",
        SourceArn=GetAtt(rule, 'Arn')
    ))


template = Template()
template.set_transform(SERVERLESS_TRANSFORM)

trdscn_table = add_trdscn_table(template)
loader_binance_lambda = add_loader_binance_lambda(template, trdscn_table)
loader_fmp_lambda = add_loader_fmp_lambda(template, trdscn_table)

add_lambda_schedule(
    function=loader_binance_lambda,
    schedule="cron(0 1 * * ? *)",
    title="LoaderBinance",
    desc="Invoke Binance loader lambda daily"
)
add_lambda_schedule(
    function=loader_fmp_lambda,
    schedule="cron(0 23 * * ? *)",
    title="LoaderFmp",
    desc="Invoke FMP loader lambda daily"
)

with open('../template.yaml', 'w') as f:
    f.write(template.to_yaml())
