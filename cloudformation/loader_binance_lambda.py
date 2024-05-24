import awacs
from awacs.aws import Allow, Statement, Action, Policy
from troposphere import Template, GetAtt, Ref
from troposphere.serverless import Function, Environment


def add_loader_binance_lambda(template: Template, trdscn_table):
    return template.add_resource(Function(
        "LoaderBinanceLambda",
        Handler="loader_binance.lambda_handler",
        Runtime="python3.11",
        CodeUri='functions/loader-binance/',
        MemorySize=128,
        Timeout=900,
        ReservedConcurrentExecutions=1,
        Environment=Environment(
            Variables={
                "DDB_TABLE_NAME": Ref(trdscn_table),
            }
        ),
        Policies=[Policy(
            Version="2012-10-17",
            Statement=[
                Statement(
                    Effect=Allow,
                    Action=[
                        Action("dynamodb", "GetItem"),
                        Action("dynamodb", "PutItem"),
                        Action("dynamodb", "UpdateItem"),
                        Action("dynamodb", "BatchWriteItem"),
                        Action("dynamodb", "Query"),
                        Action("dynamodb", "DeleteItem"),
                    ],
                    Resource=[
                        GetAtt(trdscn_table, 'Arn'),
                    ]
                ),
            ]
        )]
    ))
