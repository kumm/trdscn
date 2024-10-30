from awacs.aws import Allow, Statement, Action, Policy
from troposphere import Template, GetAtt, Ref
from troposphere.serverless import Function, Environment


def add_loader_fmp_lambda(template: Template, trdscn_table):
    return template.add_resource(Function(
        "LoaderFmpLambda",
        Handler="loader_fmp.lambda_handler",
        Runtime="python3.12",
        CodeUri='functions/loader-fmp/',
        MemorySize=128,
        Timeout=900,
        ReservedConcurrentExecutions=1,
        Environment=Environment(
            Variables={
                "DDB_TABLE_NAME": Ref(trdscn_table),
                "FMP_API_KEY": '7QKqNgh9o8Uc7vM1NTOXEWrb7n2cUMWR'
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



