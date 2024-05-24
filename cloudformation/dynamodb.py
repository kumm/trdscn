from troposphere import Template
from troposphere.dynamodb import AttributeDefinition, KeySchema
from troposphere.dynamodb import Table


def add_trdscn_table(template: Template):
    return template.add_resource(Table(
        "TrdScnTable",
        AttributeDefinitions=[
            AttributeDefinition(
                AttributeName="hash",
                AttributeType="S"
            ),
            AttributeDefinition(
                AttributeName="sort",
                AttributeType="S"
            )
        ],
        KeySchema=[
            KeySchema(
                AttributeName="hash",
                KeyType="HASH"
            ),
            KeySchema(
                AttributeName="sort",
                KeyType="RANGE"
            )
        ],
        BillingMode='PAY_PER_REQUEST',
        DeletionPolicy="Retain",
        UpdateReplacePolicy="Retain"
    ))
