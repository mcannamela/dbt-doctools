from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.parsed import ParsedSourceDefinition
from yaml import load, dump
from yaml import CLoader as Loader, CDumper as Dumper


def write_source(source:ParsedSourceDefinition, file_of_source:SchemaSourceFile):
    with open('thefile.yaml', 'w') as f:
        dump(file_of_source.dict_from_yaml, f)