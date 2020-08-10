from importlib import import_module
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy import Column, Sequence
from sqlalchemy import Integer, String

import sys

Base = declarative_base()


class ModelMeta(DeclarativeMeta):
    def __new__(mcs, class_name, class_parents, class_attrs, *args, **kwargs):
        print(class_name)
        return super().__new__(mcs, class_name, class_parents, class_attrs)

    def __init__(cls, class_name, class_parents, class_attrs):
        print(class_attrs)
        super().__init__(class_name, class_parents, class_attrs)


def build_sqlalchemy_descriptor(dt: dict):
    native_to_sqlalchemy = {
        int: Integer,
        str: String,
    }
    descriptor = {
        column: native_to_sqlalchemy[type_] for column, type_ in dt.items()
    }
    return descriptor


def set_class_module(klass, module_name: str):
    klass.__module__ = module_name
    module = sys.modules[module_name]
    module.__dict__[klass.__name__] = klass


def define_table(table_name: str, model_name: str, module: str, descriptor: dict):
    import_module(module, __name__)

    cls_dict = dict()
    cls_dict['__tablename__'] = table_name

    column_dict = dict()
    sequence = Sequence('seq_reg_id', start=1, increment=1)
    column_dict['id'] = Column(Integer, sequence, primary_key=True, autoincrement=True)
    dynamic_column_dict = {
        column: Column(t) for column, t in descriptor.items()
    }
    column_dict.update(dynamic_column_dict)
    cls_dict.update(column_dict)

    base_classes = [Base]
    cls = ModelMeta(
        model_name,
        tuple(base_class for base_class in base_classes),
        cls_dict
    )

    set_class_module(cls, module)

    return cls

