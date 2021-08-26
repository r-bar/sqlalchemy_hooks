from . import events
from sqlalchemy.orm.decl_api import DeclarativeMeta
from typing import NamedTuple


class validate:
    __slots__ = 'trigger_event', 'execution_event', 'model', 'name'

    def __init__(
        self,
        func=None,
        *,
        trigger_event='before_save',
        execution_event='before_flush'
    ):
        self.trigger_event = trigger_event
        self.execution_event = execution_event or trigger_event
        if func is not None:
            self(func)

    def __call__(self, validator):
        """Decorator invocation"""
        self.validator = validator

    def __set_name__(self, model, name):
        """Called at class creation"""
        self.model = model
        self.name = name
        events.listen(self.trigger_event, self.model, self.execution_event)(self.run)

    def run(self, **kwargs):
        return self.validator(**kwargs)
