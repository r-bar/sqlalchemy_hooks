import logging
import inspect
import itertools as it
from typing import Union, Iterable, NamedTuple, Optional, Callable

import sqlalchemy as sa
import sqlalchemy.event
import sqlalchemy.orm.events
import sqlalchemy.events
from sqlalchemy.pool import Pool
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)


class EventInfo(NamedTuple):
    target_type: type
    callback_args: Iterable[str]

    @classmethod
    def from_event_class(cls, event_class: sa.event.Events) -> dict[str:"EventInfo"]:
        events = (
            evt
            for evt in dir(event_class)
            if not evt.startswith("_") and evt != "dispatch"
        )
        output = {}
        for event in events:
            method = getattr(event_class, event)
            signature = inspect.signature(method)
            args = tuple(name for name in signature.parameters.keys() if name != "self")
            output[event] = cls(event_class._dispatch_target, args)
        return output

    @property
    def arg_count(self):
        return len(self.callback_args)

    def kwargs(self, args):
        return dict(zip(self.callback_args, args))


pool_events = EventInfo.from_event_class(sqlalchemy.events.PoolEvents)
connection_events = EventInfo.from_event_class(sa.events.ConnectionEvents)
dialect_events = EventInfo.from_event_class(sa.events.DialectEvents)
ddl_events = EventInfo.from_event_class(sa.events.DDLEvents)
session_events = EventInfo.from_event_class(sa.orm.events.SessionEvents)
mapper_events = EventInfo.from_event_class(sa.orm.events.MapperEvents)
instance_events = EventInfo.from_event_class(sa.orm.events.InstanceEvents)
attribute_events = EventInfo.from_event_class(sa.orm.events.AttributeEvents)
query_events = EventInfo.from_event_class(sa.orm.events.QueryEvents)
instrmentation_events = EventInfo.from_event_class(sa.orm.events.InstrumentationEvents)
synthetic_mapper_events = {
    "after_save": EventInfo(sa.orm.Mapper, ("mapper", "connection", "target")),
    "before_save": EventInfo(sa.orm.Mapper, ("mapper", "connection", "target")),
    "before_touch": EventInfo(sa.orm.Mapper, ("mapper", "connection", "target")),
    "after_touch": EventInfo(sa.orm.Mapper, ("mapper", "connection", "target")),
}
events = (
    pool_events
    | connection_events
    | dialect_events
    | ddl_events
    | session_events
    | mapper_events
    | instance_events
    | attribute_events
    | query_events
    | instrmentation_events
    | synthetic_mapper_events
)


def register(target, event, func, **kwargs):
    """Proxy for sa.event.listen that handles dispatching synthetic_events"""
    if event == "after_save":
        sa.event.listen(target, "after_insert", func, **kwargs)
        sa.event.listen(target, "after_update", func, **kwargs)
        logger.debug(
            "Registered %s for after_insert and after_update events"
            " for %s: %s synthetic event",
            func,
            target,
            event,
        )
    elif event == "before_save":
        sa.event.listen(target, "before_insert", func, **kwargs)
        sa.event.listen(target, "before_update", func, **kwargs)
        logger.debug(
            "Registered %s for before_insert and before_update events"
            " for %s: %s synthetic event",
            func,
            target,
            event,
        )
    elif event == "after_touch":
        sa.event.listen(target, "after_insert", func, **kwargs)
        sa.event.listen(target, "after_update", func, **kwargs)
        sa.event.listen(target, "after_delete", func, **kwargs)
        logger.debug(
            "Registered %s for after_insert, after_update, and after_delete"
            " events for %s: %s synthetic event",
            func,
            target,
            event,
        )
    elif event == "before_touch":
        sa.event.listen(target, "before_insert", func, **kwargs)
        sa.event.listen(target, "before_update", func, **kwargs)
        sa.event.listen(target, "before_delete", func, **kwargs)
        logger.debug(
            "Registered %s for before_insert, before_update, and before_delete"
            " events for %s: %s synthetic event",
            func,
            target,
            event,
        )
    else:
        sa.event.listen(target, event, func, **kwargs)


class DefferredTarget(NamedTuple):
    callback: Callable[..., object]

    def __call__(self, *args):
        return self.callback(*args)


class EventListener:
    def __init__(
        self,
        target: object,
        event: str,
        *,
        use_kwargs: bool = False,
        once: bool = False,
    ):
        self.targets = [(target, event)]
        self.conditions = [None]
        self.use_kwargs = use_kwargs
        self.once = once

        self.name = None
        self.func = None
        self.args = []
        self.method_name = None
        self.method_class = None

    def __set_name__(self, class_, name):
        self.method_name = name
        self.method_class = class_

    @property
    def _is_method_callback(self):
        return self.method_name is not None

    def _get_kwargs(self, args):
        arg_names = it.chain.from_iterable(evt.callback_args for _, evt in self.targets)
        return dict(zip(arg_names, args))

    def _get_callback(self, i: int, arg_accum: list = None) -> Callable:
        do_execute = i >= len(self.targets)
        arg_accum = arg_accum or []

        if do_execute:
            return self._wrapper_callback(arg_accum)
        else:
            return self._register_callback(i, arg_accum)

    def _wrapper_callback(self, arg_accum: list) -> Callable:
        def wrapper(*args):
            logger.debug("got here")
            all_args = arg_accum + list(args)
            if self.use_kwargs:
                return self.func(**self._get_kwargs(all_args))
            return self.func(*all_args)

        return wrapper

    def _condition_met(self, i, args):
        condition = self.conditions[i]
        if condition is None:
            return True
        if self.use_kwargs:
            return condition(**self._get_kwargs(args))
        else:
            return condition(*args)

    def _register_callback(self, i: int, arg_accum: list = None) -> Callable:
        # chain listeners are always single execution, only the base of the
        # chain can be called multiple times.

        def _register(*args):
            nonlocal arg_accum
            arg_accum += list(args)
            if not self._condition_met(i, arg_accum):
                return

            target, event = self.targets[i]
            if isinstance(target, DefferredTarget):
                target = target(*args)

            once = self.once or i > 0

            register(target, event, self._get_callback(i + 1, arg_accum), once=once)
            logger.debug(
                "Performed %s%sregistration for %s: %s of %s",
                "one time " if once else "",
                "chain " if i > 0 else "",
                target,
                event,
                self.name,
            )

        return _register

    def __call__(self, func: Callable) -> Callable:
        if not self.name:
            self.name = func.__name__
        func.__listener__ = self
        self.func = func
        self._get_callback(0)()
        return func

    def chain(
        self,
        target: Union[DefferredTarget, object],
        event: str,
        condition: Optional[Callable[..., bool]] = None,
    ) -> "EventListener":
        self.targets.append((target, event))
        self.conditions.append(condition)
        return self

    def remove(self):
        for i, (target, event) in enumerate(self.targets):
            sa.event.remove(target, identifier, self._get_callback(i))

    def __repr__(self):
        clsname = type(self).__name__
        return f"<{clsname} {self.name}>"


def before_insert(model, session, **kwargs):
    def decorator(func):
        @EventListener(session, 'before_flush', **kwargs)
        def wrapper(session, flush_context, instances):
            for instance in session.new:
                func(session, flush_context, instances, instance)
        return wrapper
    return decorator


def after_insert(
    model,
    execution_event: str = "after_flush_postexec",
    execution_target: Union[DefferredTarget, object] = DefferredTarget(
        lambda conn, mapper, target: sa.orm.object_session(target)
    ),
    **kwargs,
):
    return EventListener(model, "after_insert", **kwargs).chain(
        execution_target,
        execution_event,
    )


def before_update(
    model,
    session,
    **kwargs,
):
    def decorator(func):
        @EventListener(session, 'before_flush', **kwargs)
        def wrapper(session, flush_context, instances):
            for instance in session.dirty:
                func(session, flush_context, instances, instance)
        return wrapper
    return decorator


def after_update(
    model,
    execution_event: str = "after_flush_postexec",
    execution_target: Union[DefferredTarget, object] = DefferredTarget(
        lambda conn, mapper, target: sa.orm.object_session(target)
    ),
    **kwargs,
):
    return EventListener(model, "after_update", **kwargs).chain(
        execution_target,
        execution_event,
    )


def before_delete(
    model,
    session,
    **kwargs,
):
    def decorator(func):
        @EventListener(session, 'before_flush', **kwargs)
        def wrapper(session, flush_context, instances):
            for instance in session.deleted:
                func(session, flush_context, instances, instance)
        return wrapper
    return decorator


def after_delete(
    model,
    execution_event: str = "after_flush_postexec",
    execution_target: Union[DefferredTarget, object] = DefferredTarget(
        lambda conn, mapper, target: sa.orm.object_session(target)
    ),
    **kwargs,
):
    return EventListener(model, "after_delete", **kwargs).chain(
        execution_target,
        execution_event,
    )


def before_save(model, session, **kwargs):
    def decorator(func):
        @EventListener(session, 'before_flush', **kwargs)
        def wrapper(session, flush_context, instances):
            for instance in set(session.new) | set(session.dirty):
                func(session, flush_context, instances, instance)
        return wrapper
    return decorator


def after_save(
    model,
    execution_event: str = "after_flush_postexec",
    execution_target: Union[DefferredTarget, object] = DefferredTarget(
        lambda conn, mapper, target: sa.orm.object_session(target)
    ),
    **kwargs,
):
    return EventListener(model, "after_save", **kwargs).chain(
        execution_target,
        execution_event,
    )


def before_touch(model, session, **kwargs):
    def decorator(func):
        @EventListener(session, 'before_flush', **kwargs)
        def wrapper(session, flush_context, instances):
            for instance in set(session.new) | set(session.dirty) | set(session.deleted):
                func(session, flush_context, instances, instance)
        return wrapper
    return decorator


def after_touch(
    model,
    execution_event: str = "after_flush_postexec",
    execution_target: Union[DefferredTarget, object] = DefferredTarget(
        lambda conn, mapper, target: sa.orm.object_session(target)
    ),
    **kwargs,
):
    return EventListener(model, "after_touch", **kwargs).chain(
        execution_target,
        execution_event,
    )
