import logging
import pytest
import sqlalchemy as sa
import sqlalchemy.event
from sqlalchemy_hooks import events


logger = logging.getLogger("test")


@pytest.mark.parametrize(
    "name,event_listener,called_after_created,called_after_updated,called_after_deleted",
    [
        (
            "before_insert helper",
            events.before_insert,
            True,
            False,
            False,
        ),
        (
            "after_insert helper",
            lambda model, session: events.after_insert(model),
            True,
            False,
            False,
        ),
        (
            "before_update helper",
            events.before_update,
            False,
            True,
            False,
        ),
        (
            "after_update helper",
            lambda model, session: events.after_update(model),
            False,
            True,
            False,
        ),
        (
            "before_delete helper",
            events.before_delete,
            False,
            False,
            True,
        ),
        (
            "after_delete helper",
            lambda model, session: events.after_delete(model),
            False,
            False,
            True,
        ),
        (
            "before_save helper",
            events.before_save,
            True,
            True,
            False,
        ),
        (
            "after_save helper",
            lambda model, session: events.after_save(model),
            True,
            True,
            False,
        ),
        (
            "before_touch helper",
            events.before_touch,
            True,
            True,
            True,
        ),
        (
            "after_touch helper",
            lambda model, session: events.after_touch(model),
            True,
            True,
            True,
        ),
    ],
)
def test_basic_mapper_event_listeners(
    name,
    event_listener,
    called_after_created,
    called_after_updated,
    called_after_deleted,
    base,
    session,
    engine,
    mocker,
):
    class Foo(base):
        __tablename__ = "foos"
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String)

    def logit(log):
        return lambda *a: logger.debug(log)

    for evt in events.session_events:
        sa.event.listen(session, evt, logit(evt))
    for evt in events.mapper_events:
        sa.event.listen(sa.inspect(Foo), evt, logit(evt))

    callback = mocker.Mock()
    callback.__name__ = name + '_callback'
    event_listener = event_listener(Foo, session)
    event_listener(callback)

    base.metadata.create_all(engine)
    db = session()
    foo = Foo(name="foo")

    logger.debug('start insert')
    callback.reset_mock()
    db.add(foo)
    assert not callback.called, "not called before commit"
    db.commit()
    assert callback.called is called_after_created

    logger.debug('start update')
    callback.reset_mock()
    foo.name = "bar"
    assert not callback.called, "not called before commit"
    db.commit()
    assert callback.called is called_after_updated

    logger.debug('start delete')
    callback.reset_mock()
    db.delete(foo)
    assert not callback.called, "not called before commit"
    db.commit()
    assert callback.called is called_after_deleted
