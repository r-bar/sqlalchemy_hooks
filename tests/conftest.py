import pytest
import sqlalchemy as sa
import sqlalchemy.orm


@pytest.fixture
def engine():
    return sa.create_engine('sqlite://')


@pytest.fixture
def base(engine):
    return sa.orm.declarative_base(bind=engine)


@pytest.fixture
def session(engine):
    return sa.orm.sessionmaker(engine)
