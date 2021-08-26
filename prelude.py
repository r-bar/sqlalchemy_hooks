import sqlalchemy as sa
import sqlalchemy.orm
from sqlalchemy_hooks import events

engine = sa.create_engine('sqlite://')
SessionLocal = sa.orm.sessionmaker(bind=engine)
Base = sa.orm.declarative_base()
db: sqlalchemy.orm.Session = SessionLocal()


def repr_helper(*attrs):
    def _repr(self):
        clsname = type(self).__name__
        attr_repr = ', '.join(f'{attr}={getattr(self, attr)!r}' for attr in attrs)
        return f'{clsname}({attr_repr})'
    return _repr


class Foo(Base):
    __tablename__ = 'foofoos'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)

    __repr__ = repr_helper('id', 'name')


Base.metadata.create_all(engine)
a, b, c = [Foo(name=l) for l in 'abc']
db.add(a)
db.add(b)
db.add(c)
db.commit()
