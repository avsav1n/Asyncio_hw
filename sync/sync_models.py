from contextlib import contextmanager

import sqlalchemy as sq
import sync_config as cfg
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

DSN = (
    f"postgresql+psycopg2://{cfg.POSTGRES_USER}:"
    f"{cfg.POSTGRES_PASSWORD}@{cfg.POSTGRES_HOST}:"
    f"{cfg.POSTGRES_PORT}/{cfg.POSTGRES_DB}"
)

engine = sq.create_engine(url=DSN)
Session = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


class SyncPerson(Base):
    __tablename__ = "syncperson"

    id: Mapped[int] = mapped_column(sq.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sq.String(100), unique=True, nullable=True)
    birth_year: Mapped[str] = mapped_column(sq.String(50), nullable=True)
    gender: Mapped[str] = mapped_column(sq.String(50), nullable=True)
    homeworld: Mapped[str] = mapped_column(sq.String(100), nullable=True)
    height: Mapped[str] = mapped_column(sq.String, nullable=True)
    mass: Mapped[str] = mapped_column(sq.String, nullable=True)
    skin_color: Mapped[str] = mapped_column(sq.String(50), nullable=True)
    hair_color: Mapped[str] = mapped_column(sq.String(50), nullable=True)
    eye_color: Mapped[str] = mapped_column(sq.String(50), nullable=True)

    films: Mapped[str] = mapped_column(sq.Text, nullable=True)
    species: Mapped[str] = mapped_column(sq.Text, nullable=True)
    starships: Mapped[str] = mapped_column(sq.Text, nullable=True)
    vehicles: Mapped[str] = mapped_column(sq.Text, nullable=True)

    def __init__(self, **kwargs):
        valid_atts = kwargs.copy()
        for attr, value in kwargs.items():
            if attr not in self.__table__.columns:
                valid_atts.pop(attr, None)
                continue
            if not value or value in ("none", "unknown", "n/a"):
                valid_atts[attr] = None
        super().__init__(**valid_atts)


@contextmanager
def refresh_db_state():
    """Функция обновления состояния базы данных.
    Возможна работа как в качестве контекстного менеджера, так и декоратора.
    """
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    engine.dispose()
