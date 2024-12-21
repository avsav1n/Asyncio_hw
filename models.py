from contextlib import asynccontextmanager

import sqlalchemy as sq
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import config as cfg

DSN = (
    f"postgresql+asyncpg://{cfg.POSTGRES_USER}:"
    f"{cfg.POSTGRES_PASSWORD}@{cfg.POSTGRES_HOST}:"
    f"{cfg.POSTGRES_PORT}/{cfg.POSTGRES_DB}"
)

engine = create_async_engine(url=DSN)
Session = async_sessionmaker(bind=engine, expire_on_commit=False)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Person(Base):
    __tablename__ = "person"

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


@asynccontextmanager
async def refresh_db_state():
    """Функция обновления состояния базы данных.
    Возможна работа как в качестве контекстного менеджера, так и декоратора.
    """
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()
