from sqlalchemy import (
    Column,
    Date,
    DateTime,
    CheckConstraint,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

# Создаем базовый класс для моделей
Base = declarative_base()


class Bulletin(Base):
    __tablename__ = "bulletin"

    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String, nullable=False)
    exchange_product_name = Column(String, nullable=False)
    oil_id = Column(String, nullable=False)
    delivery_basis_id = Column(String, nullable=False)
    delivery_basis_name = Column(String, nullable=False)
    delivery_type_id = Column(String, nullable=False)
    volume = Column(
        Integer,
        CheckConstraint("volume >= 0"),
        nullable=False,
    )
    total = Column(Integer, CheckConstraint("total >= 0"), nullable=False)
    count = Column(Integer, CheckConstraint("count >= 0"), nullable=False)
    date = Column(Date, nullable=False)
    created_on = Column(DateTime, server_default=func.now(), nullable=False)
    updated_on = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint(
            "exchange_product_id", "date", name="uix_exchange_product_id_data"
        ),
    )
