from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

# Создаем базовый класс для моделей
Base = declarative_base()

class Bulletin(Base):
    __tablename__ = "bulletin"

    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String)
    exchange_product_name = Column(String)
    delivery_basis_name = Column(String)
    volume = Column(Integer)
    total = Column(Integer)
    count = Column(Integer)