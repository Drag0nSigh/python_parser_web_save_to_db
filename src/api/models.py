from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class BulletinModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: Optional[int]
    exchange_product_id: Optional[str]
    exchange_product_name: Optional[str]
    oil_id: Optional[str]
    delivery_basis_id: Optional[str]
    delivery_basis_name: Optional[str]
    delivery_type_id: Optional[str]
    volume: Optional[int]
    total: Optional[int]
    count: Optional[int]
    date: Optional[date]
    created_on: Optional[datetime]
    updated_on: Optional[datetime]


__all__ = ["BulletinModel"]
