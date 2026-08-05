"""Pydantic schema contracts for Bronze → Silver validation.

All models use extra='allow' to support schema evolution — new columns from
upstream are passed through rather than blocked. Pydantic's role here is
data quality validation (values, types, required fields), not structural
gatekeeping (that is handled by check_schema / the schema registry).
"""
from __future__ import annotations
from datetime import date
from typing import Optional
from pydantic import BaseModel, field_validator, ConfigDict


class OrderRow(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=False, extra="allow")

    order_id: str
    customer_id: str
    product_id: str
    order_date: date
    quantity: int
    unit_price: float
    status: str

    @field_validator("quantity")
    @classmethod
    def qty_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"quantity must be > 0, got {v}")
        return v

    @field_validator("unit_price")
    @classmethod
    def price_positive(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"unit_price must be >= 0, got {v}")
        return v

    @field_validator("status")
    @classmethod
    def valid_status(cls, v: str) -> str:
        allowed = {"pending", "shipped", "delivered", "cancelled", "returned"}
        if v.lower() not in allowed:
            raise ValueError(f"status '{v}' not in {allowed}")
        return v.lower()


class CustomerRow(BaseModel):
    model_config = ConfigDict(extra="allow")

    customer_id: str
    first_name: str
    last_name: str
    email: str
    city: str
    country: str
    signup_date: date
    tier: Optional[str] = "standard"

    @field_validator("email")
    @classmethod
    def email_has_at(cls, v: str) -> str:
        if "@" not in v:
            raise ValueError(f"invalid email: {v}")
        return v.lower()


class ProductRow(BaseModel):
    model_config = ConfigDict(extra="allow")

    product_id: str
    name: str
    category: str
    unit_cost: float
    supplier_id: str
    updated_at: str  # ISO string from SQLite

    @field_validator("unit_cost")
    @classmethod
    def cost_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"unit_cost must be >= 0, got {v}")
        return v