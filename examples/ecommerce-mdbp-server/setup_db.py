"""
E-Commerce Database Setup

Creates tables and seeds sample data.
Uses SQLite by default, set DATABASE_URL for PostgreSQL.

Run:
  python setup_db.py
"""

import os

from sqlalchemy import (
    Column, DateTime, ForeignKey, Integer, MetaData, Numeric, String, Table,
    Text, create_engine, insert, func,
)

_DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_URL = os.environ.get("DATABASE_URL", f"sqlite:///{os.path.join(_DB_DIR, 'ecommerce.db')}")


def create_tables(engine):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    metadata.drop_all(bind=engine)
    metadata = MetaData()

    Table("products", metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
        Column("description", Text),
        Column("price", Numeric(10, 2), nullable=False),
        Column("category", String, nullable=False),
        Column("stock", Integer, nullable=False, default=0),
        Column("created_at", DateTime, server_default=func.now()),
    )

    Table("customers", metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
        Column("email", String, nullable=False, unique=True),
        Column("phone", String),
        Column("city", String),
        Column("tier", String, default="standard"),
        Column("created_at", DateTime, server_default=func.now()),
    )

    Table("orders", metadata,
        Column("id", Integer, primary_key=True),
        Column("customer_id", Integer, ForeignKey("customers.id"), nullable=False),
        Column("product_id", Integer, ForeignKey("products.id"), nullable=False),
        Column("quantity", Integer, nullable=False, default=1),
        Column("total", Numeric(10, 2), nullable=False),
        Column("status", String, nullable=False),
        Column("created_at", DateTime, server_default=func.now()),
    )

    Table("reviews", metadata,
        Column("id", Integer, primary_key=True),
        Column("product_id", Integer, ForeignKey("products.id"), nullable=False),
        Column("customer_id", Integer, ForeignKey("customers.id"), nullable=False),
        Column("rating", Integer, nullable=False),
        Column("comment", Text),
        Column("created_at", DateTime, server_default=func.now()),
    )

    metadata.create_all(engine)
    return metadata


def seed_data(engine, metadata):
    products = [
        {"id": 1, "name": "Wireless Headphones", "description": "Bluetooth over-ear headphones with noise cancellation", "price": 79.99, "category": "electronics", "stock": 150},
        {"id": 2, "name": "USB-C Hub", "description": "7-in-1 USB-C hub with HDMI, USB-A, and SD card", "price": 34.99, "category": "electronics", "stock": 300},
        {"id": 3, "name": "Mechanical Keyboard", "description": "RGB mechanical keyboard with Cherry MX switches", "price": 129.99, "category": "electronics", "stock": 85},
        {"id": 4, "name": "Ergonomic Mouse", "description": "Vertical ergonomic wireless mouse", "price": 49.99, "category": "electronics", "stock": 200},
        {"id": 5, "name": "Standing Desk", "description": "Electric height-adjustable standing desk 160x80cm", "price": 449.99, "category": "furniture", "stock": 25},
        {"id": 6, "name": "Office Chair", "description": "Ergonomic mesh office chair with lumbar support", "price": 299.99, "category": "furniture", "stock": 40},
        {"id": 7, "name": "Desk Lamp", "description": "LED desk lamp with adjustable color temperature", "price": 39.99, "category": "lighting", "stock": 120},
        {"id": 8, "name": "Monitor Light Bar", "description": "Screen-mounted LED light bar for reducing eye strain", "price": 54.99, "category": "lighting", "stock": 90},
        {"id": 9, "name": "Laptop Stand", "description": "Aluminum laptop stand with adjustable height", "price": 27.99, "category": "accessories", "stock": 175},
        {"id": 10, "name": "Cable Management Kit", "description": "Under-desk cable management tray and clips", "price": 19.99, "category": "accessories", "stock": 250},
        {"id": 11, "name": "Webcam HD", "description": "1080p webcam with built-in microphone", "price": 69.99, "category": "electronics", "stock": 110},
        {"id": 12, "name": "Notebook Set", "description": "Premium hardcover notebook set, 3 pack", "price": 12.99, "category": "stationery", "stock": 500},
    ]

    customers = [
        {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "phone": "555-0101", "city": "New York", "tier": "vip"},
        {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "phone": "555-0102", "city": "San Francisco", "tier": "premium"},
        {"id": 3, "name": "Carol Davis", "email": "carol@example.com", "phone": "555-0103", "city": "Chicago", "tier": "standard"},
        {"id": 4, "name": "David Wilson", "email": "david@example.com", "phone": "555-0104", "city": "Austin", "tier": "standard"},
        {"id": 5, "name": "Eve Martinez", "email": "eve@example.com", "phone": "555-0105", "city": "Seattle", "tier": "premium"},
        {"id": 6, "name": "Frank Brown", "email": "frank@example.com", "phone": "555-0106", "city": "Denver", "tier": "standard"},
        {"id": 7, "name": "Grace Lee", "email": "grace@example.com", "phone": "555-0107", "city": "Boston", "tier": "vip"},
        {"id": 8, "name": "Henry Taylor", "email": "henry@example.com", "phone": "555-0108", "city": "Portland", "tier": "standard"},
    ]

    orders = [
        {"id": 1, "customer_id": 1, "product_id": 1, "quantity": 2, "total": 159.98, "status": "delivered"},
        {"id": 2, "customer_id": 1, "product_id": 5, "quantity": 1, "total": 449.99, "status": "delivered"},
        {"id": 3, "customer_id": 2, "product_id": 3, "quantity": 1, "total": 129.99, "status": "shipped"},
        {"id": 4, "customer_id": 2, "product_id": 4, "quantity": 2, "total": 99.98, "status": "delivered"},
        {"id": 5, "customer_id": 3, "product_id": 7, "quantity": 3, "total": 119.97, "status": "pending"},
        {"id": 6, "customer_id": 3, "product_id": 12, "quantity": 5, "total": 64.95, "status": "delivered"},
        {"id": 7, "customer_id": 4, "product_id": 2, "quantity": 1, "total": 34.99, "status": "shipped"},
        {"id": 8, "customer_id": 5, "product_id": 6, "quantity": 1, "total": 299.99, "status": "delivered"},
        {"id": 9, "customer_id": 5, "product_id": 8, "quantity": 2, "total": 109.98, "status": "pending"},
        {"id": 10, "customer_id": 6, "product_id": 9, "quantity": 1, "total": 27.99, "status": "cancelled"},
        {"id": 11, "customer_id": 7, "product_id": 1, "quantity": 1, "total": 79.99, "status": "delivered"},
        {"id": 12, "customer_id": 7, "product_id": 3, "quantity": 1, "total": 129.99, "status": "shipped"},
        {"id": 13, "customer_id": 7, "product_id": 10, "quantity": 4, "total": 79.96, "status": "delivered"},
        {"id": 14, "customer_id": 8, "product_id": 11, "quantity": 1, "total": 69.99, "status": "pending"},
        {"id": 15, "customer_id": 1, "product_id": 4, "quantity": 1, "total": 49.99, "status": "shipped"},
    ]

    reviews = [
        {"id": 1, "product_id": 1, "customer_id": 1, "rating": 5, "comment": "Amazing sound quality, very comfortable"},
        {"id": 2, "product_id": 1, "customer_id": 7, "rating": 4, "comment": "Great headphones, battery could be better"},
        {"id": 3, "product_id": 3, "customer_id": 2, "rating": 5, "comment": "Best keyboard I've ever used"},
        {"id": 4, "product_id": 5, "customer_id": 1, "rating": 4, "comment": "Sturdy desk, easy to assemble"},
        {"id": 5, "product_id": 6, "customer_id": 5, "rating": 5, "comment": "Very comfortable for long hours"},
        {"id": 6, "product_id": 4, "customer_id": 2, "rating": 3, "comment": "Decent mouse, scroll wheel is a bit stiff"},
        {"id": 7, "product_id": 7, "customer_id": 3, "rating": 4, "comment": "Nice warm light, good for late nights"},
        {"id": 8, "product_id": 12, "customer_id": 3, "rating": 5, "comment": "Great quality paper"},
        {"id": 9, "product_id": 2, "customer_id": 4, "rating": 4, "comment": "Works well with my MacBook"},
        {"id": 10, "product_id": 11, "customer_id": 8, "rating": 3, "comment": "Image quality is okay, not great in low light"},
    ]

    with engine.connect() as conn:
        conn.execute(insert(metadata.tables["products"]).values(products))
        conn.execute(insert(metadata.tables["customers"]).values(customers))
        conn.execute(insert(metadata.tables["orders"]).values(orders))
        conn.execute(insert(metadata.tables["reviews"]).values(reviews))
        conn.commit()


if __name__ == "__main__":
    engine = create_engine(DB_URL)
    metadata = create_tables(engine)
    seed_data(engine, metadata)
    engine.dispose()
    print(f"Database ready: {DB_URL}")
    print("  products:  12 rows")
    print("  customers:  8 rows")
    print("  orders:    15 rows")
    print("  reviews:   10 rows")
