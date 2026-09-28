import sqlalchemy as sa
engine = sa.create_engine("postgresql://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/postgres", isolation_level="AUTOCOMMIT")
try:
    engine.execute("CREATE DATABASE uip_test_1")
    print("Database uip_test_1 created!")
except Exception as e:
    print(f"Error: {e}")
