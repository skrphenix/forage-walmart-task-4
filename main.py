import csv
import sqlite3

from os import PathLike
from typing import TypeVar, Union, Optional, List, Any

Path = TypeVar("Path", str, bytes, PathLike[str], PathLike[bytes])
_T = List[List[Any]]


class Database:
    def __init__(self, db_file: Path):
        self.db_file = db_file
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        try:
            self._conn = sqlite3.connect(self.db_file)
        except sqlite3.Error:
            pass

    @property
    def is_connected(self) -> bool:
        return self._conn is not None

    def __check_connection(self) -> None:
        if not self.is_connected:
            raise RuntimeError("Database not connected")

    def populate(self, spreadsheet_dir: Path):
        self.__check_connection()

        with open(f"{spreadsheet_dir}/shipping_data_0.csv", encoding="utf-8") as csc_file:
            data0 = [row for row in csv.reader(csc_file)]
        with open(f"{spreadsheet_dir}/shipping_data_1.csv", encoding="utf-8") as csc_file:
            data1 = [row for row in csv.reader(csc_file)]
        with open(f"{spreadsheet_dir}/shipping_data_2.csv", encoding="utf-8") as csc_file:
            data2 = [row for row in csv.reader(csc_file)]

        self.populate_first_shipping(data0[1:])
        self.populate_second_shipping_data(data1[1:], data2[1:])

    def populate_first_shipping(self, data: _T):
        self.__check_connection()

        for row in data:
            origin = row[0]
            destination = row[1]
            product_name = row[2]
            product_quantity = row[4]

            self.insert_product(product_name)
            self.insert_shipment(product_name, product_quantity, origin, destination)

    def populate_second_shipping_data(self, data1: _T, data2: _T):
        self.__check_connection()

        shipment = {}
        for row in data2:
            shipment_id = row[0]
            origin = row[1]
            destination = row[2]

            shipment[shipment_id] = {
                "origin": origin,
                "destination": destination,
                "products": {}
            }

        for row in data1:
            shipment_id = row[0]
            product_name = row[1]

            products = shipment[shipment_id]["products"]
            if products.get(product_name) is None:
                products[product_name] = 1
            else:
                products[product_name] += 1

        for shipment_id, _shipment in shipment.items():
            origin = shipment[shipment_id]["origin"]
            destination = shipment[shipment_id]["destination"]

            for product_name, product_quantity in _shipment["products"].items():
                self.insert_product(product_name)
                self.insert_shipment(product_name, product_quantity, origin, destination)

    def insert_product(self, product_name: str):
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO product (name) VALUES (?)",
            (product_name,)
        )
        self._conn.commit()

        cursor.close()

    def insert_shipment(self, product_name, product_quantity, origin, destination):
        cursor = self._conn.cursor()

        cursor.execute("SELECT id FROM product WHERE product.name = ?", (product_name,))
        product_id = cursor.fetchone()[0]

        query = """
        INSERT OR IGNORE INTO shipment (product_id, quantity, origin, destination)
        VALUES (?, ?, ?, ?);
        """
        cursor.execute(query, (product_id, product_quantity, origin, destination))
        self._conn.commit()

        cursor.close()

    def close(self):
        self._conn.close()


if __name__ == '__main__':
    # For testing
    db = Database("shipment_database.db")
    db.connect()

    # populating csv to database
    db.populate("./data")

    db.close()
