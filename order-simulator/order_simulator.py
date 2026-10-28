import os
import time
import random
import uuid
from datetime import datetime

import pymysql
from pymysql import MySQLError as Error

from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))


# ------------------------------------------------------
# MySQL 설정
# ------------------------------------------------------

DB_HOST = os.getenv("DB_HOST", "mysql.default.svc.cluster.local")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "stud12")
DB_USER = os.getenv("DB_USER", "stud12")
DB_PASSWORD = os.getenv("DB_PASSWORD", "stud12")


# ------------------------------------------------------
# Simulator 설정
# ------------------------------------------------------

ORDER_INTERVAL = 10

# 3분마다 Phase 변경
PHASE_DURATION = 180


# ------------------------------------------------------
# 제품 정보
# ------------------------------------------------------

PRODUCTS = {

    "P001": {
        "name": "Laptop",
        "price": 1500000
    },

    "P002": {
        "name": "Monitor",
        "price": 400000
    },

    "P003": {
        "name": "Keyboard",
        "price": 100000
    },

    "P004": {
        "name": "Mouse",
        "price": 50000
    },

    "P005": {
        "name": "Server",
        "price": 5000000
    }
}


# ------------------------------------------------------
# 고객별 기본 주문 발생 확률
# ------------------------------------------------------

CUSTOMERS = [
    "C001",
    "C002",
    "C003",
    "C004",
    "C005"
]


CUSTOMER_WEIGHTS = [
    30,     # C001
    25,     # C002
    15,     # C003
    15,     # C004
    15      # C005
]


# ------------------------------------------------------
# DB 연결
# ------------------------------------------------------

def connect_db():

    while True:

        try:

            conn = pymysql.connect(

                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME

            )

            if conn.open:

                print("MySQL connected")

                return conn

        except Error as e:

            print("MySQL connection failed:", e)
            print("Retry after 5 seconds")

            time.sleep(5)


# ------------------------------------------------------
# 테이블 생성
# ------------------------------------------------------

def create_table(conn):

    sql = """

    CREATE TABLE IF NOT EXISTS orders (

        order_id VARCHAR(40) PRIMARY KEY,

        customer_id VARCHAR(10) NOT NULL,

        product_id VARCHAR(10) NOT NULL,

        quantity INT NOT NULL,

        unit_price INT NOT NULL,

        total_amount BIGINT NOT NULL,

        simulation_phase INT NOT NULL,

        order_time DATETIME NOT NULL

    )

    """

    cursor = conn.cursor()

    cursor.execute(sql)

    conn.commit()

    cursor.close()


# ------------------------------------------------------
# 현재 Phase 계산
# ------------------------------------------------------

def get_phase(start_time):

    elapsed = time.time() - start_time

    phase = int(elapsed // PHASE_DURATION) + 1

    # Phase는 1~3 반복
    phase = ((phase - 1) % 3) + 1

    return phase


# ------------------------------------------------------
# 고객별 주문 수량 생성
# ------------------------------------------------------

def generate_quantity(customer_id, phase):

    # C001 : 대량 고객
    if customer_id == "C001":

        return random.randint(5, 10)


    # C002 : 일반 고객
    elif customer_id == "C002":

        return random.randint(2, 6)


    # C003 : 소량 고객
    elif customer_id == "C003":

        return random.randint(1, 3)


    # C004 : 성장 고객
    elif customer_id == "C004":

        if phase == 1:

            return random.randint(2, 4)

        elif phase == 2:

            return random.randint(4, 7)

        else:

            return random.randint(7, 12)


    # C005 : 감소 고객
    elif customer_id == "C005":

        if phase == 1:

            return random.randint(7, 10)

        elif phase == 2:

            return random.randint(4, 6)

        else:

            return random.randint(1, 3)


    return 1


# ------------------------------------------------------
# 고객 선택
# ------------------------------------------------------

def select_customer(phase):

    weights = CUSTOMER_WEIGHTS.copy()

    # C004는 시간이 지나면서 주문 발생 빈도 증가
    if phase == 2:

        weights[3] = 25

    elif phase == 3:

        weights[3] = 35


    # C005는 시간이 지나면서 주문 발생 빈도 감소
    if phase == 2:

        weights[4] = 10

    elif phase == 3:

        weights[4] = 5


    customer = random.choices(
        CUSTOMERS,
        weights=weights,
        k=1
    )[0]

    return customer


# ------------------------------------------------------
# 제품 선택
# ------------------------------------------------------

def select_product(customer_id):

    # 고객마다 선호 제품도 약간 다르게

    if customer_id == "C001":

        products = [
            "P001",
            "P002",
            "P005"
        ]

    elif customer_id == "C002":

        products = [
            "P001",
            "P002",
            "P003",
            "P004"
        ]

    elif customer_id == "C003":

        products = [
            "P003",
            "P004"
        ]

    elif customer_id == "C004":

        products = [
            "P001",
            "P002",
            "P005"
        ]

    else:

        products = [
            "P001",
            "P002",
            "P003"
        ]

    return random.choice(products)


# ------------------------------------------------------
# 주문 생성
# ------------------------------------------------------

def generate_order(phase):

    customer_id = select_customer(phase)

    product_id = select_product(customer_id)

    quantity = generate_quantity(
        customer_id,
        phase
    )

    unit_price = PRODUCTS[
        product_id
    ]["price"]

    total_amount = (
        quantity * unit_price
    )

    order_id = (
        "ORD-" +
        str(uuid.uuid4())[:8]
    )

    return {

        "order_id": order_id,

        "customer_id": customer_id,

        "product_id": product_id,

        "quantity": quantity,

        "unit_price": unit_price,

        "total_amount": total_amount,

        "simulation_phase": phase,

        "order_time": datetime.now(KST).replace(tzinfo=None)
    }


# ------------------------------------------------------
# MySQL 저장
# ------------------------------------------------------

def insert_order(conn, order):

    sql = """

    INSERT INTO orders (

        order_id,

        customer_id,

        product_id,

        quantity,

        unit_price,

        total_amount,

        simulation_phase,

        order_time

    )

    VALUES (

        %s, %s, %s, %s,
        %s, %s, %s, %s

    )

    """

    cursor = conn.cursor()

    cursor.execute(

        sql,

        (

            order["order_id"],

            order["customer_id"],

            order["product_id"],

            order["quantity"],

            order["unit_price"],

            order["total_amount"],

            order["simulation_phase"],

            order["order_time"]

        )

    )

    conn.commit()

    cursor.close()


# ------------------------------------------------------
# Main
# ------------------------------------------------------

def main():

    print("================================")
    print(" Order Simulator")
    print("================================")

    conn = connect_db()

    create_table(conn)

    start_time = time.time()

    previous_phase = None


    while True:

        try:

            phase = get_phase(
                start_time
            )


            if phase != previous_phase:

                print()
                print(
                    f"===== PHASE {phase} START ====="
                )
                print()

                previous_phase = phase


            order = generate_order(
                phase
            )


            insert_order(
                conn,
                order
            )


            print(

                f'{order["order_time"]} | '

                f'Phase={phase} | '

                f'{order["order_id"]} | '

                f'Customer={order["customer_id"]} | '

                f'Product={order["product_id"]} | '

                f'Qty={order["quantity"]} | '

                f'Amount={order["total_amount"]:,}'

            )


            time.sleep(
                ORDER_INTERVAL
            )


        except Exception as e:

            print(
                "Error:",
                e
            )

            try:

                conn.close()

            except:

                pass

            conn = connect_db()


if __name__ == "__main__":

    main()