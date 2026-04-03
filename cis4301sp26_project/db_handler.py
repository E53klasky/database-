from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])

cur = conn.cursor()


def _get_cur():
    """Returns a valid cursor, reconnecting if the connection was closed."""
    global conn, cur
    try:
        conn.ping()
    except Exception:
        conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"],
                       host=DB_CONFIG["host"], database=DB_CONFIG["database"],
                       port=DB_CONFIG["port"])
        cur = conn.cursor()
    return cur


def add_item(new_item: Item = None):
    cur = _get_cur()
    cur.execute("SELECT MAX(i_item_sk) FROM item")
    row = cur.fetchone()
    new_sk = (row[0] or 0) + 1

    rec_start_date = f"{new_item.start_year}-01-01"

    cur = _get_cur()
    cur.execute("""
        INSERT INTO item (
            i_item_sk, i_item_id, i_rec_start_date,
            i_product_name, i_brand, i_class, i_category,
            i_manufact, i_current_price, i_num_owned
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        new_sk,
        new_item.item_id,
        rec_start_date,
        new_item.product_name,
        new_item.brand,
        None,           # i_class – not in model, set NULL
        new_item.category,
        new_item.manufact,
        new_item.current_price,
        new_item.num_owned,
    ))


def add_customer(new_customer: Customer = None):

    street_part, city_part, state_zip_part = new_customer.address.split(",")
    street_part = street_part.strip()
    city = city_part.strip()
    state_zip = state_zip_part.strip().split()
    state = state_zip[0]
    zip_code = state_zip[1] if len(state_zip) > 1 else ""

    street_tokens = street_part.split(" ", 1)
    street_number = street_tokens[0]
    street_name = street_tokens[1] if len(street_tokens) > 1 else ""

    cur = _get_cur()
    cur.execute("SELECT MAX(ca_address_sk) FROM customer_address")
    row = cur.fetchone()
    new_addr_sk = (row[0] or 0) + 1

    cur = _get_cur()
    cur.execute("""
        INSERT INTO customer_address (
            ca_address_sk, ca_street_number, ca_street_name,
            ca_city, ca_state, ca_zip
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (new_addr_sk, street_number, street_name, city, state, zip_code))

    name_tokens = new_customer.name.split(" ", 1)
    first_name = name_tokens[0]
    last_name = name_tokens[1] if len(name_tokens) > 1 else ""

    cur = _get_cur()
    cur.execute("SELECT MAX(c_customer_sk) FROM customer")
    row = cur.fetchone()
    new_cust_sk = (row[0] or 0) + 1

    cur = _get_cur()
    cur.execute("""
        INSERT INTO customer (
            c_customer_sk, c_customer_id,
            c_first_name, c_last_name,
            c_email_address, c_current_addr_sk
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        new_cust_sk,
        new_customer.customer_id,
        first_name,
        last_name,
        new_customer.email,
        new_addr_sk,
    ))


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):

    cur = _get_cur()
    cur.execute("""
        SELECT c_customer_sk, c_current_addr_sk
        FROM customer
        WHERE c_customer_id = ?
    """, (original_customer_id,))
    row = cur.fetchone()
    if row is None:
        return
    cust_sk, addr_sk = row

    customer_updates = []
    customer_params = []

    if new_customer.customer_id is not None:
        customer_updates.append("c_customer_id = ?")
        customer_params.append(new_customer.customer_id)

    if new_customer.name is not None:
        name_tokens = new_customer.name.split(" ", 1)
        customer_updates.append("c_first_name = ?")
        customer_params.append(name_tokens[0])
        customer_updates.append("c_last_name = ?")
        customer_params.append(name_tokens[1] if len(name_tokens) > 1 else "")

    if new_customer.email is not None:
        customer_updates.append("c_email_address = ?")
        customer_params.append(new_customer.email)

    if customer_updates:
        customer_params.append(cust_sk)
        cur.execute(
            f"UPDATE customer SET {', '.join(customer_updates)} WHERE c_customer_sk = ?",
            customer_params
        )

    if new_customer.address is not None:
        street_part, city_part, state_zip_part = new_customer.address.split(",")
        street_part = street_part.strip()
        city = city_part.strip()
        state_zip = state_zip_part.strip().split()
        state = state_zip[0]
        zip_code = state_zip[1] if len(state_zip) > 1 else ""
        street_tokens = street_part.split(" ", 1)
        street_number = street_tokens[0]
        street_name = street_tokens[1] if len(street_tokens) > 1 else ""

        cur.execute("""
            UPDATE customer_address
            SET ca_street_number = ?,
                ca_street_name   = ?,
                ca_city          = ?,
                ca_state         = ?,
                ca_zip           = ?
            WHERE ca_address_sk = ?
        """, (street_number, street_name, city, state, zip_code, addr_sk))


def rent_item(item_id: str = None, customer_id: str = None):
    today = date.today()
    due = today + timedelta(days=14)
    cur = _get_cur()
    cur.execute("""
        INSERT INTO rental (item_id, customer_id, rental_date, due_date)
        VALUES (?, ?, ?, ?)
    """, (item_id, customer_id, str(today), str(due)))


def return_item(item_id: str = None, customer_id: str = None):
    cur = _get_cur()
    cur.execute("""
        SELECT rental_date, due_date FROM rental
        WHERE item_id = ? AND customer_id = ?
    """, (item_id, customer_id))
    row = cur.fetchone()
    if row is None:
        return
    rental_date, due_date = row

    cur = _get_cur()
    cur.execute("""
        INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date)
        VALUES (?, ?, ?, ?, ?)
    """, (item_id, customer_id, str(rental_date), str(due_date), str(date.today())))

    cur = _get_cur()
    cur.execute("""
        DELETE FROM rental WHERE item_id = ? AND customer_id = ?
    """, (item_id, customer_id))


def grant_extension(item_id: str = None, customer_id: str = None):
    cur = _get_cur()
    cur.execute("""
        UPDATE rental
        SET due_date = DATE_ADD(due_date, INTERVAL 14 DAY)
        WHERE item_id = ? AND customer_id = ?
    """, (item_id, customer_id))



def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:

    new_place = line_length(item_id) + 1
    cur = _get_cur()
    cur.execute("""
        INSERT INTO waitlist (item_id, customer_id, place_in_line)
        VALUES (?, ?, ?)
    """, (item_id, customer_id, new_place))
    return new_place


def update_waitlist(item_id: str = None):
    cur = _get_cur()
    cur.execute("""
        DELETE FROM waitlist WHERE item_id = ? AND place_in_line = 1
    """, (item_id,))

    cur = _get_cur()
    cur.execute("""
        UPDATE waitlist SET place_in_line = place_in_line - 1
        WHERE item_id = ?
    """, (item_id,))




def number_in_stock(item_id: str = None) -> int:
    cur = _get_cur()
    cur.execute("SELECT i_num_owned FROM item WHERE i_item_id = ?", (item_id,))
    row = cur.fetchone()
    if row is None:
        return -1
    num_owned = row[0]

    cur = _get_cur()
    cur.execute("SELECT COUNT(*) FROM rental WHERE item_id = ?", (item_id,))
    active = cur.fetchone()[0]
    return num_owned - active


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    cur = _get_cur()
    cur.execute("""
        SELECT place_in_line FROM waitlist
        WHERE item_id = ? AND customer_id = ?
    """, (item_id, customer_id))
    row = cur.fetchone()
    return row[0] if row else -1


def line_length(item_id: str = None) -> int:
    cur = _get_cur()
    cur.execute("SELECT COUNT(*) FROM waitlist WHERE item_id = ?", (item_id,))
    return cur.fetchone()[0]


def _build_where(conditions: list, params: list) -> str:
    if not conditions:
        return ""
    return " WHERE " + " AND ".join(conditions)


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    conditions = []
    params = []
    op = "LIKE" if use_patterns else "="

    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            conditions.append(f"i_item_id {op} ?")
            params.append(filter_attributes.item_id)
        if filter_attributes.product_name is not None:
            conditions.append(f"i_product_name {op} ?")
            params.append(filter_attributes.product_name)
        if filter_attributes.brand is not None:
            conditions.append(f"i_brand {op} ?")
            params.append(filter_attributes.brand)
        if filter_attributes.category is not None:
            conditions.append(f"i_category {op} ?")
            params.append(filter_attributes.category)
        if filter_attributes.manufact is not None:
            conditions.append(f"i_manufact {op} ?")
            params.append(filter_attributes.manufact)
        if filter_attributes.current_price is not None and filter_attributes.current_price != -1:
            conditions.append("i_current_price = ?")
            params.append(filter_attributes.current_price)
        if filter_attributes.start_year is not None and filter_attributes.start_year != -1:
            conditions.append("YEAR(i_rec_start_date) = ?")
            params.append(filter_attributes.start_year)
        if filter_attributes.num_owned is not None and filter_attributes.num_owned != -1:
            conditions.append("i_num_owned = ?")
            params.append(filter_attributes.num_owned)

    if min_price != -1:
        conditions.append("i_current_price >= ?")
        params.append(min_price)
    if max_price != -1:
        conditions.append("i_current_price <= ?")
        params.append(max_price)
    if min_start_year != -1:
        conditions.append("YEAR(i_rec_start_date) >= ?")
        params.append(min_start_year)
    if max_start_year != -1:
        conditions.append("YEAR(i_rec_start_date) <= ?")
        params.append(max_start_year)

    sql = ("SELECT i_item_id, i_product_name, i_brand, i_category, i_manufact, "
           "i_current_price, YEAR(i_rec_start_date), i_num_owned FROM item"
           + _build_where(conditions, params))
    cur = _get_cur()
    cur.execute(sql, params)
    results = []
    for row in cur.fetchall():
        results.append(Item(
            item_id=row[0].strip() if row[0] else row[0],
            product_name=row[1].strip() if row[1] else row[1],
            brand=row[2].strip() if row[2] else row[2],
            category=row[3].strip() if row[3] else row[3],
            manufact=row[4].strip() if row[4] else row[4],
            current_price=float(row[5]) if row[5] is not None else None,
            start_year=row[6],
            num_owned=row[7],
        ))
    return results


def get_filtered_customers(filter_attributes: Customer = None,
                            use_patterns: bool = False) -> list[Customer]:
    conditions = []
    params = []
    op = "LIKE" if use_patterns else "="

    if filter_attributes is not None:
        if filter_attributes.customer_id is not None:
            conditions.append(f"c.c_customer_id {op} ?")
            params.append(filter_attributes.customer_id)
        if filter_attributes.name is not None:
            conditions.append(f"CONCAT(c.c_first_name, ' ', c.c_last_name) {op} ?")
            params.append(filter_attributes.name)
        if filter_attributes.email is not None:
            conditions.append(f"c.c_email_address {op} ?")
            params.append(filter_attributes.email)
        if filter_attributes.address is not None:
            conditions.append(
                f"CONCAT(ca.ca_street_number, ' ', ca.ca_street_name, ', ', "
                f"ca.ca_city, ', ', ca.ca_state, ' ', ca.ca_zip) {op} ?"
            )
            params.append(filter_attributes.address)

    sql = ("""
        SELECT c.c_customer_id,
               CONCAT(c.c_first_name, ' ', c.c_last_name),
               CONCAT(ca.ca_street_number, ' ', ca.ca_street_name, ', ',
                      ca.ca_city, ', ', ca.ca_state, ' ', ca.ca_zip),
               c.c_email_address
        FROM customer c
        JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    """ + _build_where(conditions, params))

    cur = _get_cur()
    cur.execute(sql, params)
    results = []
    for row in cur.fetchall():
        results.append(Customer(
            customer_id=row[0].strip() if row[0] else row[0],
            name=row[1].strip() if row[1] else row[1],
            address=row[2].strip() if row[2] else row[2],
            email=row[3].strip() if row[3] else row[3],
        ))
    return results


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    conditions = []
    params = []

    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            conditions.append("item_id = ?")
            params.append(filter_attributes.item_id)
        if filter_attributes.customer_id is not None:
            conditions.append("customer_id = ?")
            params.append(filter_attributes.customer_id)
        if filter_attributes.rental_date is not None:
            conditions.append("rental_date = ?")
            params.append(filter_attributes.rental_date)
        if filter_attributes.due_date is not None:
            conditions.append("due_date = ?")
            params.append(filter_attributes.due_date)

    if min_rental_date is not None:
        conditions.append("rental_date >= ?")
        params.append(min_rental_date)
    if max_rental_date is not None:
        conditions.append("rental_date <= ?")
        params.append(max_rental_date)
    if min_due_date is not None:
        conditions.append("due_date >= ?")
        params.append(min_due_date)
    if max_due_date is not None:
        conditions.append("due_date <= ?")
        params.append(max_due_date)

    sql = ("SELECT item_id, customer_id, rental_date, due_date FROM rental"
           + _build_where(conditions, params))
    cur = _get_cur()
    cur.execute(sql, params)
    results = []
    for row in cur.fetchall():
        results.append(Rental(
            item_id=row[0].strip() if row[0] else row[0],
            customer_id=row[1].strip() if row[1] else row[1],
            rental_date=str(row[2]),
            due_date=str(row[3]),
        ))
    return results


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                   min_rental_date: str = None,
                                   max_rental_date: str = None,
                                   min_due_date: str = None,
                                   max_due_date: str = None,
                                   min_return_date: str = None,
                                   max_return_date: str = None) -> list[RentalHistory]:
    conditions = []
    params = []

    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            conditions.append("item_id = ?")
            params.append(filter_attributes.item_id)
        if filter_attributes.customer_id is not None:
            conditions.append("customer_id = ?")
            params.append(filter_attributes.customer_id)
        if filter_attributes.rental_date is not None:
            conditions.append("rental_date = ?")
            params.append(filter_attributes.rental_date)
        if filter_attributes.due_date is not None:
            conditions.append("due_date = ?")
            params.append(filter_attributes.due_date)
        if filter_attributes.return_date is not None:
            conditions.append("return_date = ?")
            params.append(filter_attributes.return_date)

    if min_rental_date is not None:
        conditions.append("rental_date >= ?")
        params.append(min_rental_date)
    if max_rental_date is not None:
        conditions.append("rental_date <= ?")
        params.append(max_rental_date)
    if min_due_date is not None:
        conditions.append("due_date >= ?")
        params.append(min_due_date)
    if max_due_date is not None:
        conditions.append("due_date <= ?")
        params.append(max_due_date)
    if min_return_date is not None:
        conditions.append("return_date >= ?")
        params.append(min_return_date)
    if max_return_date is not None:
        conditions.append("return_date <= ?")
        params.append(max_return_date)

    sql = ("SELECT item_id, customer_id, rental_date, due_date, return_date FROM rental_history"
           + _build_where(conditions, params))
    cur = _get_cur()
    cur.execute(sql, params)
    results = []
    for row in cur.fetchall():
        results.append(RentalHistory(
            item_id=row[0].strip() if row[0] else row[0],
            customer_id=row[1].strip() if row[1] else row[1],
            rental_date=str(row[2]),
            due_date=str(row[3]),
            return_date=str(row[4]),
        ))
    return results


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                           min_place_in_line: int = -1,
                           max_place_in_line: int = -1) -> list[Waitlist]:
    conditions = []
    params = []

    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            conditions.append("item_id = ?")
            params.append(filter_attributes.item_id)
        if filter_attributes.customer_id is not None:
            conditions.append("customer_id = ?")
            params.append(filter_attributes.customer_id)
        if filter_attributes.place_in_line is not None and filter_attributes.place_in_line != -1:
            conditions.append("place_in_line = ?")
            params.append(filter_attributes.place_in_line)

    if min_place_in_line != -1:
        conditions.append("place_in_line >= ?")
        params.append(min_place_in_line)
    if max_place_in_line != -1:
        conditions.append("place_in_line <= ?")
        params.append(max_place_in_line)

    sql = ("SELECT item_id, customer_id, place_in_line FROM waitlist"
           + _build_where(conditions, params))
    cur = _get_cur()
    cur.execute(sql, params)
    results = []
    for row in cur.fetchall():
        results.append(Waitlist(
            item_id=row[0].strip() if row[0] else row[0],
            customer_id=row[1].strip() if row[1] else row[1],
            place_in_line=row[2],
        ))
    return results



def save_changes():
    conn.commit()


def close_connection():
    global conn, cur
    try:
        cur.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass