import os
import random

OUT_DIR = "./tpcds_data"
os.makedirs(OUT_DIR, exist_ok=True)

date_dim_rows = []
base_sk = 2451544
from datetime import date, timedelta
for i in range(365 * 5):  
    d = date(2000, 1, 1) + timedelta(days=i)
    date_dim_rows.append(f"{base_sk + i}|{d.isoformat()}")

with open(f"{OUT_DIR}/date_dim.csv", "w") as f:
    f.write("d_date_sk|d_date\n")
    f.write("\n".join(date_dim_rows) + "\n")

print(f"date_dim.csv     — {len(date_dim_rows)} rows")

streets = ["Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "Washington", "Hill"]
cities  = ["Springfield", "Fairview", "Oakdale", "Riverside", "Clinton"]
states  = ["FL", "TX", "CA", "NY", "IL", "AZ", "PA", "CO"]

addr_rows = []
for sk in range(1, 201):  
    num    = random.randint(1, 999)
    street = random.choice(streets)
    city   = random.choice(cities)
    state  = random.choice(states)
    zip_   = f"{random.randint(10000, 99999)}"
    addr_rows.append(f"{sk}|{num}|{street}|{city}|{state}|{zip_}")

with open(f"{OUT_DIR}/customer_address.csv", "w") as f:
    f.write("ca_address_sk|ca_street_number|ca_street_name|ca_city|ca_state|ca_zip\n")
    f.write("\n".join(addr_rows) + "\n")

print(f"customer_address.csv — {len(addr_rows)} rows")

first_names = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank"]
last_names  = ["Smith", "Jones", "Brown", "Davis", "Wilson", "Moore", "Taylor"]
domains     = ["gmail.com", "yahoo.com", "outlook.com", "edu.net"]

cust_rows = []
used_ids = set()
for sk in range(1, 51): 
    cid = f"CUST{sk:012d}"[:16] 
    fn  = random.choice(first_names)
    ln  = random.choice(last_names)
    email = f"{fn}.{ln}{sk}@{random.choice(domains)}"
    addr_sk = random.randint(1, 200)
    cust_rows.append(f"{sk}|{cid}|{fn}|{ln}|{email}|{addr_sk}")

with open(f"{OUT_DIR}/customer.csv", "w") as f:
    f.write("c_customer_sk|c_customer_id|c_first_name|c_last_name|c_email_address|c_current_addr_sk\n")
    f.write("\n".join(cust_rows) + "\n")

print(f"customer.csv     — {len(cust_rows)} rows")

brands      = ["BrandA", "BrandB", "BrandC", "BrandD"]
classes_    = ["pop", "dresses", "shirts", "pants", "decor", "toddlers"]
categories  = ["Music", "Women", "Men", "Children", "Home"]
manufacts   = ["ManufactA", "ManufactB", "ManufactC"]
prod_names  = ["Widget", "Gadget", "Doohickey", "Thingamajig", "Gizmo"]
years       = [1997, 1999, 2000, 2001, 2003]

item_rows = []
for sk in range(1, 31):
    iid   = f"ITEM{sk:012d}"[:16]
    year  = random.choice(years)
    date_ = f"{year}-01-01"
    name  = random.choice(prod_names)
    brand = random.choice(brands)
    cls   = random.choice(classes_)
    cat   = random.choice(categories)
    mfg   = random.choice(manufacts)
    price = round(random.uniform(0.50, 100.00), 2)
    item_rows.append(f"{sk}|{iid}|{date_}|{name}|{brand}|{cls}|{cat}|{mfg}|{price}")

with open(f"{OUT_DIR}/item.csv", "w") as f:
    f.write("i_item_sk|i_item_id|i_rec_start_date|i_product_name|i_brand|i_class|i_category|i_manufact|i_current_price\n")
    f.write("\n".join(item_rows) + "\n")

print(f"item.csv         — {len(item_rows)} rows")

ss_rows = []
ticket = 1
for i in range(100):
    date_sk  = random.randint(base_sk, base_sk + 365)
    item_sk  = random.randint(1, 30)
    cust_sk  = random.randint(1, 50)
    net_paid = round(random.uniform(1.00, 500.00), 2)
    ss_rows.append(f"{date_sk}|{item_sk}|{cust_sk}|{ticket}|{net_paid}")
    if i % 3 == 0:
        ticket += 1

with open(f"{OUT_DIR}/store_sales.csv", "w") as f:
    f.write("ss_sold_date_sk|ss_item_sk|ss_customer_sk|ss_ticket_number|ss_net_paid\n")
    f.write("\n".join(ss_rows) + "\n")

print(f"store_sales.csv  — {len(ss_rows)} rows")
print("\nDone! All CSVs written to ./tpcds_data/")
