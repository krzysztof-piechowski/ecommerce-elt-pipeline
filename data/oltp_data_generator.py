import json
import os
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

OUTPUT_DIR = "raw"
BATCHES = 5

CUSTOMERS_PER_BATCH = 500
PRODUCTS = 100
ORDERS_PER_BATCH = 1000

STATUSES_ORDER = ["CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]
STATUSES_PAYMENT = ["INITIATED", "SUCCESS", "FAILED"]
STATUSES_SHIPMENT = ["CREATED", "IN_TRANSIT", "DELIVERED"]

os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_json(folder, filename, data):
    path = os.path.join(OUTPUT_DIR, folder)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, filename), "w") as f:
        json.dump(data, f, indent=2, default=str)


# -------------------------
# PRODUCTS - STATIC DATA
# -------------------------

PRODUCT_CATALOG = [
    # Electronics
    ("iPhone 15 Pro", "Electronics", "Apple", 1199.00),
    ("Samsung Galaxy S24", "Electronics", "Samsung", 999.00),
    ("MacBook Pro 16", "Electronics", "Apple", 2499.00),
    ("Dell XPS 15", "Electronics", "Dell", 1899.00),
    ("iPad Air", "Electronics", "Apple", 599.00),
    ("Sony WH-1000XM5", "Electronics", "Sony", 349.00),
    ("AirPods Pro", "Electronics", "Apple", 249.00),
    ("PlayStation 5", "Electronics", "Sony", 499.00),
    ("Xbox Series X", "Electronics", "Microsoft", 499.00),
    ("Nintendo Switch OLED", "Electronics", "Nintendo", 349.00),
    ("LG OLED TV 55", "Electronics", "LG", 1499.00),
    ("Samsung 4K Monitor", "Electronics", "Samsung", 399.00),
    ("Logitech MX Master 3", "Electronics", "Logitech", 99.00),
    ("Razer BlackWidow V3", "Electronics", "Razer", 139.00),
    ("Canon EOS R6", "Electronics", "Canon", 2499.00),
    ("Sony A7 IV", "Electronics", "Sony", 2499.00),
    ("GoPro Hero 12", "Electronics", "GoPro", 399.00),
    ("DJI Mini 3 Pro", "Electronics", "DJI", 759.00),
    ("Kindle Paperwhite", "Electronics", "Amazon", 139.00),
    ("Apple Watch Series 9", "Electronics", "Apple", 399.00),
    
    # Home & Kitchen
    ("Dyson V15 Detect", "Home", "Dyson", 649.00),
    ("iRobot Roomba j7+", "Home", "iRobot", 799.00),
    ("Nespresso Vertuo", "Home", "Nespresso", 179.00),
    ("KitchenAid Stand Mixer", "Home", "KitchenAid", 449.00),
    ("Ninja Air Fryer", "Home", "Ninja", 129.00),
    ("Instant Pot Duo", "Home", "Instant Pot", 99.00),
    ("Philips Hue Starter Kit", "Home", "Philips", 179.00),
    ("Nest Learning Thermostat", "Home", "Google", 249.00),
    ("Ring Video Doorbell", "Home", "Ring", 99.00),
    ("Shark Navigator Vacuum", "Home", "Shark", 199.00),
    ("Cuisinart Food Processor", "Home", "Cuisinart", 149.00),
    ("Vitamix E310", "Home", "Vitamix", 349.00),
    ("All-Clad D3 Pan Set", "Home", "All-Clad", 599.00),
    ("Le Creuset Dutch Oven", "Home", "Le Creuset", 379.00),
    ("Breville Barista Express", "Home", "Breville", 699.00),
    ("Anova Sous Vide", "Home", "Anova", 199.00),
    ("Casper Original Mattress", "Home", "Casper", 1095.00),
    ("Purple Mattress Queen", "Home", "Purple", 1299.00),
    ("Brooklinen Sheets Set", "Home", "Brooklinen", 149.00),
    ("Parachute Down Comforter", "Home", "Parachute", 299.00),
    
    # Sports & Outdoors
    ("Peloton Bike+", "Sports", "Peloton", 2495.00),
    ("NordicTrack Treadmill", "Sports", "NordicTrack", 1599.00),
    ("Bowflex Adjustable Dumbbells", "Sports", "Bowflex", 549.00),
    ("Rogue Power Rack", "Sports", "Rogue", 895.00),
    ("Theragun PRO", "Sports", "Therabody", 599.00),
    ("Hydro Flask 32oz", "Sports", "Hydro Flask", 44.95),
    ("Yeti Cooler 45", "Sports", "Yeti", 349.00),
    ("Garmin Fenix 7", "Sports", "Garmin", 699.00),
    ("Fitbit Charge 6", "Sports", "Fitbit", 159.00),
    ("Whoop 4.0 Strap", "Sports", "Whoop", 239.00),
    ("Nike Air Zoom Pegasus", "Sports", "Nike", 139.00),
    ("Adidas Ultraboost 23", "Sports", "Adidas", 189.00),
    ("Lululemon Align Leggings", "Sports", "Lululemon", 98.00),
    ("Arc'teryx Beta Jacket", "Sports", "Arc'teryx", 649.00),
    ("Patagonia Down Sweater", "Sports", "Patagonia", 279.00),
    ("North Face Borealis Backpack", "Sports", "The North Face", 99.00),
    ("Osprey Atmos 65L", "Sports", "Osprey", 299.00),
    ("Black Diamond Headlamp", "Sports", "Black Diamond", 49.95),
    ("REI Half Dome Tent", "Sports", "REI", 299.00),
    ("MSR PocketRocket Stove", "Sports", "MSR", 44.95),
    
    # Fashion & Accessories
    ("Ray-Ban Aviator", "Fashion", "Ray-Ban", 189.00),
    ("Oakley Holbrook", "Fashion", "Oakley", 189.00),
    ("Michael Kors Jet Set", "Fashion", "Michael Kors", 298.00),
    ("Coach Leather Wallet", "Fashion", "Coach", 178.00),
    ("Fossil Gen 6 Smartwatch", "Fashion", "Fossil", 299.00),
    ("Swatch Sistem51", "Fashion", "Swatch", 150.00),
    ("Timex Weekender", "Fashion", "Timex", 49.95),
    ("Casio G-Shock", "Fashion", "Casio", 99.00),
    ("Herschel Little America", "Fashion", "Herschel", 119.00),
    ("Fjällräven Kånken", "Fashion", "Fjällräven", 89.00),
    
    # Books & Media
    ("Kindle Scribe", "Books", "Amazon", 339.00),
    ("Audible Premium Plus", "Books", "Audible", 14.95),
    ("Spotify Premium Family", "Media", "Spotify", 16.99),
    ("Netflix Premium", "Media", "Netflix", 19.99),
    ("Disney+ Subscription", "Media", "Disney", 10.99),
    ("Apple Music", "Media", "Apple", 10.99),
    ("YouTube Premium", "Media", "YouTube", 13.99),
    ("PlayStation Plus", "Media", "Sony", 9.99),
    ("Xbox Game Pass Ultimate", "Media", "Microsoft", 16.99),
    ("Nintendo Switch Online", "Media", "Nintendo", 19.99),
    
    # Beauty & Personal Care
    ("Dyson Airwrap", "Beauty", "Dyson", 599.00),
    ("Philips OneBlade", "Beauty", "Philips", 49.95),
    ("Oral-B iO Series 9", "Beauty", "Oral-B", 299.00),
    ("Foreo Luna 3", "Beauty", "Foreo", 219.00),
    ("NuFace Trinity", "Beauty", "NuFace", 339.00),
    ("Clarisonic Mia Smart", "Beauty", "Clarisonic", 199.00),
    ("Revlon One-Step Dryer", "Beauty", "Revlon", 59.99),
    ("Conair Infiniti Pro", "Beauty", "Conair", 44.99),
    
    # Office & Stationery
    ("Herman Miller Aeron", "Office", "Herman Miller", 1495.00),
    ("Steelcase Leap", "Office", "Steelcase", 1099.00),
    ("Autonomous SmartDesk", "Office", "Autonomous", 499.00),
    ("Uplift V2 Standing Desk", "Office", "Uplift", 599.00),
    ("Logitech C920 Webcam", "Office", "Logitech", 79.99),
    ("Blue Yeti Microphone", "Office", "Blue", 129.00),
    ("Elgato Stream Deck", "Office", "Elgato", 149.00),
    ("Wacom Intuos Pro", "Office", "Wacom", 379.00),
    ("Moleskine Classic Notebook", "Office", "Moleskine", 24.95),
    ("Leuchtturm1917 Dotted", "Office", "Leuchtturm", 21.95),
    ("Pilot G2 Pen Set", "Office", "Pilot", 12.99),
    ("Sharpie Permanent Markers", "Office", "Sharpie", 14.99),
    ("Post-it Notes Pack", "Office", "3M", 19.99),
    ("HP OfficeJet Pro 9015", "Office", "HP", 299.00),
]

products = [
    {
        "product_id": i + 1,
        "name": PRODUCT_CATALOG[i][0],
        "category": PRODUCT_CATALOG[i][1],
        "brand": PRODUCT_CATALOG[i][2],
        "price": PRODUCT_CATALOG[i][3],
        "currency": "EUR",
        "status": "ACTIVE",
        "created_at": datetime(2024, 1, 1),
        "updated_at": datetime(2024, 1, 1),
    }
    for i in range(PRODUCTS)
]

# SAVE PRODUCTS
write_json("products_raw", "products_raw.json", products)

# ---------------------------
# GENERATE TRANSACTIONAL DATA
# ---------------------------

customer_id_seq = 1
address_id_seq = 1
order_id_seq = 1
order_item_id_seq = 1
payment_id_seq = 1
shipment_id_seq = 1

base_date = datetime.now() - timedelta(days=30)

for batch in range(1, BATCHES + 1):
    print(f"Generating batch {batch}/{BATCHES}...")
    
    # --- CUSTOMERS ---
    batch_customers = []
    for i in range(CUSTOMERS_PER_BATCH):
        batch_customers.append({
            "customer_id": customer_id_seq,
            "email": fake.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "phone": fake.phone_number(),
            "status": "ACTIVE",
            "created_at": base_date + timedelta(days=batch * 2),
            "updated_at": base_date + timedelta(days=batch * 2),
        })
        customer_id_seq += 1
    
    # --- ADDRESSES ---
    batch_addresses = []
    for customer in batch_customers:
        batch_addresses.append({
            "address_id": address_id_seq,
            "customer_id": customer["customer_id"],
            "type": "SHIPPING",
            "street": fake.street_address(),
            "city": fake.city(),
            "postal_code": fake.postcode(),
            "country": fake.country_code(),
            "is_default": True,
            "created_at": customer["created_at"],
            "updated_at": customer["updated_at"],
        })
        address_id_seq += 1
    
    # --- ORDERS ---
    batch_orders = []
    batch_order_items = []
    batch_payments = []
    batch_shipments = []
    
    for _ in range(ORDERS_PER_BATCH):
        customer = random.choice(batch_customers)
        order_date = base_date + timedelta(days=batch * 3, hours=random.randint(0, 23))
        
        # Determine order, payment, and shipment statuses
        order_status = random.choice(STATUSES_ORDER)
        
        if order_status == "CANCELLED":
            payment_status = random.choice(["FAILED", "SUCCESS"])
            shipment_status = "CREATED"
        elif order_status == "DELIVERED":
            payment_status = "SUCCESS"
            shipment_status = "DELIVERED"
        elif order_status == "SHIPPED":
            payment_status = "SUCCESS"
            shipment_status = "IN_TRANSIT"
        elif order_status == "PAID":
            payment_status = "SUCCESS"
            shipment_status = random.choice(["CREATED", "IN_TRANSIT"])
        else:  # CREATED
            payment_status = random.choice(["INITIATED", "SUCCESS"])
            shipment_status = "CREATED"
        
        order = {
            "order_id": order_id_seq,
            "customer_id": customer["customer_id"],
            "order_status": order_status,
            "order_total_amount": 0,  # to be updated later
            "currency": "EUR",
            "created_at": order_date,
            "updated_at": order_date,
        }
        
        # ORDER ITEMS
        items_count = random.randint(1, 5)
        total = 0
        
        for _ in range(items_count):
            product = random.choice(products)
            qty = random.randint(1, 3)
            amount = qty * product["price"]
            total += amount
            
            batch_order_items.append({
                "order_item_id": order_item_id_seq,
                "order_id": order_id_seq,
                "product_id": product["product_id"],
                "quantity": qty,
                "unit_price": product["price"],
                "created_at": order_date,
            })
            order_item_id_seq += 1
        
        order["order_total_amount"] = total
        batch_orders.append(order)
        
        # PAYMENT
        batch_payments.append({
            "payment_id": payment_id_seq,
            "order_id": order_id_seq,
            "provider": random.choice(["VISA", "MASTERCARD", "PAYPAL"]),
            "payment_status": payment_status,
            "amount": order["order_total_amount"],
            "created_at": order_date,
            "updated_at": order_date + timedelta(hours=1),
        })
        payment_id_seq += 1
        
        # SHIPMENT
        shipped_at = order_date + timedelta(days=1) if shipment_status != "CREATED" else None
        delivered_at = order_date + timedelta(days=random.randint(3, 7)) if shipment_status == "DELIVERED" else None
        
        batch_shipments.append({
            "shipment_id": shipment_id_seq,
            "order_id": order_id_seq,
            "carrier": random.choice(["DHL", "UPS", "FEDEX"]),
            "shipment_status": shipment_status,
            "shipped_at": shipped_at,
            "delivered_at": delivered_at,
            "updated_at": delivered_at or shipped_at or order_date,
        })
        shipment_id_seq += 1
        
        order_id_seq += 1
    
    # --- SAVE ---
    write_json("customers_raw", f"customers_raw_{batch}.json", batch_customers)
    write_json("addresses_raw", f"addresses_raw_{batch}.json", batch_addresses)
    write_json("orders_raw", f"orders_raw_{batch}.json", batch_orders)
    write_json("order_items_raw", f"order_items_raw_{batch}.json", batch_order_items)
    write_json("payments_raw", f"payments_raw_{batch}.json", batch_payments)
    write_json("shipments_raw", f"shipments_raw_{batch}.json", batch_shipments)

print(f"\n✅ Data generation completed!")
print(f"📊 Generated:")
print(f"   - 1 products file ({PRODUCTS} products)")
print(f"   - {BATCHES} batches × 6 files = {BATCHES * 6} transaction files")
print(f"   - Total customers: {BATCHES * CUSTOMERS_PER_BATCH}")
print(f"   - Total orders: {BATCHES * ORDERS_PER_BATCH}")
print(f"   - Total files: {1 + BATCHES * 6}")