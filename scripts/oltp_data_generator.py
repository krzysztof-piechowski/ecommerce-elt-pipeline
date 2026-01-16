import json
import os
import random
import time
import sys
from datetime import datetime, timedelta
from faker import Faker
from common_logger import logger

# --- AZURE IMPORTS ---
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

# ==========================================
# CONFIGURATION
# ==========================================
STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
REMOTE_ROOT_FOLDER = "data"
STATE_FILE_PATH = "state/generator_state.json"

# Generation parameters
BATCHES = 5
CUSTOMERS_PER_BATCH = 300
ORDERS_PER_BATCH = 500

fake = Faker()

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def get_initial_state():
    """Returns default state starting from 1"""
    return {
        "customer_id_seq": 1,
        "address_id_seq": 1,
        "order_id_seq": 1,
        "order_item_id_seq": 1,
        "payment_id_seq": 1,
        "shipment_id_seq": 1,
        "history_id_seq": 1,
        "last_run_ts": "2024-01-01T00:00:00"
    }

def load_state(blob_service_client):
    """Downloads state JSON from Azure Blob Storage or returns default."""
    try:
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=STATE_FILE_PATH)
        download = blob_client.download_blob()
        state_data = json.loads(download.readall())
        logger.info(f"Loaded existing state from Azure. Resuming Order ID from: {state_data.get('order_id_seq')}")
        return state_data
    except ResourceNotFoundError:
        logger.info("State file not found in Azure. Starting fresh (seq=1).")
        return get_initial_state()
    except Exception as e:
        logger.warning(f"Failed to load state: {e}. Defaulting to fresh state.")
        return get_initial_state()

def save_state(blob_service_client, state):
    """Uploads updated state JSON to Azure Blob Storage."""
    try:
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=STATE_FILE_PATH)
        blob_client.upload_blob(json.dumps(state, indent=2, default=str), overwrite=True)
        logger.info("State saved successfully to Azure!")
    except Exception as e:
        logger.error(f"Failed to save state to Azure: {e}")
        raise e

def upload_json_to_azure(blob_service_client, folder, filename, data):
    """Uploads data to Azure Blob Storage in JSON format. Raises error on failure."""

    blob_path = f"{REMOTE_ROOT_FOLDER}/{folder}/{filename}"

    try:
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_path)
        
        # Serialize to JSON string -> bytes
        json_data = json.dumps(data, indent=2, default=str)
    
        blob_client.upload_blob(
            json_data, 
            overwrite=True,
            content_settings=ContentSettings(content_type='application/json')
        )
            
    except Exception as e:
        logger.error(f"CRITICAL: Upload failed for {filename}: {e}")
        raise e

def generate_email():
    domains = ["gmail.com", "outlook.com", "hotmail.com", "yahoo.com", "icloud.com"]
    return f"{fake.user_name()}@{random.choice(domains)}"

def generate_phone():
    return f"+{random.randint(10,99)} {random.randint(100,999)} {random.randint(100,999)} {random.randint(100,999)}"




# ==========================================
# MAIN EXECUTION
# ==========================================

def main():
    try:
        # 1. CONNECT TO AZURE
        logger.info(f"Connecting to Azure Storage: {STORAGE_ACCOUNT_NAME}...")
        credential = DefaultAzureCredential()
        account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        logger.info("Connected to Azure via Identity!")

        # 2. LOAD STATE
        current_state = load_state(blob_service_client)

        # Extract sequences
        customer_id_seq = current_state["customer_id_seq"]
        address_id_seq = current_state["address_id_seq"]
        order_id_seq = current_state["order_id_seq"]
        order_item_id_seq = current_state["order_item_id_seq"]
        payment_id_seq = current_state["payment_id_seq"]
        shipment_id_seq = current_state["shipment_id_seq"]
        history_id_seq = current_state["history_id_seq"]

        base_date = datetime.now()


        # 3. STATIC DATA GENERATION (CATALOG)
        PRODUCT_CATALOG = [
            ("iPhone 15 Pro", "Electronics", "Apple", 1199.00),
            ("Samsung Galaxy S24", "Electronics", "Samsung", 999.00),
            ("MacBook Pro 16", "Electronics", "Apple", 2499.00),
            ("AirPods Pro", "Electronics", "Apple", 249.00),
            ("PlayStation 5", "Electronics", "Sony", 499.00),
            ("Xbox Series X", "Electronics", "Microsoft", 499.00),
            ("Nintendo Switch OLED", "Electronics", "Nintendo", 349.00),
            ("LG OLED TV 55", "Electronics", "LG", 1499.00),
            ("Samsung 4K Monitor", "Electronics", "Samsung", 399.00),
            ("Logitech MX Master 3", "Electronics", "Logitech", 99.00),
            ("Razer BlackWidow V3", "Electronics", "Razer", 139.00),    
            ("Sony WH-1000XM5", "Electronics", "Sony", 349.00),
            ("Dyson V15 Detect", "Home", "Dyson", 649.00),
            ("Nespresso Vertuo", "Home", "Nespresso", 179.00),
            ("Nike Air Zoom", "Sports", "Nike", 139.00),
            ("Adidas Ultraboost", "Sports", "Adidas", 189.00),
            ("Ray-Ban Aviator", "Fashion", "Ray-Ban", 189.00),
            ("Kindle Paperwhite", "Electronics", "Amazon", 139.00),
        ]

        products = []
        for i, (name, cat, brand, price) in enumerate(PRODUCT_CATALOG):
            products.append({
                "product_id": i + 1,
                "name": name,
                "category": cat,
                "brand": brand,
                "price": price,
                "currency": "EUR",
                "status": "ACTIVE",
                "created_at": datetime(2024, 1, 1),
                "updated_at": datetime(2024, 1, 1),
            })

        products_ts = datetime.now().strftime("%Y%m%d_%H_%M_%S")
        logger.info(f"Uploading Product Catalog ({len(products)} items)...")
        upload_json_to_azure(blob_service_client, "products_raw", f"products_{products_ts}.json", products)


        # 4. TRANSACTION GENERATION LOOP
        logger.info(f"Starting Incremental Transaction Generation ({BATCHES} batches)...")
        ORDER_FLOW = ["CREATED", "PAID", "SHIPPED", "DELIVERED"]

        for batch in range(1, BATCHES + 1):

            timestamp_str = datetime.now().strftime("%Y%m%d_%H_%M_%S")
            logger.info(f"Processing Batch {batch}/{BATCHES} with TS: {timestamp_str}...")
            
            # --- CUSTOMERS & ADDRESSES ---
            batch_customers = []
            batch_addresses = []
            
            for _ in range(CUSTOMERS_PER_BATCH):
                # Customers created recently
                cust_created = base_date - timedelta(days=random.randint(0, 30))
                
                # Customer
                batch_customers.append({
                    "customer_id": customer_id_seq,
                    "email": generate_email(),
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "phone": generate_phone(),
                    "status": "ACTIVE",
                    "created_at": cust_created,
                    "updated_at": cust_created
                })
                
                # Address
                batch_addresses.append({
                    "address_id": address_id_seq,
                    "customer_id": customer_id_seq,
                    "type": "SHIPPING",
                    "street": fake.street_address(),
                    "city": fake.city(),
                    "postal_code": fake.postcode(),
                    "country": fake.country_code(),
                    "is_default": True,
                    "created_at": cust_created,
                    "updated_at": cust_created
                })
                
                customer_id_seq += 1
                address_id_seq += 1

            # --- ORDERS & TRANSACTIONS ---
            batch_orders = []
            batch_items = []
            batch_payments = []
            batch_shipments = []
            batch_history = []
            
            for _ in range(ORDERS_PER_BATCH):
                cust = random.choice(batch_customers)
                order_date = cust["created_at"] + timedelta(hours=random.randint(1, 48))
                
                # Determine Status Flow
                final_status = random.choice(ORDER_FLOW)
                status_steps = ORDER_FLOW[:ORDER_FLOW.index(final_status) + 1]
                
                # Generate Status History
                current_ts = order_date
                for step in status_steps:
                    batch_history.append({
                        "status_history_id": history_id_seq,
                        "order_id": order_id_seq,
                        "status": step,
                        "changed_at": current_ts
                    })
                    history_id_seq += 1
                    current_ts += timedelta(hours=random.randint(2, 12))
                
                final_ts = current_ts
                
                # Create Order Items
                items_count = random.randint(1, 4)
                total_amount = 0
                for _ in range(items_count):
                    prod = random.choice(products)
                    qty = random.randint(1, 2)
                    line_total = qty * prod["price"]
                    total_amount += line_total
                    
                    batch_items.append({
                        "order_item_id": order_item_id_seq,
                        "order_id": order_id_seq,
                        "product_id": prod["product_id"],
                        "quantity": qty,
                        "unit_price": prod["price"],
                        "created_at": order_date
                    })
                    order_item_id_seq += 1
                    
                # Create Order Header
                batch_orders.append({
                    "order_id": order_id_seq,
                    "customer_id": cust["customer_id"],
                    "order_status": final_status,
                    "order_total_amount": round(total_amount, 2),
                    "currency": "EUR",
                    "created_at": order_date,
                    "updated_at": final_ts
                })
                
                # Payment
                if final_status in ["PAID", "SHIPPED", "DELIVERED"]:
                    batch_payments.append({
                        "payment_id": payment_id_seq,
                        "order_id": order_id_seq,
                        "provider": random.choice(["VISA", "PAYPAL"]),
                        "payment_status": "SUCCESS",
                        "amount": round(total_amount, 2),
                        "created_at": order_date + timedelta(minutes=15),
                        "updated_at": order_date + timedelta(minutes=15)
                    })
                    payment_id_seq += 1
                    
                # Shipment
                if final_status in ["SHIPPED", "DELIVERED"]:
                    ship_ts = next(h["changed_at"] for h in batch_history if h["status"] == "SHIPPED")
                    del_ts = next((h["changed_at"] for h in batch_history if h["status"] == "DELIVERED"), None)
                    
                    batch_shipments.append({
                        "shipment_id": shipment_id_seq,
                        "order_id": order_id_seq,
                        "carrier": "DHL",
                        "shipment_status": final_status,
                        "shipped_at": ship_ts,
                        "delivered_at": del_ts,
                        "updated_at": del_ts if del_ts else ship_ts
                    })
                    shipment_id_seq += 1
                    
                order_id_seq += 1

            # --- UPLOAD BATCH  ---
            upload_json_to_azure(blob_service_client, "customers_raw", f"customers_{timestamp_str}.json", batch_customers)
            upload_json_to_azure(blob_service_client, "addresses_raw", f"addresses_{timestamp_str}.json", batch_addresses)
            upload_json_to_azure(blob_service_client, "orders_raw", f"orders_{timestamp_str}.json", batch_orders)
            upload_json_to_azure(blob_service_client, "order_items_raw", f"items_{timestamp_str}.json", batch_items)
            upload_json_to_azure(blob_service_client, "payments_raw", f"payments_{timestamp_str}.json", batch_payments)
            upload_json_to_azure(blob_service_client, "shipments_raw", f"shipments_{timestamp_str}.json", batch_shipments)
            upload_json_to_azure(blob_service_client, "order_status_history_raw", f"history_{timestamp_str}.json", batch_history)

            time.sleep(1)

        # 5. SAVE STATE
        new_state = {
            "customer_id_seq": customer_id_seq,
            "address_id_seq": address_id_seq,
            "order_id_seq": order_id_seq,
            "order_item_id_seq": order_item_id_seq,
            "payment_id_seq": payment_id_seq,
            "shipment_id_seq": shipment_id_seq,
            "history_id_seq": history_id_seq,
            "last_run_ts": datetime.now().isoformat()
        }
        save_state(blob_service_client, new_state)

        logger.info(f"SUCCESS: Data generation finished. Next Order ID will be: {order_id_seq}")

    except Exception as e:
        logger.error(f"FATAL ERROR in Data Generator: {e}")
        logger.error("Stopping execution to prevent ADF trigger.")
        sys.exit(1)


if __name__ == "__main__":
    main()