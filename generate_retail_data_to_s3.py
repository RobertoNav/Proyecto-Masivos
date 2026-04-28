import argparse
import csv
import os
import random
import subprocess
import time
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

REGIONS = ["north", "south", "east", "west", "central"]
CATEGORIES = ["electronics", "home", "clothing", "sports", "beauty", "books", "toys", "automotive"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "cash", "bank_transfer"]
ORDER_STATUS = ["completed", "cancelled", "refunded", "pending"]

def random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86399)
    return start_date + timedelta(days=random_days, seconds=random_seconds)

def run_command(command):
    print("Running:", " ".join(command))
    subprocess.run(command, check=True)

def upload_to_s3(local_path, s3_path):
    run_command(["aws", "s3", "cp", local_path, s3_path])

def write_products(local_dir, product_count):
    path = os.path.join(local_dir, "products.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "category", "product_name", "brand", "unit_price"])

        for product_id in range(1, product_count + 1):
            category = random.choice(CATEGORIES)
            product_name = f"{category}_{fake.word()}_{product_id}"
            brand = fake.company().replace(",", "")
            unit_price = round(random.uniform(5, 2500), 2)

            writer.writerow([product_id, category, product_name, brand, unit_price])

    return path

def write_customers(local_dir, customer_count):
    path = os.path.join(local_dir, "customers.csv")

    start_date = datetime(2021, 1, 1)
    end_date = datetime(2026, 1, 1)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "first_name", "last_name", "email", "region", "signup_date"])

        for customer_id in range(1, customer_count + 1):
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = f"user{customer_id}@example.com"
            region = random.choice(REGIONS)
            signup_date = random_date(start_date, end_date).date().isoformat()

            writer.writerow([customer_id, first_name, last_name, email, region, signup_date])

    return path

def write_transaction_file(local_dir, file_index, start_order_id, customers_count, products_count, rows_per_file):
    file_path = os.path.join(local_dir, f"transactions_part_{file_index:05d}.csv")

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2026, 1, 1)

    order_id = start_order_id

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_rate",
            "payment_method",
            "order_status",
            "order_timestamp",
            "shipping_region",
            "device_type"
        ])

        for _ in range(rows_per_file):
            customer_id = random.randint(1, customers_count)
            product_id = random.randint(1, products_count)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(5, 2500), 2)
            discount_rate = round(random.choice([0, 0.05, 0.10, 0.15, 0.20]), 2)
            payment_method = random.choice(PAYMENT_METHODS)
            order_status = random.choice(ORDER_STATUS)
            order_timestamp = random_date(start_date, end_date).strftime("%Y-%m-%d %H:%M:%S")
            shipping_region = random.choice(REGIONS)
            device_type = random.choice(["mobile", "desktop", "tablet"])

            # Dirty data intentionally added for the Spark cleaning step
            if random.random() < 0.002:
                shipping_region = ""
            if random.random() < 0.001:
                quantity = ""

            writer.writerow([
                order_id,
                customer_id,
                product_id,
                quantity,
                unit_price,
                discount_rate,
                payment_method,
                order_status,
                order_timestamp,
                shipping_region,
                device_type
            ])

            order_id += 1

    return file_path, order_id

def write_summary(local_dir, s3_prefix, total_bytes, total_rows, transaction_files, customers, products):
    path = os.path.join(local_dir, "_generation_summary.txt")

    total_gb = round(total_bytes / 1024 / 1024 / 1024, 2)

    with open(path, "w", encoding="utf-8") as f:
        f.write("Synthetic Retail Dataset Generation Summary\n")
        f.write("===========================================\n")
        f.write(f"S3 raw data path: {s3_prefix}\n")
        f.write(f"Total generated transaction size GB: {total_gb}\n")
        f.write(f"Total transaction rows: {total_rows}\n")
        f.write(f"Transaction CSV files: {transaction_files}\n")
        f.write(f"Customers: {customers}\n")
        f.write(f"Products: {products}\n")
        f.write("Data model type: relational\n")
        f.write("Tables: customers, products, transactions\n")

    return path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-prefix", required=True)
    parser.add_argument("--local-dir", default="data/tmp_generation")
    parser.add_argument("--target-gb", type=float, default=31)
    parser.add_argument("--customers", type=int, default=500000)
    parser.add_argument("--products", type=int, default=50000)
    parser.add_argument("--rows-per-file", type=int, default=1000000)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    random.seed(args.seed)
    Faker.seed(args.seed)

    os.makedirs(args.local_dir, exist_ok=True)

    s3_prefix = args.s3_prefix.rstrip("/")

    print("Generating products.csv...")
    products_path = write_products(args.local_dir, args.products)
    upload_to_s3(products_path, f"{s3_prefix}/products/products.csv")
    os.remove(products_path)

    print("Generating customers.csv...")
    customers_path = write_customers(args.local_dir, args.customers)
    upload_to_s3(customers_path, f"{s3_prefix}/customers/customers.csv")
    os.remove(customers_path)

    print("Generating transaction files...")
    target_bytes = int(args.target_gb * 1024 * 1024 * 1024)

    total_bytes = 0
    total_rows = 0
    file_index = 0
    next_order_id = 1
    start_time = time.time()

    while total_bytes < target_bytes:
        file_path, next_order_id = write_transaction_file(
            local_dir=args.local_dir,
            file_index=file_index,
            start_order_id=next_order_id,
            customers_count=args.customers,
            products_count=args.products,
            rows_per_file=args.rows_per_file
        )

        file_size = os.path.getsize(file_path)
        total_bytes += file_size
        total_rows += args.rows_per_file

        s3_file_path = f"{s3_prefix}/transactions/{os.path.basename(file_path)}"
        upload_to_s3(file_path, s3_file_path)
        os.remove(file_path)

        file_index += 1

        current_gb = round(total_bytes / 1024 / 1024 / 1024, 2)
        elapsed = round((time.time() - start_time) / 60, 2)

        print(
            f"Uploaded file {file_index} | "
            f"Current generated size: {current_gb} GB | "
            f"Rows: {total_rows} | "
            f"Elapsed: {elapsed} min"
        )

    summary_path = write_summary(
        local_dir=args.local_dir,
        s3_prefix=s3_prefix,
        total_bytes=total_bytes,
        total_rows=total_rows,
        transaction_files=file_index,
        customers=args.customers,
        products=args.products
    )

    upload_to_s3(summary_path, f"{s3_prefix}/_generation_summary.txt")
    os.remove(summary_path)

    print("Finished.")
    print(f"Raw dataset path: {s3_prefix}")
    print(f"Generated transaction size: {round(total_bytes / 1024 / 1024 / 1024, 2)} GB")
    print(f"Total transaction rows: {total_rows}")
    print(f"Transaction files: {file_index}")

if __name__ == "__main__":
    main()
