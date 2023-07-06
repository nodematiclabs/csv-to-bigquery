import csv
import random
from datetime import datetime, timedelta

# Function to generate random date
def random_date(start, end):
    return start + timedelta(
        days=random.randint(0, int((end - start).days)),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

# Define the product names
products = ['Widget', 'Gadget', 'Doohickey']

# Generate random orders
orders = []
for i in range(10):  # generate 10 random orders
    order = {
        'partner_name': f'Partner{random.randint(1, 5)}',
        'order_id': str(random.randint(100, 999)),
        'product': random.choice(products),
        'quantity': str(random.randint(1, 50)),
        'date': random_date(datetime(2023, 1, 1), datetime(2023, 7, 6)).strftime('%Y-%m-%d %H:%M')
    }
    orders.append(order)

# Specify the file name
filename = 'partner_orders.csv'

# Writing to csv file
with open(filename, 'w', newline='') as csvfile:
    # Create a CSV DictWriter
    writer = csv.DictWriter(csvfile, fieldnames=orders[0].keys())
    
    # Write the header
    writer.writeheader()
    
    # Write the data rows
    writer.writerows(orders)

print(f'Random partner orders have been successfully written to {filename}')
