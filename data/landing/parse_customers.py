import json
import pandas as pd

# Read the JSON file
with open('data/landing/customers/customers.json', 'r') as f:
    customers_data = json.load(f)

# Flatten the nested structure
flattened_data = []
for customer in customers_data:
    flat_customer = {
        'customer_id': customer['customer_id'],
        'first_name': customer['first_name'],
        'last_name': customer['last_name'],
        'email': customer['email'],
        'city': customer['address']['city'],
        'country': customer['address']['country'],
        'signup_date': customer['signup_date'],
        'tier': customer['tier']
    }
    flattened_data.append(flat_customer)

# Create DataFrame
df = pd.DataFrame(flattened_data)

# Print the DataFrame
print(df)
