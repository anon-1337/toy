import random
import csv
import argparse
from datetime import datetime


def generate_transaction_csv(num_clients: int, num_transactions: int, output_file: str):
    """
    Generate a CSV file with random deposit and withdrawal transactions.

    Args:
        num_clients (int): Number of unique clients
        num_transactions (int): Total number of transactions to generate
        output_file (str): Path to save the CSV file
    """
    # Transaction types
    transaction_types = ["deposit", "withdrawal"]

    # Generate transactions
    transactions = []
    for tx_id in range(1, num_transactions + 1):
        tx_type = random.choice(transaction_types)
        client_id = random.randint(1, num_clients)

        # Generate a random amount between 0.01 and 1000.00
        amount = round(random.uniform(0.01, 1000.00), 2)

        transactions.append({
            "type": tx_type,
            "client": client_id,
            "tx": tx_id,
            "amount": amount
        })

    # Write to CSV file
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['type', 'client', 'tx', 'amount']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for transaction in transactions:
            writer.writerow(transaction)

    print(f"Generated {num_transactions} transactions for {num_clients} clients in {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate transaction CSV data')
    parser.add_argument('--clients', type=int, default=10, help='Number of unique clients')
    parser.add_argument('--transactions', type=int, default=100, help='Number of transactions to generate')
    parser.add_argument('--output', type=str, default='transactions.csv', help='Output file name')

    args = parser.parse_args()

    # Add timestamp to filename to avoid overwriting
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{args.output.split('.')[0]}_{timestamp}.csv"

    generate_transaction_csv(args.clients, args.transactions, output_file)
