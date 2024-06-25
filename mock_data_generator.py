import csv
import random
import datetime
import boto3
import os
from create_rds_schema import connect_and_create_db

# Initialize the S3 client
s3_client = boto3.client('s3')

# Lambda function to generate a random debit card number
generate_debit_card_number = lambda: ''.join([str(random.randint(0, 9)) for _ in range(16)])

# Lambda function to generate a random name
generate_name = lambda: f"{random.choice(['John', 'Jane', 'Alex', 'Emily', 'Chris', 'Katie', 'Michael', 'Sarah', 'David', 'Laura'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor'])}"

# Lambda function to generate a random debit card type
generate_debit_card_type = lambda: random.choice(['Visa', 'MasterCard', 'Discover', 'American Express'])

# Function to generate mock data and save to CSV
def generate_mock_data_for_day(num_records, date_str, bucket_name):
    filename = f"transactions_{date_str}.csv"
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['customer_id', 'name', 'debit_card_number', 'debit_card_type', 'bank_name', 'transaction_date', 'amount_spend']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for i in range(num_records):
            writer.writerow({
                'customer_id': i + 1,
                'name': generate_name(),
                'debit_card_number': generate_debit_card_number(),
                'debit_card_type': generate_debit_card_type(),
                'bank_name': random.choice(['Bank of America', 'Chase', 'Wells Fargo', 'Citi', 'Capital One']),
                'transaction_date': date_str,
                'amount_spend': round(random.uniform(5.0, 500.0), 2)
            })
    
    # Upload the file to S3 with Hive-like partitioning
    s3_key = f"tmp/transactions/date={date_str}/{filename}"
    source_s3_bucket = "customer-debit-card-purchase-source-data"
    s3_client.upload_file(filename, source_s3_bucket, s3_key)
    
    # Remove the file after upload
    os.remove(filename)

# Function to get the last generated date from S3
def get_last_generated_date(bucket_name, metadata_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=metadata_key)
        last_date_str = response['Body'].read().decode('utf-8')
        return datetime.datetime.strptime(last_date_str, '%Y-%m-%d').date()
    except s3_client.exceptions.NoSuchKey:
        return None

# Function to update the last generated date in S3
def update_last_generated_date(bucket_name, metadata_key, date):
    s3_client.put_object(Bucket=bucket_name, Key=metadata_key, Body=date.strftime('%Y-%m-%d'))

# Main function for AWS Lambda
def lambda_handler(event, context):
    bucket_name = 'hh-s3-datalake-gds'
    metadata_key = 'last_generated_date.txt'
    num_records_per_day = 100
    
    # Get the last generated date
    last_date = get_last_generated_date(bucket_name, metadata_key)
    
    # Calculate the next date
    if last_date:
        next_date = last_date + datetime.timedelta(days=1)
    else:
        next_date = datetime.date.today()
    
    date_str = next_date.strftime('%Y-%m-%d')
    
    # Generate data for the next date
    generate_mock_data_for_day(num_records_per_day, date_str, bucket_name)
    print("Mock data generated succesfully")
    
    # Update the last generated date
    update_last_generated_date(bucket_name, metadata_key, next_date)

    print("Updated last generated data")

    # Connect to RDS, create customers database and customer_transactions table - mysql-aws-de-db
    try:
        connect_and_create_db()
    except Exception as e:
        print("Exception occurs while connect_and_create_db :",str(e))
    
    return {
        'statusCode': 200,
        'body': f'Mock data for {date_str} generated and uploaded to S3 successfully'
    }

# # Example event and context for local testing
# if __name__ == "__main__":
#     class Context:
#         def __init__(self):
#             self.function_name = 'local_test'
#             self.memory_limit_in_mb = '128'
#             self.invoked_function_arn = 'arn:aws:lambda:local:test'
#             self.aws_request_id = 'local_test_request_id'
    
#     event = {}
#     context = Context()
#     lambda_handler(event, context)
#test
