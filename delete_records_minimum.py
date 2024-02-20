import requests

api_key = ''
base_id = ''
table_name = ''
headers = {'Authorization': 'Bearer ' + api_key}

def fetch_all_records():
    records = []
    url = f'https://api.airtable.com/v0/{base_id}/{table_name}'

    while True:
        response = requests.get(url, headers=headers)
        response_json = response.json()
        records.extend(response_json.get('records', []))

        offset = response_json.get('offset')
        if not offset:
            break

        url = f'https://api.airtable.com/v0/{base_id}/{table_name}?offset={offset}'

    return records

all_records = fetch_all_records()
print(len(all_records))

# Step 2: Determine the Minimum Date

def find_minimum_date(records):
    min_date = None
    for record in records:
        record_date = record['fields'].get('As Of Date')
        if record_date:
            # Use datetime.datetime.strptime
            record_date = datetime.datetime.strptime(record_date, '%Y-%m-%d')
            if not min_date or record_date < min_date:
                min_date = record_date
    return min_date

min_date = find_minimum_date(all_records)
print(min_date)

def delete_records(records, min_date):
    record_ids_to_delete = [record['id'] for record in records if datetime.datetime.strptime(record['fields'].get('As Of Date', '9999-12-31'), '%Y-%m-%d') <= min_date]
    batch_size = 10  # Airtable allows up to 10 records per batch deletion
    
    for i in range(0, len(record_ids_to_delete), batch_size):
        batch = record_ids_to_delete[i:i + batch_size]
        # Construct the URL with multiple record IDs
        url = f'https://api.airtable.com/v0/{base_id}/{table_name}?' + '&'.join([f'records[]={id}' for id in batch])
        response = requests.delete(url, headers=headers)
        
        # # Handle response and errors
        # if response.status_code == 200:
        #     print(f'Successfully deleted batch: {batch}')
        # else:
        #     print(f'Error deleting batch: {batch}. Response: {response.text}')

if min_date:
    delete_records(all_records, min_date)
    print(f"Records deleted for {min_date}")
