import pandas as pd
from google.cloud import bigtable

# Set these variables
PROJECT_ID = 'education-457121'
INSTANCE_ID = 'ev-bigtable'
TABLE_ID = 'ev-population'
COLUMN_FAMILY_ID = 'ev_info'

def load_data(csv_path):
    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(INSTANCE_ID)
    table = instance.table(TABLE_ID)

    counter_row = table.direct_row(b"total_rows")
    counter_row.set_cell("ev_info", b"count", str(0).encode())
    counter_row.commit()

    df = pd.read_csv(csv_path, dtype=str, na_filter=False)
    total_rows = 0
    rows = []
    for _, row in df.iterrows():
        row_key = row['DOL Vehicle ID'].encode()
        bt_row = table.direct_row(row_key)
        bt_row.set_cell(COLUMN_FAMILY_ID, 'make', row['Make'])
        bt_row.set_cell(COLUMN_FAMILY_ID, 'model', row['Model'])
        bt_row.set_cell(COLUMN_FAMILY_ID, 'model year', row['Model Year'])
        bt_row.set_cell(COLUMN_FAMILY_ID, 'electric range', row['Electric Range'].strip() if row['Electric Range'].strip() else "0")
        bt_row.set_cell(COLUMN_FAMILY_ID, 'city', row['City'])
        bt_row.set_cell(COLUMN_FAMILY_ID, 'county', row['County'])
        rows.append(bt_row)
        total_rows += 1
        
        # batch write, then write rest
        if len(rows) == 500:
            table.mutate_rows(rows)
            counter_row.increment_cell_value("ev_info", b"count", 500)
            counter_row.commit()
            rows = []
    if rows:
        table.mutate_rows(rows)
        counter_row.increment_cell_value("ev_info", b"count", len(rows))
        counter_row.commit()
    print(f"Loaded {total_rows} rows.")

if __name__ == "__main__":
    load_data('Electric_Vehicle_Population_Data.csv')

