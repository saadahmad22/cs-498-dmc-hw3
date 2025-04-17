from flask import Flask, request
from google.cloud import bigtable
from google.cloud.bigtable import row_filters

PROJECT_ID = 'education-457121'
INSTANCE_ID = 'ev-bigtable'
TABLE_ID = 'ev-population'
COLUMN_FAMILY_ID = 'ev_info'

client = bigtable.Client(project=PROJECT_ID, admin=True)
instance = client.instance(INSTANCE_ID)
table = instance.table(TABLE_ID)

app = Flask(__name__)

@app.route('/rows')
def total_rows():
    return str(sum(1 for _ in table.read_rows()))

@app.route('/Best-BMW')
def best_bmw():
    count = 0
    rows = table.read_rows()
    for row in rows:
        cells = row.cells[COLUMN_FAMILY_ID]
        make = cells.get(b'make', [None])[0]
        erange = cells.get(b'electric range', [None])[0]
        if make and make.value.decode().strip().upper() == 'BMW':
            try:
                if erange and int(erange.value) > 100:
                    count += 1
            except ValueError:
                # ignore errorss
                continue
    return str(count)

@app.route('/tesla-owners')
def tesla_owners():
    count = 0
    rows = table.read_rows()
    for row in rows:
        cells = row.cells[COLUMN_FAMILY_ID]
        make = cells.get(b'make', [None])[0]
        city = cells.get(b'city', [None])[0]
        if make and city:
            if make.value.decode().strip().upper() == 'TESLA' and city.value.decode().strip().upper() == 'SEATTLE':
                count += 1
    return str(count)

@app.route('/update')
def update_electric_range():
    row_key = b'257246118'
    row = table.direct_row(row_key)
    row.set_cell(COLUMN_FAMILY_ID, 'electric range', '200')
    row.commit()
    return "Success"

@app.route('/delete')
def delete_old():
    # delete all records where model year < 2014
    rows = table.read_rows()
    to_delete = []
    for row in rows:
        cells = row.cells[COLUMN_FAMILY_ID]
        model_year = cells.get(b'model year', [None])[0]
        if model_year:
            try:
                if int(model_year.value) < 2014:
                    to_delete.append(row.row_key)
            except ValueError:
                continue
    # delete rows
    for row_key in to_delete:
        row = table.direct_row(row_key)
        row.delete()
        row.commit()
    return str(sum(1 for _ in table.read_rows()))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


