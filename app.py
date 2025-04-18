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
    row = table.read_row(b"total_rows")
    if not row:
        return "0"
    cells = row.cells.get(COLUMN_FAMILY_ID, {})
    count_cell = cells.get(b'count', [None])[0]
    return count_cell.value.decode() if count_cell else "0"

@app.route('/Best-BMW')
def best_bmw():
    filter_chain = row_filters.RowFilterChain([
        row_filters.ColumnQualifierRegexFilter(b'make'),
        row_filters.ValueRegexFilter(b'BMW'),
        row_filters.ColumnQualifierRegexFilter(b'electric range'),
        row_filters.ValueRangeFilter(start_value=b'101', end_value=b'9999')
    ])
    
    return str(sum(1 for _ in table.read_rows(filter_=filter_chain)))

@app.route('/tesla-owners')
def tesla_owners():
    filter_chain = row_filters.RowFilterChain([
        row_filters.ColumnQualifierRegexFilter(b'make'),
        row_filters.ValueRegexFilter(b'TESLA'),
        row_filters.ColumnQualifierRegexFilter(b'city'),
        row_filters.ValueRegexFilter(b'SEATTLE')
    ])
    return str(sum(1 for _ in table.read_rows(filter_=filter_chain)))

@app.route('/update')
def update_electric_range():
    row_key = b'257246118'
    row = table.direct_row(row_key)
    row.set_cell(COLUMN_FAMILY_ID, b'electric range', '200')
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
        if model_year and int(model_year.value) < 2014:
            to_delete.append(row.row_key)
    # delete rows
    for i in range(0, len(to_delete), 500):
        batch = table.mutate_rows()
        for key in to_delete[i:i+500]:
            row = table.direct_row(key)
            row.delete()
            batch.add(row)
        batch.commit()

    counter_row = table.direct_row(b"total_rows")
    counter_row.increment_cell_value(COLUMN_FAMILY_ID, b"count", -len(to_delete))
    counter_row.commit()
    return str(int(total_rows()) - len(to_delete))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)


