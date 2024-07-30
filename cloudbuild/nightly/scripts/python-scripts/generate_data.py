import base64
import datetime
import json
import random
import string
import geojson

from google.cloud import bigquery


def generate_int():
    return random.randint(1, 10000)


def generate_string():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))


def generate_bytes():
    byte_result = random.randbytes(random.randint(5, 10))
    return base64.b64encode(byte_result).decode()


def generate_float_array():
    array_size = random.randint(1, 6)
    array = []
    for _ in range(array_size):
        array.append(random.uniform(1, 5))
    return array


def generate_numeric():
    scale = random.randint(2, 9)
    precision = random.randint(max(1, scale) + 1, scale + 29)
    prev = precision - scale
    # print("Scale", scale, "precision", precision)
    numeric = (str(random.randint(10 ** (prev - 1), 10 ** prev)) + "." +
               str(random.randint(10 ** (scale - 1), 10 ** scale)))
    # print("val", numeric)
    return random.choice([numeric, None])


def generate_bignumeric():
    scale = random.randint(9, 38)
    precision = random.randint(max(1, scale) + 1, scale + 38)
    prev = precision - scale
    # print("Scale", scale, "precision", precision)
    bignumeric = (str(random.randint(10 ** (prev - 1), 10 ** prev)) + "."
                  + str(random.randint(10 ** (scale - 1), 10 ** scale)))
    # print("val", bignumeric)
    return random.choice([bignumeric, None])


def generate_bool():
    return random.choice([True, False, None])


def generate_timestamp():
    return random.choice([str(datetime.datetime.now()), None])


def generate_date():
    return random.choice([str(datetime.datetime.now().date()), None])


def generate_datetime():
    return random.choice([str(datetime.datetime.utcnow()), None])


def generate_geography():
    return random.choice(
        [
            geojson.LineString([(-118.4085, 32.91234), (-72.7781, 56.64127)]),
            geojson.LineString([(-118.325, 53.345), (-73.7781, 40.6413)]),
            geojson.LineString([(-118.324, 43.435), (-43.34543, 56.546)]),
            geojson.LineString([(-118.4085, 33.9416), (-57.345, 35.345)])])


def generate_json():
    return random.choice([{"name": "Alice", "items": [{"product": "book", "price": 10},
                                                      {"product": "food", "price": 5}]},
                          {"name": "Bob", "items": [{"product": "pen", "price": 20}]},
                          {"hotel class": "5-star", "vacancy": True}])


def generate_record():
    return {"json_field": random.choice([json.dumps(generate_json()), None]),
            "geography_field": random.choice([geojson.dumps(generate_geography()), None])}


def generate_row():
    return {"name": generate_string(),
            "bytes_field": generate_bytes(),
            "integer_field": generate_int(),
            "array_field": generate_float_array(),
            "numeric_field": generate_numeric(),
            "bignumeric_field": generate_bignumeric(),
            "boolean_field": generate_bool(),
            "ts_field": generate_timestamp(),
            "date_field": generate_date(),
            "datetime_field": generate_datetime(),
            "geography_field": random.choice([geojson.dumps(generate_geography()), None]),
            "record_field": generate_record()
            }


rows_to_insert = []
for i in range(1):
    for _ in range(880):
        row = generate_row()
        rows_to_insert.append(row)

    print("Data Generated")
    table_id = "testproject-398714.write_testing_dataset.allBigQueryDataTypesTable"
    # print(rows_to_insert)
    # Construct a BigQuery Client Object.
    client = bigquery.Client()

    errors = client.insert_rows_json(table_id, rows_to_insert, skip_invalid_rows=True)  # Make an API request.
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    print("Done ", i)
