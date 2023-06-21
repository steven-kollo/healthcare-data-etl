from google.cloud import bigquery
client = bigquery.Client()


def insert_clean_data(query):
    print(query)
    client.query(query)


def generate_insert_data_query(df, dataset_name, period):
    cols = []
    for col in df.columns:
        cols.append(col)

    QUERY = generate_insert_query_base(cols, dataset_name)
    for index, row in df.iterrows():
        QUERY = QUERY + generate_values_str(row, cols, period)
    return QUERY[:-1]


def generate_insert_query_base(cols, dataset_name):
    cols_str = '(period,' + ','.join(cols) + ')'
    QUERY_BASE = f"INSERT INTO `clean_data_{dataset_name}.data` {cols_str} VALUES "
    return QUERY_BASE


def generate_values_str(row, cols, period):
    values_str = f"({period}"
    for col in cols:
        values_str = values_str + f",{row[col]}"
    values_str = values_str + "),"
    return values_str
