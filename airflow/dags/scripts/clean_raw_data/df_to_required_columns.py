import pandas as pd


def prepare_whyus(df):
    df = df[df['G'].notnull()]
    headers = df.iloc[0].tolist()
    return {
        "df": df,
        "headers": headers
    }


def clean_whyus(df):
    for column in df:
        df[column] = df[column].astype('str')
        df[column] = df[column].str.lstrip()
    return df


def prepare_newpatients(df):
    df = df[df['G'].notnull()]
    headers = df.iloc[0].tolist()
    return {
        "df": df,
        "headers": headers
    }


def clean_newpatients(df):
    df = df.replace({'Branch: ': ''}, regex=True)
    return df


def prepare_transactions(df):
    df = df[df['G'].notnull()]
    print(df.iloc[0])
    headers = df.iloc[0].tolist()
    return {
        "df": df,
        "headers": headers
    }


def clean_transactions(df):
    df = df.replace({'u00a3': ''}, regex=True)
    return df


def prepare_salesbybrand(df):
    df = df[df['AB'].notnull()]
    print(df.iloc[0])
    headers = df.iloc[0].tolist()
    return {
        "df": df,
        "headers": headers
    }


def clean_salesbybrand(df):
    df = df.replace({'u00a3': ''}, regex=True)
    return df


def rename_df_columns(df, old_headers, new_headers):
    for i in range(len(old_headers)):
        df.rename(columns={old_headers[i]: new_headers[i]}, inplace=True)
    return df


def format_float(column):
    column = column.fillna(value=0).astype("float64")
    return column


def format_int(column):
    column = column.fillna(value=0).astype("int64")
    return column


def format_str(column):
    column = column.fillna(value="").astype("string")
    column = column.str.lstrip()
    return column


def transform_date(date):
    # YYYY-MM-DD
    year = date[-4:]
    month = date[2:4]
    day = date[:2]
    return f"{year}-{month}-{day}"


def format_date(column):
    column = column.fillna(value="").astype("string")
    column = column.apply(lambda date: transform_date(date)).astype("string")
    return column


RAW_REPORTS_CONFIG = {
    "format_scripts": {
        "str": format_str,
        "float": format_float,
        "int": format_int,
        "date": format_date
    },
    "whyus": {
        "prepare_script": prepare_whyus,
        "clean_script": clean_whyus,
        "required_fields": ["Branch", "WhyUs2", "Name2"],
        "db_fields": ["branch", "whyUs", "whyUsClientName"],
        "type_fields": ["str", "str", "str"],
        "dim_tables_names": ["branch", "whyUs", "whyUsClientName"],
        "facts_table_name": "survey"
    },
    "newpatients": {
        "prepare_script": prepare_newpatients,
        "clean_script": clean_newpatients,
        "required_fields": ["Branch", "StaffTitle", "PxPublicID", "PxTitle", "BookingDate", "AppStartDate", "AppType", "AppStatusDesc"],
        "db_fields": ["branch", "staffTitle", "pxPublicId", "pxTitle", "bookingDate", "appStartDate", "appType", "appStatus"],
        "type_fields": ["str", "str", "int", "str", "date", "date", "str", "str"],
        "dim_tables_names": ["branch", "staffTitle", "pxTitle", "appType", "appStatus"],
        "facts_table_name": "newPatients"
    },
    "transactions": {
        "prepare_script": prepare_transactions,
        "clean_script": clean_transactions,
        "required_fields": ["TransDate", "PxPublicID", "PxLastName", "TransPublicID", "PaymentCategory", "ItemDesc", "SalesValue", "Discount", "NetSales", "Sales", "Takings", "Refunds1"],
        "db_fields": ["transDate", "pxPublicId", "pxLastName", "transPublicId", "paymentCategory", "itemDesc", "salesValue", "discount", "netSales", "sales", "takings", "refunds"],
        "type_fields": ["date", "int", "str", "int", "str", "str", "float", "float", "float", "float", "float", "float"],
        "dim_tables_names": ["branch", "pxLastName", "paymentCategory", "itemDesc"],
        "facts_table_name": "transactions"
    },
    "salesbybrand": {
        "prepare_script": prepare_salesbybrand,
        "clean_script": clean_salesbybrand,
        "required_fields": ["BranchName", "ProdType2", "Brand2", "Manufacturer2", "ProdCode2", "ProdName2", "UnitPrice2", "Textbox63", "Textbox64", "Discount2", "Void_Refund2", "Textbox83"],
        "db_fields": ["branch", "prodType", "brand", "manufacturer", "prodCode", "prodName", "unitPrice", "quantity", "totalPrice", "discount", "voidRefund", "endPrice"],
        "type_fields": ["str", "str", "str", "str", "str", "str", "float", "int", "float", "float", "float", "float"],
        "dim_tables_names": ["branch", "prodType", "brand", "manufacturer", "prodCode", "prodName"],
        "facts_table_name": "productSales"
    }
}


def format_df(df, fields, type_fields, format_scripts):
    for i in range(len(fields)):
        df[fields[i]] = format_scripts[type_fields[i]](df[fields[i]])
    return df


def filter_df_columns(raw_df, headers, required_fields):
    df_required_columns = pd.DataFrame()
    for header in headers:
        for field in required_fields:
            if field.lower() == header.lower():
                values = raw_df.iloc[:, headers.index(header)].tolist()
                values.pop(0)
                print(values)
                df_required_columns[field] = values
    return df_required_columns


def to_required_columns(raw_df, report_type):
    format_scripts = RAW_REPORTS_CONFIG["format_scripts"]
    config = RAW_REPORTS_CONFIG[report_type]
    db_fields = config["db_fields"]
    type_fields = config["type_fields"]
    required_fields = config["required_fields"]
    prepared_data = config["prepare_script"](raw_df)
    headers = prepared_data["headers"]
    raw_df = prepared_data["df"]

    df_required_columns = filter_df_columns(raw_df, headers, required_fields)
    cleaned_df = config["clean_script"](df_required_columns)
    cleaned_df = rename_df_columns(
        cleaned_df, required_fields, db_fields)
    formated_df = format_df(cleaned_df, db_fields, type_fields, format_scripts)

    formated_df.info()
    return formated_df


# END OF CLEANING DATA
# ====================
def remove_duplicates(x):
    return list(dict.fromkeys(x))


# UPDATE DIMS

def update_dims(dataset, field, new_dims):
    print(f'Updating "{field}" field dims')

    new_dims_str = '"' + '","'.join(new_dims) + '"'
    starting_index = get_dims_starting_index(dataset, field)
    existing_dims = get_existing_dims(new_dims_str, dataset, field)
    dims_to_add = filter_new_dims_with_existing_dims(
        new_dims, existing_dims, starting_index)
    if dims_to_add == []:
        print("Nothing to add")
        return

    client.query(generate_insert_dims_query(
        dims_to_add, starting_index, dataset, field))
    print("New dims added:")
    print(dims_to_add)


def get_dims_starting_index(dataset, field):
    starting_index = 0
    GET_LAST_ROW = (
        f"SELECT MAX(index) AS index FROM `clean_data_{dataset}.{field}`"
    )
    query_job = client.query(GET_LAST_ROW)
    rows = query_job.result()
    for row in rows:
        starting_index = row.index + 1
    return starting_index


def get_existing_dims(new_dims_str, dataset, field):
    existing_dims = []
    GET_EXISTING_DIMS_QUERY = (
        f"SELECT value FROM `clean_data_{dataset}.{field}` WHERE value IN ({new_dims_str})"
    )
    query_job = client.query(GET_EXISTING_DIMS_QUERY)
    rows = query_job.result()
    for row in rows:
        try:
            existing_dims.append(row.value)
        except:
            pass
    return existing_dims


def filter_new_dims_with_existing_dims(new_dims, existing_dims, starting_index):
    for existing_dim in existing_dims:
        new_dims = list(filter(lambda a: a != existing_dim, new_dims))
    return new_dims


def generate_insert_dims_query(dims_to_add, starting_index, dataset, field):
    INSERT_NEW_DIMS_QUERY = f"INSERT INTO `clean_data_{dataset}.{field}` VALUES "
    for dim in dims_to_add:
        INSERT_NEW_DIMS_QUERY = INSERT_NEW_DIMS_QUERY + \
            f'({starting_index}, "{dim}"),'
        starting_index += 1
    return INSERT_NEW_DIMS_QUERY[:-1]


# REPLACE DIMS WITH INDEXES


def replace_dims_with_indexes(dataset, field, dims, values):
    dims_dict = create_dims_dict(dims, dataset, field)
    for i in range(len(values)):
        values[i] = dims_dict[values[i]]
    return values


def create_dims_dict(dims, dataset, field):
    dims_str = '"' + '","'.join(dims) + '"'
    GET_EXISTING_DIMS_QUERY = (
        f"SELECT * FROM `clean_data_{dataset}.{field}` WHERE value IN ({dims_str})"
    )
    query_job = client.query(GET_EXISTING_DIMS_QUERY)
    rows = query_job.result()
    dims_dict = {"": ""}
    for row in rows:
        try:
            dims_dict[row.value] = row.index
        except:
            pass
    return dims_dict
