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
