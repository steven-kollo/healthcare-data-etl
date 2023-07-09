import pandas as pd
# Dims query logic:
#     INSERT INTO branch (branch)
#     SELECT branch
#     FROM (VALUES ('Middleton'), ('test1'), ('test2'), ('test3')) v(branch)
#     WHERE NOT EXISTS (SELECT 1 FROM branch t1 WHERE t1.branch = v.branch)


def create_dims_query(df, dim_tables_names):
    dims = get_dims_from_df(df, dim_tables_names)
    query_string = "WITH "
    for dim in dim_tables_names:
        query_string = query_string + \
            f"ins_{dim} AS ( INSERT INTO {dim} ({dim}) SELECT {dim} FROM (VALUES "
        values = ""
        for name in dims[dim]:
            values = values + f"('{name}'),"
        values = values[:-1] + \
            f") v({dim}) WHERE NOT EXISTS (SELECT 1 FROM {dim} t1 WHERE t1.{dim} = v.{dim})"
        query_string = query_string + values + "),"

    return query_string[:-1] + " SELECT * FROM branch"


def get_dims_from_df(df, dim_tables_names):
    dims = {}
    for dim_name in dim_tables_names:
        dims[dim_name] = remove_duplicates(df[dim_name])
    return dims


def remove_duplicates(x):
    return list(dict.fromkeys(x))
