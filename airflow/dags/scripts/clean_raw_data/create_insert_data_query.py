import json
# Insert data query logic:
#   WITH temp_values (period, branch, whyusclientname, whyus) AS (
#       VALUES
#       (2023103, 'Middleton', 'Keith Robinson', 'CUESPEARS to EE'),
#       (2023103, 'Middleton', 'Nicholas Hamilton', 'Family & Friends')),
#   temp_ids as (
#       SELECT temp_values.period, branch.branchid, whyusclientname.whyusclientnameid, whyus.whyusid  FROM temp_values
#       JOIN whyus on whyus.whyus = temp_values.whyus
#       JOIN whyusclientname on whyusclientname.whyusclientname = temp_values.whyusclientname
#       JOIN branch on branch.branch = temp_values.branch)
#   INSERT INTO survey (period, branchid, whyusclientnameid, whyusid)
#   SELECT period, branchid, whyusclientnameid, whyusid
#   FROM temp_ids


def create_values_string(data, dims, period):
    values_string = ""
    for row in data:
        value_string = f"({period}"
        for dim in dims:
            value_string = value_string + f",'{row[dim]}'"
        value_string = value_string + "),"
        values_string = values_string + value_string

    return values_string[:-1]


def create_joins_subquery(dims):
    fields = "temp_values.period"
    joins = ""
    for dim in dims:
        fields = fields + f",{dim}.{dim}id"
        joins = joins + f"join {dim} on {dim}.{dim} = temp_values.{dim} "
    joins_subquery = f"SELECT {fields} FROM temp_values {joins}"

    return joins_subquery


def create_insert_data_query(data, dims, period, facts_table):
    data = json.loads('{ "data": ' + data + "}")['data']
    value_columns = "period,"+(",".join(dims))
    dim_columns = "period,"+("id,".join(dims))+"id"
    values_string = create_values_string(data, dims, period)
    joins_subquery = create_joins_subquery(dims)
    insert_statement = f"INSERT INTO {facts_table} ({dim_columns}) SELECT {dim_columns} FROM temp_ids"
    query = f"WITH temp_values ({value_columns}) AS (VALUES {values_string}), temp_ids AS ({joins_subquery}) {insert_statement}"

    return query
