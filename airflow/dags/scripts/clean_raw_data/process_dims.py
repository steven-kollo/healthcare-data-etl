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

    # client.query(generate_insert_dims_query(
    #     dims_to_add, starting_index, dataset, field))
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
    # query_job = client.query(GET_EXISTING_DIMS_QUERY)
    # rows = query_job.result()
    # for row in rows:
    #     try:
    #         existing_dims.append(row.value)
    #     except:
    #         pass
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
    # query_job = client.query(GET_EXISTING_DIMS_QUERY)
    # rows = query_job.result()
    # dims_dict = {"": ""}
    # for row in rows:
    #     try:
    #         dims_dict[row.value] = row.index
    #     except:
    #         pass
    # return dims_dict
