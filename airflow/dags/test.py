import pandas as pd
# import json
# with open('trans.txt', 'r') as f:
#     lines = f.read()
#     print(lines)

# raw_data = '{ "data": [' + \
#     raw_data.replace("[", "").replace("]", "").replace("'", "") + "]}"
# dict_raw = json.loads(raw_data)
# df = pd.DataFrame.from_records(dict_raw['data'])
# df = df[df['B'].notnull()]
# print(df.head())

dim_tables_names = ["branch", "whyUs", "whyUsClientName"]
DATA = {
    "branch": ['Middleton'],
    "whyUs": ['CUESPEARS to EE', 'Existing Client', 'Family & Friends', 'Google search',
              'Local to the Area', 'New CUES', 'Prospectrn', 'Referral', 'Reviews on Internet', 'Work Voucher'],
    "whyUsClientName": ['Chunkit Fong', 'Louise Bezyk', 'Alec Copeland', 'Chun Kit Fong', 'Steven Hamer', 'Sue Pownall', 'Katie Palmer', 'Emma Connolly', 'Liyana Junaid', 'Azizur Rehman', 'Elizabeth  Boott-Wroe', 'Joe Farry', 'Alfie Watterson', 'Deepa Scaria', 'Kirsty Scanlan', 'Bonatla Morupisi', 'Aleksandra Matuszczak', 'Aishai Junaid', 'Adrian Wawoczny', 'Tyreese Macfarlane', 'Shah Ali', 'Guelany Ketura Marie  Guiri',
                        'Chris Hardcastle', 'Jaqueline Whitehead', 'Adil Arfan', 'John Barker', 'ANDREW RAWLINSON', 'James Taylor', 'Rachael Mccrystal', 'Thomas McDermott', 'Samuel Keqi', 'Elizabeth McDermott', 'James McDermott', 'Elsa Rush', 'Philip Heywood', 'Ciara Purcell', 'Lee Northover', 'Keith Robinson', 'Denise Dale', 'Liz Chadwick', 'Nicholas Hamilton', 'Janine Parry', 'Vincent Wood', 'Adele Platt', 'Mark Gibbons', 'Mohammed Qasim']
}
query_string = "WITH "
for dim in dim_tables_names:
    query_string = query_string + \
        f"ins_{dim} AS ( INSERT INTO {dim} ({dim}) SELECT {dim} FROM (VALUES "
    values = ""
    for name in DATA[dim]:
        values = values + f"('{name}'),"
    values = values[:-1] + \
        f") v({dim}) WHERE NOT EXISTS (SELECT 1 FROM {dim} t1 WHERE t1.{dim} = v.{dim})"
    query_string = query_string + values + "),"

print(query_string[:-1] + " SELECT * FROM branch")
# print(query_string + values)


#     INSERT INTO branch (branch)
#     SELECT branch
#     FROM (VALUES ('Middleton'), ('test1'), ('test2'), ('test3')) v(branch)
#     WHERE NOT EXISTS (SELECT 1 FROM branch t1 WHERE t1.branch = v.branch)

# WITH ins_branch AS (
#             INSERT INTO branch
#                 (branch)
#             VALUES
#                 ('test 1')
#             ),
#             ins_product AS (
#             INSERT INTO product
#                 (product)
#             VALUES
#                 ('test 1')
#             )
#         SELECT * FROM branch
