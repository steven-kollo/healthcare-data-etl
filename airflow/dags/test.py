import pandas as pd
import json
with open('trans.txt', 'r') as f:
    lines = f.read()
    print(lines)

# raw_data = '{ "data": [' + \
#     raw_data.replace("[", "").replace("]", "").replace("'", "") + "]}"
# dict_raw = json.loads(raw_data)
# df = pd.DataFrame.from_records(dict_raw['data'])
# df = df[df['B'].notnull()]
# print(df.head())
