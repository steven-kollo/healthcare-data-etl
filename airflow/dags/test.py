import pandas as pd
import json

raw_data = """[['[{"A":"Textbox28","B":"WhyUs","C":"Textbox22","D":"Name","E":"Textbox17","F":null,"G":null},{"A":"46","B":"CUESPEARS to EE","C":"5","D":"Chunkit Fong","E":"1","F":null,"G":null},{"A":"46","B":"CUESPEARS to EE","C":"5","D":"Louise Bezyk","E":"1","F":null,"G":null},{"A":"46","B":"CUESPEARS to EE","C":"5","D":"Alec Copeland","E":"1","F":null,"G":null},{"A":"46","B":"CUESPEARS to EE","C":"5","D":"Chun Kit Fong","E":"1","F":null,"G":null},{"A":"46","B":"CUESPEARS to EE","C":"5","D":"Steven Hamer","E":"1","F":null,"G":null},{"A":"46","B":"Existing Client","C":"4","D":"Sue Pownall","E":"1","F":null,"G":null},{"A":"46","B":"Existing Client","C":"4","D":"Katie Palmer","E":"1","F":null,"G":null},{"A":"46","B":"Existing Client","C":"4","D":"Emma Connolly","E":"1","F":null,"G":null},{"A":"46","B":"Existing Client","C":"4","D":"Liyana Junaid","E":"1","F":null,"G":null},{"A":"46","B":"Family & Friends ","C":"4","D":"Azizur Rehman","E":"1","F":null,"G":null},{"A":"46","B":"Family & Friends ","C":"4","D":"Elizabeth Boott-Wroe ","E":"1","F":null,"G":null},{"A":"46","B":"Family & Friends ","C":"4","D":"Joe Farry","E":"1","F":null,"G":null},{"A":"46","B":"Family & Friends ","C":"4","D":"Alfie Watterson","E":"1","F":null,"G":null},{"A":"46","B":"Google search","C":"3","D":"Deepa Scaria","E":"1","F":null,"G":null},{"A":"46","B":"Google search","C":"3","D":"Kirsty Scanlan","E":"1","F":null,"G":null},{"A":"46","B":"Google search","C":"3","D":"Bonatla Morupisi","E":"1","F":null,"G":null},{"A":"46","B":"Local to the Area","C":"4","D":"Aleksandra Matuszczak","E":"1","F":null,"G":null},{"A":"46","B":"Local to the Area","C":"4","D":"Aishai Junaid","E":"1","F":null,"G":null},{"A":"46","B":"Local to the Area","C":"4","D":"Adrian Wawoczny","E":"1","F":null,"G":null},{"A":"46","B":"Local to the Area","C":"4","D":"Tyreese Macfarlane","E":"1","F":null,"G":null},{"A":"46","B":"New CUES ","C":"7","D":"Shah Ali","E":"1","F":null,"G":null},{"A":"46","B":"New CUES ","C":"7","D":"Guelany Ketura Marie Guiri","E":"1","F":null,"G":null},{"A":"46","B":"New CUES ","C":"7","D":"Chris Hardcastle","E":"1","F":null,"G":null},{"A":"46","B":"New CUES ","C":"7","D":"Jaqueline Whitehead","E":"1","F":null,"G":null},{"A":"46","B":"New CUES ","C":"7","D":"Adil Arfan","E":"1","F":null,"G":null},{"A":"46","B":"New CUES ","C":"7","D":"John Barker","E":"1","F":null,"G":null},{"A":"46","B":"New CUES ","C":"7","D":"ANDREW RAWLINSON","E":"1","F":null,"G":null},{"A":"46","B":"Prospectrn","C":"2","D":"James Taylor","E":"1","F":null,"G":null},{"A":"46","B":"Prospectrn","C":"2","D":"Rachael Mccrystal","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Thomas McDermott","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Samuel Keqi","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Elizabeth McDermott","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"James McDermott","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Elsa Rush","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Philip Heywood","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Ciara Purcell","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Lee Northover","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Keith Robinson","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Denise Dale","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Liz Chadwick","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Nicholas Hamilton","E":"1","F":null,"G":null},{"A":"46","B":"Referral","C":"13","D":"Janine Parry","E":"1","F":null,"G":null},{"A":"46","B":"Reviews on Internet","C":"3","D":"Vincent Wood","E":"1","F":null,"G":null},{"A":"46","B":"Reviews on Internet","C":"3","D":"Adele Platt","E":"1","F":null,"G":null},{"A":"46","B":"Reviews on Internet","C":"3","D":"Mark Gibbons","E":"1","F":null,"G":null},{"A":"46","B":"Work Voucher ","C":"1","D":"Mohammed Qasim","E":"1","F":null,"G":null},{"A":"Textbox31","B":"Branch","C":"Textbox49","D":"WhyUs2","E":"Textbox24","F":"Name2","G":"Textbox25"},{"A":"46","B":"Middleton","C":"46","D":"CUESPEARS to EE","E":"5","F":"Chunkit Fong","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"CUESPEARS to EE","E":"5","F":"Louise Bezyk","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"CUESPEARS to EE","E":"5","F":"Alec Copeland","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"CUESPEARS to EE","E":"5","F":"Chun Kit Fong","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"CUESPEARS to EE","E":"5","F":"Steven Hamer","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Existing Client","E":"4","F":"Sue Pownall","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Existing Client","E":"4","F":"Katie Palmer","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Existing Client","E":"4","F":"Emma Connolly","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Existing Client","E":"4","F":"Liyana Junaid","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Family & Friends ","E":"4","F":"Azizur Rehman","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Family & Friends ","E":"4","F":"Elizabeth Boott-Wroe ","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Family & Friends ","E":"4","F":"Joe Farry","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Family & Friends ","E":"4","F":"Alfie Watterson","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Google search","E":"3","F":"Deepa Scaria","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Google search","E":"3","F":"Kirsty Scanlan","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Google search","E":"3","F":"Bonatla Morupisi","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Local to the Area","E":"4","F":"Aleksandra Matuszczak","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Local to the Area","E":"4","F":"Aishai Junaid","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Local to the Area","E":"4","F":"Adrian Wawoczny","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Local to the Area","E":"4","F":"Tyreese Macfarlane","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"New CUES ","E":"7","F":"Shah Ali","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"New CUES ","E":"7","F":"Guelany Ketura Marie Guiri","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"New CUES ","E":"7","F":"Chris Hardcastle","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"New CUES ","E":"7","F":"Jaqueline Whitehead","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"New CUES ","E":"7","F":"Adil Arfan","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"New CUES ","E":"7","F":"John Barker","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"New CUES ","E":"7","F":"ANDREW RAWLINSON","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Prospectrn","E":"2","F":"James Taylor","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Prospectrn","E":"2","F":"Rachael Mccrystal","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Thomas McDermott","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Samuel Keqi","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Elizabeth McDermott","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"James McDermott","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Elsa Rush","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Philip Heywood","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Ciara Purcell","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Lee Northover","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Keith Robinson","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Denise Dale","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Liz Chadwick","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Nicholas Hamilton","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Referral","E":"13","F":"Janine Parry","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Reviews on Internet","E":"3","F":"Vincent Wood","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Reviews on Internet","E":"3","F":"Adele Platt","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Reviews on Internet","E":"3","F":"Mark Gibbons","G":"1"},{"A":"46","B":"Middleton","C":"46","D":"Work Voucher ","E":"1","F":"Mohammed Qasim","G":"1"}]']]"""
raw_data = '{ "data": [' + \
    raw_data.replace("[", "").replace("]", "").replace("'", "") + "]}"
dict_raw = json.loads(raw_data)
df = pd.DataFrame.from_records(dict_raw['data'])
print(df)
