import pandas as pd

class SQL:
    def __init__(self, sql, **bindings):
        self.sql = sql
        self.bindings = bindings

df = pd.read_html(
        "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)",
    )[0]

s = SQL("select * from $df", df=df)
s1 = SQL("select * from $s", s=s)

print(s1.bindings)