import sqlite3
from pprint import pprint

conn = sqlite3.connect('local.db')
cur = conn.cursor()
print('tables:')
for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY 1"):
    print(' -', r[0])

print('\nindicators columns:')
for r in cur.execute('PRAGMA table_info(indicators)'):
    print(r)

print('\nindicators sample:')
rows = list(cur.execute('SELECT id, key, name, fmt, category FROM indicators ORDER BY id DESC LIMIT 5'))
for r in rows:
    print(r)
