import requests
import json

r = requests.get('http://localhost:8050/api/comparacion', timeout=5)
print('Status:', r.status_code)
data = r.json()
print("Año  | Total Estado (MM)     | Total Cttos  | Sector TIC (MM)     | Cttos TIC   | % TIC")
print("-" * 80)
for row in data:
    print(f"{row['anio']} | ${row['total_mm']:>17,.2f} | {row['total_contratos']:>12,} | ${row['tic_mm']:>15,.2f} | {row['tic_contratos']:>11,} | {row['pct_tic']:>5.2f}%")
