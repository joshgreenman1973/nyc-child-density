import csv, json
study = [
    ("36","005","Bronx"),("36","047","Brooklyn (Kings)"),("36","061","Manhattan (NY)"),
    ("36","081","Queens"),("36","085","Staten Is. (Richmond)"),
    ("36","059","Nassau"),("36","119","Westchester"),
    ("34","003","Bergen"),("34","017","Hudson"),("34","023","Middlesex"),("34","039","Union"),
]
out = {c[2]:{"fips":c[0]+c[1],"group":("nyc" if c[0]+c[1] in {"36005","36047","36061","36081","36085"} else "suburb"),"years":{}} for c in study}

# 2010-2019 file
with open("/tmp/co-est2019.csv", encoding="latin-1") as f:
    r = csv.DictReader(f)
    for row in r:
        key = (row["STATE"], row["COUNTY"])
        for c in study:
            if (c[0], c[1]) == key:
                for y in range(2011, 2020):
                    out[c[2]]["years"][str(y)] = {
                        "natural": int(row[f"NATURALINC{y}"]),
                        "netmig":  int(row[f"NETMIG{y}"]),
                    }
# 2020-2024 file
with open("/tmp/co-est.csv", encoding="latin-1") as f:
    r = csv.DictReader(f)
    for row in r:
        key = (row["STATE"], row["COUNTY"])
        for c in study:
            if (c[0], c[1]) == key:
                for y in range(2020, 2025):
                    out[c[2]]["years"][str(y)] = {
                        "natural": int(row[f"NATURALCHG{y}"]),
                        "netmig":  int(row[f"NETMIG{y}"]),
                    }

# Cumulative 2011-2024
for name, d in out.items():
    nat = sum(d["years"][str(y)]["natural"] for y in range(2011,2025))
    mig = sum(d["years"][str(y)]["netmig"] for y in range(2011,2025))
    d["cum"] = {"natural": nat, "netmig": mig, "total": nat+mig}

print(json.dumps(out, indent=2))
