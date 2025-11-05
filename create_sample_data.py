import pandas as pd
from pathlib import Path

Path("data/demo").mkdir(parents=True, exist_ok=True)

df = pd.DataFrame([
    ["Main_Page", "Python_(programming_language)", "link", 153],
    ["Python_(programming_language)", "Guido_van_Rossum", "link", 42],
    ["Main_Page", "C_(programming_language)", "link", 88],
    ["C_(programming_language)", "Dennis_Ritchie", "link", 31],
])

output_path = Path("data/demo/clickstream-enwiki-2025-09-sample.tsv")
df.to_csv(output_path, sep="\t", header=False, index=False)
print(f"✅ Wrote {output_path}")