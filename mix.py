import os

import pandas as pd

output_dir = "profiles_batches"
merged_file = "profiles_merged.xlsx"

all_files = [
    os.path.join(output_dir, f)
    for f in os.listdir(output_dir)
    if f.endswith(".xlsx")
]

merged_df = pd.concat([pd.read_excel(f) for f in all_files], ignore_index=True)
merged_df.to_excel(merged_file, index=False)
print(f"🎉 All batches merged into {merged_file}")
