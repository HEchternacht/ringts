import pandas as pd
import shutil
from datetime import datetime

# Backup the original file
backup_file = 'var/data/deltas_backup_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'
shutil.copy('var/data/deltas.csv', backup_file)
print(f"Backup created: {backup_file}")

# Read the deltas file
df = pd.read_csv('var/data/deltas.csv')
print(f"Original records: {len(df)}")

# Convert update time to datetime
df['update time'] = pd.to_datetime(df['update time'])

# Find duplicates
duplicates = df[df.duplicated(subset=['name', 'update time'], keep=False)]
if not duplicates.empty:
    print(f"\nFound {len(duplicates)} duplicate records:")
    print(duplicates.sort_values(['update time', 'name']))
    
    # Show some examples
    print("\nExample duplicates:")
    example_name = duplicates['name'].iloc[0]
    example_time = duplicates['update time'].iloc[0]
    example_dupes = df[(df['name'] == example_name) & (df['update time'] == example_time)]
    print(example_dupes)

# Remove duplicates - keep the first occurrence
df_clean = df.drop_duplicates(subset=['name', 'update time'], keep='first')
removed = len(df) - len(df_clean)
print(f"\nRemoved {removed} duplicate records")
print(f"Clean records: {len(df_clean)}")

# Save cleaned file
df_clean.to_csv('var/data/deltas.csv', index=False)
print("Cleaned deltas.csv saved")
