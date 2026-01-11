"""
Migration script to convert CSV data to SQLite database.
Run this script once to migrate existing CSV data to the new SQLite database.
"""
import pandas as pd
import os
from datetime import datetime
from database_models import DatabaseManager, Player, Delta, VIP, VIPData, VIPDelta
import json


def migrate_csv_to_sqlite(data_folder='/var/data'):
    """Migrate all CSV data to SQLite."""
    print("Starting migration from CSV to SQLite...")
    
    db_manager = DatabaseManager(db_path=f'{data_folder}/ringts.db')
    session = db_manager.get_session()
    
    try:
        # Migrate exps.csv to players table
        exps_file = f'{data_folder}/exps.csv'
        if os.path.exists(exps_file):
            print(f"Migrating {exps_file}...")
            df_exps = pd.read_csv(exps_file, parse_dates=['last update'])
            
            for _, row in df_exps.iterrows():
                player = Player(
                    name=row['name'],
                    exp=int(row['exp']),
                    last_update=row['last update'],
                    world=row.get('world', 'Auroria'),
                    guild=row.get('guild', 'Ascended Auroria')
                )
                session.merge(player)
            
            session.commit()
            print(f"✓ Migrated {len(df_exps)} players")
        
        # Migrate deltas.csv to deltas table
        deltas_file = f'{data_folder}/deltas.csv'
        if os.path.exists(deltas_file):
            print(f"Migrating {deltas_file}...")
            df_deltas = pd.read_csv(deltas_file, parse_dates=['update time'])
            
            batch_size = 1000
            for i in range(0, len(df_deltas), batch_size):
                batch = df_deltas.iloc[i:i+batch_size]
                for _, row in batch.iterrows():
                    delta = Delta(
                        name=row['name'],
                        deltaexp=int(row['deltaexp']),
                        update_time=row['update time'],
                        world=row.get('world', 'Auroria'),
                        guild=row.get('guild', 'Ascended Auroria')
                    )
                    session.add(delta)
                
                session.commit()
                print(f"  Migrated {min(i+batch_size, len(df_deltas))}/{len(df_deltas)} deltas")
            
            print(f"✓ Migrated {len(df_deltas)} deltas")
        
        # Migrate vips.txt to vips table
        vips_file = f'{data_folder}/vips.txt'
        if os.path.exists(vips_file):
            print(f"Migrating {vips_file}...")
            with open(vips_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                
                if content:
                    try:
                        # Try JSON format first
                        vips_data = json.loads(content)
                    except json.JSONDecodeError:
                        # Fall back to CSV format
                        vips_data = []
                        for line in content.split('\n'):
                            line = line.strip()
                            if line and ',' in line:
                                parts = line.split(',', 1)
                                if len(parts) == 2:
                                    vips_data.append({'name': parts[0].strip(), 'world': parts[1].strip()})
                    
                    for vip_data in vips_data:
                        vip = VIP(name=vip_data['name'], world=vip_data['world'])
                        session.merge(vip)
                    
                    session.commit()
                    print(f"✓ Migrated {len(vips_data)} VIPs")
        
        # Migrate vipsdata.csv to vip_data table
        vipsdata_file = f'{data_folder}/vipsdata.csv'
        if os.path.exists(vipsdata_file):
            print(f"Migrating {vipsdata_file}...")
            df_vipsdata = pd.read_csv(vipsdata_file)
            
            for _, row in df_vipsdata.iterrows():
                vip_data = VIPData(
                    name=row['name'],
                    world=row['world'],
                    today_exp=int(row.get('today_exp', 0)),
                    today_online=float(row.get('today_online', 0.0))
                )
                session.merge(vip_data)
            
            session.commit()
            print(f"✓ Migrated {len(df_vipsdata)} VIP data entries")
        
        # Migrate deltavip.csv to vip_deltas table
        deltavip_file = f'{data_folder}/deltavip.csv'
        if os.path.exists(deltavip_file):
            print(f"Migrating {deltavip_file}...")
            df_deltavip = pd.read_csv(deltavip_file, parse_dates=['update_time'])
            
            for _, row in df_deltavip.iterrows():
                vip_delta = VIPDelta(
                    name=row['name'],
                    world=row['world'],
                    date=row['date'],
                    delta_exp=int(row['delta_exp']),
                    delta_online=float(row['delta_online']),
                    update_time=row['update_time']
                )
                session.add(vip_delta)
            
            session.commit()
            print(f"✓ Migrated {len(df_deltavip)} VIP deltas")
        
        print("\n✅ Migration completed successfully!")
        print(f"Database location: {data_folder}/ringts.db")
        print("\nYou can now backup and remove the old CSV files:")
        print(f"  - {data_folder}/exps.csv")
        print(f"  - {data_folder}/deltas.csv")
        print(f"  - {data_folder}/vipsdata.csv")
        print(f"  - {data_folder}/deltavip.csv")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        session.rollback()
        raise
    finally:
        session.close()
        db_manager.close()


if __name__ == '__main__':
    migrate_csv_to_sqlite()
