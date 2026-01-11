#!/usr/bin/env python3
"""
Database cleanup script for Ring TS application.
This script fixes corrupted records in the SQLite database where numeric fields contain bytes data.
"""

import sys
import os

# Add the current directory to the path so we can import the database module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database_sqlalchemy import SQLAlchemyDatabase

def main():
    """Main cleanup function"""
    print("Ring TS Database Cleanup Tool")
    print("=" * 40)

    try:
        # Initialize database
        db = SQLAlchemyDatabase()
        print("✓ Database initialized")

        # Run cleanup
        print("Starting cleanup of corrupted records...")
        db.cleanup_corrupted_delta_records()

        print("✓ Database cleanup completed successfully!")

    except Exception as e:
        print(f"✗ Database cleanup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()