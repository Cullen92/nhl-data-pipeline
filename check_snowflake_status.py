#!/usr/bin/env python3
"""
Check Snowflake database status and list all tables.
"""
import os
import sys
from datetime import datetime

try:
    import snowflake.connector
    from snowflake.connector import DictCursor
except ImportError:
    print("Error: snowflake-connector-python not installed")
    print("Install with: pip install snowflake-connector-python")
    sys.exit(1)


def get_snowflake_connection():
    """Create a Snowflake connection using environment variables."""
    required_vars = [
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account='XWOLWMN-IH17965',
            warehouse='NHL_WH',
            database='NHL',
            role='ACCOUNTADMIN'
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        sys.exit(1)


def check_database_status(conn):
    """Check database, schemas, and tables."""
    cursor = conn.cursor(DictCursor)
    
    print("\n" + "="*80)
    print("SNOWFLAKE DATABASE STATUS CHECK")
    print("="*80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Database: NHL")
    print("Account: XWOLWMN-IH17965")
    
    # Check warehouse status
    print("\n" + "-"*80)
    print("WAREHOUSE STATUS")
    print("-"*80)
    try:
        cursor.execute("SHOW WAREHOUSES LIKE 'NHL_WH'")
        wh = cursor.fetchone()
        if wh:
            print(f"✓ Warehouse: {wh['name']}")
            print(f"  State: {wh['state']}")
            print(f"  Size: {wh['size']}")
        else:
            print("✗ Warehouse NHL_WH not found")
    except Exception as e:
        print(f"✗ Error checking warehouse: {e}")
    
    # Check schemas
    print("\n" + "-"*80)
    print("SCHEMAS")
    print("-"*80)
    try:
        cursor.execute("SHOW SCHEMAS IN DATABASE NHL")
        schemas = cursor.fetchall()
        for schema in schemas:
            if schema['name'] not in ['INFORMATION_SCHEMA']:
                print(f"✓ {schema['name']}")
    except Exception as e:
        print(f"✗ Error listing schemas: {e}")
    
    # Check tables by schema
    print("\n" + "-"*80)
    print("TABLES BY SCHEMA")
    print("-"*80)
    
    schemas_to_check = ['STAGING', 'BRONZE', 'SILVER', 'GOLD']
    total_tables = 0
    
    for schema in schemas_to_check:
        try:
            cursor.execute(f"SHOW TABLES IN SCHEMA NHL.{schema}")
            tables = cursor.fetchall()
            
            if tables:
                print(f"\n{schema} Schema ({len(tables)} tables):")
                for table in tables:
                    # Get row count and last update
                    try:
                        cursor.execute(f"SELECT COUNT(*) as cnt FROM NHL.{schema}.{table['name']}")
                        count_result = cursor.fetchone()
                        row_count = count_result['CNT'] if count_result else 0
                        
                        print(f"  • {table['name']:<40} {row_count:>12,} rows")
                        total_tables += 1
                    except Exception:
                        print(f"  • {table['name']:<40} (error getting count)")
            else:
                print(f"\n{schema} Schema: (empty)")
        except Exception:
            print(f"\n{schema} Schema: Not accessible or doesn't exist")
    
    # Check views
    print("\n" + "-"*80)
    print("VIEWS")
    print("-"*80)
    
    for schema in schemas_to_check:
        try:
            cursor.execute(f"SHOW VIEWS IN SCHEMA NHL.{schema}")
            views = cursor.fetchall()
            
            if views:
                print(f"\n{schema} Schema ({len(views)} views):")
                for view in views:
                    print(f"  • {view['name']}")
        except Exception:
            pass
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total tables found: {total_tables}")
    
    # Check latest data freshness
    print("\n" + "-"*80)
    print("DATA FRESHNESS CHECK")
    print("-"*80)
    
    freshness_checks = [
        ("BRONZE.BRONZE_SCHEDULE_SNAPSHOTS", "SNAPSHOT_TIMESTAMP"),
        ("BRONZE.BRONZE_GAME_BOXSCORE_SNAPSHOTS", "SNAPSHOT_TIMESTAMP"),
        ("BRONZE.BRONZE_GAME_PBP_SNAPSHOTS", "SNAPSHOT_TIMESTAMP"),
    ]
    
    for table_name, timestamp_col in freshness_checks:
        try:
            query = f"""
            SELECT MAX({timestamp_col}) as latest_date
            FROM NHL.{table_name}
            """
            cursor.execute(query)
            result = cursor.fetchone()
            if result and result['LATEST_DATE']:
                latest = result['LATEST_DATE']
                print(f"  {table_name}: {latest}")
            else:
                print(f"  {table_name}: No data")
        except Exception:
            print(f"  {table_name}: Table not found or error")
    
    cursor.close()


def main():
    """Main function."""
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    
    try:
        check_database_status(conn)
        print("\n✓ Database check complete!\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
