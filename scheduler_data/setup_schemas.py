#!/usr/bin/env python3
"""
Setup script to create the medallion architecture schemas in Snowflake
"""
import os
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError

def setup_medallion_schemas():
    """Create Bronze (raw), Silver, and Gold schemas for the project"""

    # Get connection parameters from environment
    connection_params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'role': os.getenv('SNOWFLAKE_ROLE')
    }

    try:
        print("🏗️  Setting up Medallion Architecture schemas...")
        conn = snowflake.connector.connect(**connection_params)
        cursor = conn.cursor()

        # Create schemas for medallion architecture
        schemas_to_create = [
            ('RAW', 'Bronze layer - Raw data as ingested from source'),
            ('SILVER', 'Silver layer - Cleaned and standardized data'),
            ('GOLD', 'Gold layer - Business-ready dimensional model')
        ]

        for schema_name, description in schemas_to_create:
            print(f"📁 Creating schema: {schema_name}")
            try:
                cursor.execute(f"""
                    CREATE SCHEMA IF NOT EXISTS {schema_name}
                    COMMENT = '{description}'
                """)
                print(f"✅ Schema {schema_name} created successfully")
            except Exception as e:
                print(f"⚠️  Schema {schema_name} may already exist: {e}")

        # Verify schemas exist
        print("\n🔍 Verifying created schemas...")
        cursor.execute("SHOW SCHEMAS")
        schemas = cursor.fetchall()

        existing_schemas = [row[1] for row in schemas]  # Schema name is in column 1

        for schema_name, _ in schemas_to_create:
            if schema_name in existing_schemas:
                print(f"✅ {schema_name} schema verified")
            else:
                print(f"❌ {schema_name} schema missing")

        # Show current database info
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        db_info = cursor.fetchone()
        print(f"\n🎯 Current context:")
        print(f"   Database: {db_info[0]}")
        print(f"   Default Schema: {db_info[1]}")

        cursor.close()
        conn.close()

        print("\n🎉 Medallion architecture setup completed!")
        return True

    except Exception as e:
        print(f"❌ Error setting up schemas: {e}")
        return False

if __name__ == "__main__":
    success = setup_medallion_schemas()
    exit(0 if success else 1)