#!/usr/bin/env python3
"""
Test script to verify Snowflake connection using Mage secrets
"""
import os
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError

def test_snowflake_connection():
    """Test Snowflake connection with environment variables"""

    # Get connection parameters from environment
    connection_params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'role': os.getenv('SNOWFLAKE_ROLE'),
        'schema': 'raw'
    }

    # Check if all required parameters are present
    missing_params = [key for key, value in connection_params.items() if not value]
    if missing_params:
        print(f"❌ Missing environment variables: {missing_params}")
        return False

    try:
        print("🔗 Attempting to connect to Snowflake...")
        print(f"   Account: {connection_params['account']}")
        print(f"   User: {connection_params['user']}")
        print(f"   Database: {connection_params['database']}")
        print(f"   Warehouse: {connection_params['warehouse']}")
        print(f"   Role: {connection_params['role']}")

        # Establish connection
        conn = snowflake.connector.connect(**connection_params)

        # Test with a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")
        result = cursor.fetchone()

        print("✅ Connection successful!")
        print(f"   Snowflake Version: {result[0]}")
        print(f"   Current Warehouse: {result[1]}")
        print(f"   Current Database: {result[2]}")
        print(f"   Current Schema: {result[3]}")
        print(f"   Current Role: {result[4]}")

        # Test creating a simple table in raw schema
        print("\n🧪 Testing table creation permissions...")
        cursor.execute("""
            CREATE OR REPLACE TABLE raw.connection_test (
                test_id INTEGER,
                test_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                test_message STRING
            )
        """)

        # Insert test data
        cursor.execute("""
            INSERT INTO raw.connection_test (test_id, test_message)
            VALUES (1, 'Connection test successful')
        """)

        # Query test data
        cursor.execute("SELECT * FROM raw.connection_test")
        test_result = cursor.fetchone()
        print(f"   Test table created and data inserted: {test_result}")

        # Clean up test table
        cursor.execute("DROP TABLE raw.connection_test")
        print("   Test table cleaned up")

        cursor.close()
        conn.close()

        return True

    except DatabaseError as e:
        print(f"❌ Database Error: {e}")
        return False
    except ProgrammingError as e:
        print(f"❌ Programming Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return False

if __name__ == "__main__":
    success = test_snowflake_connection()
    exit(0 if success else 1)