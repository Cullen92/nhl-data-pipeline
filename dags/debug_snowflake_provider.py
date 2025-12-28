"""
Diagnostic DAG to check Snowflake provider installation and availability.
Run this manually to see detailed import and provider status in task logs.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

def check_snowflake_packages():
    """Check if Snowflake packages are installed and importable."""
    import sys
    import subprocess
    
    logging.info("=" * 80)
    logging.info("SNOWFLAKE PROVIDER DIAGNOSTICS")
    logging.info("=" * 80)
    
    # Check Python version
    logging.info(f"\nPython version: {sys.version}")
    logging.info(f"Python executable: {sys.executable}")
    
    # Check installed packages
    logging.info("\n" + "=" * 80)
    logging.info("INSTALLED PACKAGES (snowflake, airflow-providers)")
    logging.info("=" * 80)
    try:
        result = subprocess.run(
            ["pip", "list", "--format=freeze"],
            capture_output=True,
            text=True,
            check=True
        )
        for line in result.stdout.split('\n'):
            if 'snowflake' in line.lower() or 'apache-airflow-providers' in line.lower():
                logging.info(line)
    except Exception as e:
        logging.error(f"Failed to list packages: {e}")
    
    # Test snowflake-connector-python
    logging.info("\n" + "=" * 80)
    logging.info("TEST: Import snowflake.connector")
    logging.info("=" * 80)
    try:
        import snowflake.connector
        logging.info(f"✓ SUCCESS: snowflake.connector imported")
        logging.info(f"  Version: {snowflake.connector.__version__}")
    except ImportError as e:
        logging.error(f"✗ FAILED: Cannot import snowflake.connector")
        logging.error(f"  Error: {e}")
    except Exception as e:
        logging.error(f"✗ ERROR: {e}")
    
    # Test snowflake-sqlalchemy
    logging.info("\n" + "=" * 80)
    logging.info("TEST: Import snowflake.sqlalchemy")
    logging.info("=" * 80)
    try:
        import snowflake.sqlalchemy
        logging.info(f"✓ SUCCESS: snowflake.sqlalchemy imported")
    except ImportError as e:
        logging.error(f"✗ FAILED: Cannot import snowflake.sqlalchemy")
        logging.error(f"  Error: {e}")
    except Exception as e:
        logging.error(f"✗ ERROR: {e}")
    
    # Test Airflow Snowflake provider
    logging.info("\n" + "=" * 80)
    logging.info("TEST: Import Airflow Snowflake Provider")
    logging.info("=" * 80)
    try:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        logging.info(f"✓ SUCCESS: SnowflakeHook imported")
    except ImportError as e:
        logging.error(f"✗ FAILED: Cannot import SnowflakeHook")
        logging.error(f"  Error: {e}")
        logging.error(f"  This means apache-airflow-providers-snowflake is not properly installed")
    except Exception as e:
        logging.error(f"✗ ERROR: {e}")
    
    # Check if provider is registered with Airflow
    logging.info("\n" + "=" * 80)
    logging.info("TEST: Check if Snowflake provider is registered")
    logging.info("=" * 80)
    try:
        from airflow.providers_manager import ProvidersManager
        pm = ProvidersManager()
        providers = pm.providers
        
        logging.info(f"Total providers loaded: {len(providers)}")
        
        if 'apache-airflow-providers-snowflake' in providers:
            logging.info(f"✓ SUCCESS: Snowflake provider IS registered")
            provider_info = providers['apache-airflow-providers-snowflake']
            logging.info(f"  Version: {provider_info.version}")
            logging.info(f"  Hooks: {provider_info.hook_class_names}")
        else:
            logging.error(f"✗ FAILED: Snowflake provider NOT registered")
            logging.info(f"\nRegistered providers containing 'snow' or 'sql':")
            for prov_name in sorted(providers.keys()):
                if 'snow' in prov_name.lower() or 'sql' in prov_name.lower():
                    logging.info(f"  - {prov_name}")
    except Exception as e:
        logging.error(f"✗ ERROR checking providers: {e}")
    
    # Check common-sql provider (which IS working)
    logging.info("\n" + "=" * 80)
    logging.info("TEST: Check Common SQL provider (for comparison)")
    logging.info("=" * 80)
    try:
        from airflow.providers_manager import ProvidersManager
        pm = ProvidersManager()
        if 'apache-airflow-providers-common-sql' in pm.providers:
            logging.info(f"✓ Common SQL provider IS registered (working)")
            provider_info = pm.providers['apache-airflow-providers-common-sql']
            logging.info(f"  Version: {provider_info.version}")
        else:
            logging.info(f"Common SQL provider not found")
    except Exception as e:
        logging.error(f"Error: {e}")
    
    logging.info("\n" + "=" * 80)
    logging.info("DIAGNOSTICS COMPLETE")
    logging.info("=" * 80)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    'debug_snowflake_provider',
    default_args=default_args,
    description='Diagnose Snowflake provider installation issues',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['debug', 'diagnostic'],
)

diagnose_task = PythonOperator(
    task_id='check_snowflake_packages',
    python_callable=check_snowflake_packages,
    dag=dag,
)
