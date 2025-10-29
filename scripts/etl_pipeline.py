"""
Project Performance Analytics - ETL Pipeline
Automates data extraction, transformation, and loading for KPI tracking
"""

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import sys

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# ============================================
# CONFIGURATION
# ============================================

DB_CONFIG = {
    'user': os.getenv('USER'),
    'password': os.getenv('PASSWORD'),
    'host': os.getenv('HOST'),
    'database': os.getenv('DATABASE')
}


CSV_FILES = {
    'marketing': 'marketing_projects.csv',
    'operations': 'operations_projects.csv',
    'it': 'it_projects.csv'
}

# ============================================
# DATABASE CONNECTION
# ============================================
def create_db_connection():
    """Create SQLAlchemy engine for database connection"""
    try:
        connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        logging.info("Database connection established successfully")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to database: {str(e)}")
        raise

# ============================================
# DATA EXTRACTION
# ============================================
def extract_csv_data(file_path):
    """Extract data from CSV file"""
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully extracted {len(df)} records from {file_path}")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error reading {file_path}: {str(e)}")
        raise

# ============================================
# DATA TRANSFORMATION
# ============================================
def transform_data(df):
    """
    Transform data by adding calculated columns:
    - Delay_Days: Duration between start and end date
    - Cost_Variance_%: Percentage variance between planned and actual cost
    - On_Time: Boolean indicating if project completed on time
    - Completion_Rate: Percentage of tasks completed
    """
    try:
        # Convert date columns to datetime
        df['Start_Date'] = pd.to_datetime(df['Start_Date'])
        df['End_Date'] = pd.to_datetime(df['End_Date'])
        
        # Calculate delay in days
        df['Delay_Days'] = (df['End_Date'] - df['Start_Date']).dt.days
        
        # Calculate cost variance percentage
        df['Cost_Variance_%'] = ((df['Actual_Cost'] - df['Planned_Cost']) / df['Planned_Cost']) * 100
        
        # Determine if project is on time
        df['On_Time'] = df['Actual_Completion'] >= df['Planned_Completion']
        
        # Calculate completion rate
        df['Completion_Rate_%'] = (df['Actual_Completion'] / df['Planned_Completion']) * 100
        
        # Round numerical columns for readability
        df['Cost_Variance_%'] = df['Cost_Variance_%'].round(2)
        df['Completion_Rate_%'] = df['Completion_Rate_%'].round(2)
        
        logging.info(f"Data transformation completed for {len(df)} records")
        return df
        
    except Exception as e:
        logging.error(f"Error during data transformation: {str(e)}")
        raise

# ============================================
# DATA LOADING
# ============================================
def load_to_database(df, table_name, engine):
    """Load transformed data to SQL database"""
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Successfully loaded {len(df)} records to table '{table_name}'")
    except Exception as e:
        logging.error(f"Error loading data to {table_name}: {str(e)}")
        raise

# ============================================
# CONSOLIDATION
# ============================================
def consolidate_departments(engine):
    """
    Combine data from all department tables into consolidated_projects table
    """
    try:
        # Read data from each department table
        marketing_df = pd.read_sql('SELECT * FROM marketing_projects', engine)
        ops_df = pd.read_sql('SELECT * FROM operations_projects', engine)
        it_df = pd.read_sql('SELECT * FROM it_projects', engine)
        
        logging.info(f"Extracted - Marketing: {len(marketing_df)}, Operations: {len(ops_df)}, IT: {len(it_df)} records")
        
        # Concatenate all dataframes
        all_projects = pd.concat([marketing_df, ops_df, it_df], ignore_index=True)
        
        # Add timestamp for when consolidation was performed
        all_projects['Last_Updated'] = datetime.now()
        
        # Load consolidated data
        load_to_database(all_projects, 'consolidated_projects', engine)
        
        logging.info(f"Successfully consolidated {len(all_projects)} total records")
        return all_projects
        
    except Exception as e:
        logging.error(f"Error during consolidation: {str(e)}")
        raise

# ============================================
# KPI CALCULATION
# ============================================
def calculate_kpis(df):
    """Calculate key performance indicators"""
    try:
        # Ensure expected columns exist
        required_cols = ['On_Time', 'Cost_Variance_%', 'Delay_Days', 'Completion_Rate_%', 'Planned_Cost', 'Actual_Cost']
        for col in required_cols:
            if col not in df.columns:
                raise KeyError(f"Missing expected column: {col}")
        
        # Convert numeric columns safely
        df['Cost_Variance_%'] = pd.to_numeric(df['Cost_Variance_%'], errors='coerce')
        df['Delay_Days'] = pd.to_numeric(df['Delay_Days'], errors='coerce')
        df['Completion_Rate_%'] = pd.to_numeric(df['Completion_Rate_%'], errors='coerce')
        df['Planned_Cost'] = pd.to_numeric(df['Planned_Cost'], errors='coerce')
        df['Actual_Cost'] = pd.to_numeric(df['Actual_Cost'], errors='coerce')

        # Convert On_Time to boolean (if not already)
        df['On_Time'] = df['On_Time'].astype(str).str.lower().isin(['yes', 'true', '1', 'on time'])

        # Fill NaNs
        df = df.fillna(0)

        kpis = {
            'total_projects': len(df),
            'on_time_delivery_%': round(df['On_Time'].sum() / len(df) * 100, 2) if len(df) else 0,
            'avg_cost_variance_%': round(df['Cost_Variance_%'].mean(), 2),
            'avg_delay_days': round(df['Delay_Days'].mean(), 2),
            'avg_completion_rate_%': round(df['Completion_Rate_%'].mean(), 2),
            'total_planned_cost': round(df['Planned_Cost'].sum(), 2),
            'total_actual_cost': round(df['Actual_Cost'].sum(), 2),
            'projects_over_budget': len(df[df['Cost_Variance_%'] > 0]),
            'projects_delayed': len(df[~df['On_Time']])
        }

        logging.info("KPI Calculations:")
        for key, value in kpis.items():
            logging.info(f"  {key}: {value}")

        return kpis

    except Exception as e:
        logging.error(f"Error calculating KPIs: {str(e)}")
        raise


# ============================================
# MAIN ETL PIPELINE
# ============================================
def run_etl_pipeline():
    """Execute the complete ETL pipeline"""
    logging.info("="*50)
    logging.info("Starting ETL Pipeline Execution")
    logging.info("="*50)
    
    try:
        # Step 1: Create database connection
        engine = create_db_connection()
        
        # Step 2: Extract, Transform, and Load each department's data
        for dept_name, csv_file in CSV_FILES.items():
            logging.info(f"\nProcessing {dept_name.upper()} department...")
            
            # Extract
            df = extract_csv_data(csv_file)
            
            # Transform
            df = transform_data(df)
            
            # Load
            table_name = f"{dept_name}_projects"
            load_to_database(df, table_name, engine)
        
        # Step 3: Consolidate all departments
        logging.info("\nConsolidating all departments...")
        consolidated_df = consolidate_departments(engine)
        
        # Step 4: Calculate KPIs
        logging.info("\nCalculating KPIs...")
        kpis = calculate_kpis(consolidated_df)
        
        logging.info("\n" + "="*50)
        logging.info("ETL Pipeline Completed Successfully!")
        logging.info("="*50)
        
        return True
        
    except Exception as e:
        logging.error(f"\nETL Pipeline Failed: {str(e)}")
        return False

# ============================================
# EXECUTION
# ============================================
if __name__ == "__main__":
    success = run_etl_pipeline()
    sys.exit(0 if success else 1)