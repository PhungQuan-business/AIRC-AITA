from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import numpy as np
import os
import logging


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 13),
    'email': ['kilancauvonglaplanh@gmail.com'],  
    'email_on_failure': True,  # Enable email notifications on failure
    'email_on_retry': False,
    'retries': 2,  # Increase retry attempts
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,  # Enable exponential backoff
    'max_retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(hours=2),  # Set execution timeout
}

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pandas as pd
import logging
import psycopg2
from airflow.models import Variable

dag = DAG(
    'canada_housing_data_cleaning_ver1',
    default_args=default_args,
    description='A DAG to clean Canada housing data from PostgreSQL',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['data_cleaning', 'canada_housing'],
    doc_md="""
    # Canada Housing Data Cleaning
    
    This DAG processes raw housing data from multiple Canadian provinces stored in PostgreSQL and 
    performs various cleaning operations to prepare it for analysis.
    
    ## Data Flow
    1. Test database connection
    2. Load data from PostgreSQL tables
    3. Clean and standardize different property features
    4. Export cleaned data to CSV
    """,
)

# Database connection parameters
DB_PARAMS_LANDING = {
    'host': '192.168.88.146',
    'database': 'landing-zone',
    'user': 'airc',
    'password': 'admin',
    'port': '5432'  # default PostgreSQL port
}

DB_PARAMS_FORMARTTED = {
    'host': '192.168.88.146',
    'database': 'formatted-zone',
    'user': 'airc',
    'password': 'admin',
    'port': '5432'  # default PostgreSQL port
}


def test_db_connection(**kwargs):
    """Test the connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_PARAMS_LANDING)
        cursor = conn.cursor()
        # Test query to check connection
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        
        logging.info(f"Successfully connected to PostgreSQL. Version: {db_version}")
        kwargs['ti'].xcom_push(key='db_connection_status', value='success')
        
        cursor.close()
        conn.close()
        return "Database connection test successful"
    except Exception as e:
        logging.error(f"Database connection failed: {str(e)}")
        kwargs['ti'].xcom_push(key='db_connection_status', value='failed')
        raise

def load_data(**kwargs):
    """Load data from PostgreSQL tables into individual Pandas DataFrames and concatenate them"""
    try:
        # Check if connection test was successful
        # connection_status = kwargs['ti'].xcom_pull(key='db_connection_status')
        # if connection_status != 'success':
        #     raise ValueError("Database connection test failed")

        # List of province codes
        # provinces = ['ab', 'bc', 'mb', 'nb', 'nl', 'ns', 'nt', 'on', 'pe', 'sk', 'yt']
        provinces = ['nt', 'yt'] # 
        # Load each province's data
        dataframes = []
        conn = psycopg2.connect(**DB_PARAMS_LANDING)
        
        for province in provinces:
            # Assuming table names follow a pattern like 'housing_data_ab'
            table_name = f'data_{province}'
            try:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql_query(query, conn)
                
                if df.empty:
                    kwargs['ti'].xcom_push(key=f'empty_table_{province}', value=table_name)
                    continue
                    
                kwargs['ti'].xcom_push(key=f'rows_{province}', value=len(df))
                dataframes.append(df)
            except Exception as e:
                logging.warning(f"No data found for {table_name}: {str(e)}")
                kwargs['ti'].xcom_push(key=f'missing_table_{province}', value=table_name)
                continue
        
        conn.close()
        
        # Concatenate all DataFrames
        if not dataframes:
            raise ValueError("No data tables were found")
            
        df = pd.concat(dataframes, axis=0) #  chỉnh lại để số nhỏ hơn
        
        df = df.iloc[:, :-4]
        
        # Log total number of rows
        kwargs['ti'].xcom_push(key='total_rows_raw', value=len(df))
        
        # Drop duplicates
        df = df.drop_duplicates()
        
        # Log rows after removing duplicates
        kwargs['ti'].xcom_push(key='rows_after_dedup', value=len(df))
        
        # Drop duplicate columns
        df = df.T.drop_duplicates().T
        
        # Drop rows with null values in essential columns
        df = df.dropna(subset=["streetAddress", "addressLocality", "addressRegion", "price"])
        
        # Log rows after removing nulls
        kwargs['ti'].xcom_push(key='rows_after_dropna', value=len(df))
        
        # Store the DataFrame for the next task
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json(orient='split'))
        
        return "Data loaded and initial cleaning complete"
    except Exception as e:
        logging.error(f"Error in load_data: {str(e)}")
        raise


def clean_square_footage(**kwargs):
    """Clean and combine square footage data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='raw_data', task_ids='load_data')
    df = pd.read_json(df_json, orient='split')
    
    # Clean 'Square Footage' column
    df.loc[:, 'Square_Footage_new'] = df['Square_Footage'].str.replace(' SQFT', '', regex=False)
    df.loc[:, 'Square_Footage_new'] = df['Square_Footage_new'].fillna(df['property_sqft'])
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_square_footage', value=df.to_json(orient='split'))
    
    return "Square footage data cleaned"

def clean_acreage(**kwargs):
    """Clean acreage data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_square_footage', task_ids='clean_square_footage')
    df = pd.read_json(df_json, orient='split')
    
    # Fill NaN values in 'Acreage' with 0
    df["Acreage"] = df["Acreage"].fillna(0)
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_acreage', value=df.to_json(orient='split'))
    
    return "Acreage data cleaned"

def clean_bathrooms(**kwargs):
    """Clean bathroom data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_acreage', task_ids='clean_acreage')
    df = pd.read_json(df_json, orient='split')
    
    # Fix incorrect bathroom values
    df.loc[df['property_baths'] == 803, 'property_baths'] = 1
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_bathrooms', value=df.to_json(orient='split'))
    
    return "Bathroom data cleaned"

def extract_unique_values(column, df):
    """Helper function to extract unique values from a list-like column."""
    unique_features = set()
    for features in df[column].dropna():
        # Clean and split the features
        cleaned_features = features.replace("[", "").replace("]", "").replace('"', '').replace("'", "")
        feature_list = [f.strip() for f in cleaned_features.split(",")]
        unique_features.update(feature_list)
    return unique_features

def check_keywords(features, keywords):
    """Helper function to check if a string contains any of the specified keywords."""
    try:
        cleaned_features = features.replace("[", "").replace("]", "").replace('"', '').replace("'", "").replace(";", ",")
        list_to_check = [f.strip().lower() for f in cleaned_features.split(',')]
        common = set(list_to_check) & set(keywords)
        if common: 
            return 'Yes'  
    except AttributeError:
        return 'No'
    return 'No'

def check_type(features, types):
    """Helper function to categorize features based on predefined types."""
    try:
        cleaned_features = features.replace("[", "").replace("]", "").replace('"', '').replace("'", "").replace(";", ",")
        list_to_check = [f.strip().lower() for f in cleaned_features.split(',')]
        for type_name, type_list in types.items():
            if set(list_to_check) & set(type_list): 
                return type_name
        return np.nan
    except AttributeError:
        return np.nan

def clean_garage_parking(**kwargs):
    """Clean garage and parking data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_bathrooms', task_ids='clean_bathrooms')
    df = pd.read_json(df_json, orient='split')
    
    # Process garage data
    garage_keywords = {"garage", "carport", "grge", "attached", "detached", "tandem", "car"}
    garage_list = []
    
    # Get unique values from Features and Parking columns
    unique_features = extract_unique_values('Features', df)
    unique_parking = extract_unique_values('Parking', df)
    unique_parking_features = unique_features | unique_parking
    
    # Create a list of garage-related terms
    for item in unique_parking_features:
        lower_item = item.lower()
        if any(keyword in lower_item for keyword in garage_keywords):
            garage_list.append(item.lower())
    
    # Remove 'no garage' if present
    if 'no garage' in garage_list:
        garage_list.remove('no garage')
    
    # Clean garage data
    df['Garage new'] = df['Garage']
    df.loc[df.Garage.isna(), 'Garage new'] = df.loc[df.Garage.isna(), 'Features'].apply(lambda x: check_keywords(x, garage_list))
    df.loc[df.Garage == 'No', 'Garage new'] = df.loc[df.Garage == 'No', 'Parking'].apply(lambda x: check_keywords(x, garage_list))
    
    # Clean parking data
    parking_list = ["2 outdoor stalls", "add. parking avail.", "additional parking", "assigned",
                    "carport", "carport & garage", "carport double", "carport quad+", "carport triple", 
                    "carport; multiple", "carport; single", "multiple driveways", "parkade", "parking lot", 
                    "parking pad", "parking pad cement/paved", "parking space(s)", "parking spaces", 
                    "rv", "rv access/parking", "rv gated", "rv hookup", "rv parking", "rv parking avail.", 
                    "shared driveway", "stall", "tandem parking", "underground", "underground parking", 
                    "visitor parking"]
    
    df['Parking_new'] = df['Parking']
    df['Parking_new'] = df['Parking_new'].apply(lambda x: check_keywords(x, parking_list))
    df.loc[df['Parking_new'] == 'No', 'Parking_new'] = df.loc[df['Parking_new'] == 'No', 'Features'].apply(lambda x: check_keywords(x, parking_list))
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_garage_parking', value=df.to_json(orient='split'))
    
    return "Garage and parking data cleaned"

def clean_basement(**kwargs):
    """Clean basement data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_garage_parking', task_ids='clean_garage_parking')
    df = pd.read_json(df_json, orient='split')
    
    # Define basement categories
    basement_types = {
        'Finished': ['full basement', 'full', 'dugout', 'fully finished', 'finished', 'remodeled basement', 
                     'apartment in basement', 'suite', 'walk-out access', 'walk-out to grade', 'walk-up to grade', 
                     'walk-out', 'walkout', 'walk-up', 'with windows', 'separate/exterior entry', 'separate entrance'], 
        'Partial': ['partially finished', 'partial', 'partially finished', 'partial basement', 
                    'not full height', 'cellar'], 
        'No basement': ['unfinished', 'no basement', 'slab', 'crawl space', 'crawl', 'none', 
                        'not applicable', 'n/a']
    }
    
    # Apply the categorization
    df['Basement_new'] = df['Basement']
    df['Basement_new'] = df['Basement_new'].apply(lambda x: check_type(x, basement_types))
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_basement', value=df.to_json(orient='split'))
    
    return "Basement data cleaned"

def clean_exterior(**kwargs):
    """Clean exterior data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_basement', task_ids='clean_basement')
    df = pd.read_json(df_json, orient='split')
    
    # Define exterior categories
    exterior_materials = {
        'Metal': ['aluminum', 'aluminum siding', 'aluminum/vinyl', 'colour loc', 'metal', 'steel'],
        'Brick': ['brick', 'brick facing', 'brick imitation', 'brick veneer'],
        'Concrete': ['concrete', 'concrete siding', 'concrete/stucco', 'concrete block', 'insul brick', 'stucco'],
        'Wood': ['cedar', 'cedar shingles', 'cedar siding', 'wood', 'wood siding', 'wood shingles', 'wood shingles', 'wood siding'],
        'Composite': ['composite siding', 'hardboard', 'masonite', 'shingles'],
        'Vinyl': ['vinyl', 'vinyl siding', 'asbestos', 'siding']
    }
    
    # Apply the categorization
    df['Exterior_new'] = df['Exterior']
    df['Exterior_new'] = df['Exterior_new'].apply(lambda x: check_type(x, exterior_materials))
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_exterior', value=df.to_json(orient='split'))
    
    return "Exterior data cleaned"

def clean_fireplace(**kwargs):
    """Clean fireplace data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_exterior', task_ids='clean_exterior')
    df = pd.read_json(df_json, orient='split')
    
    # Convert fireplace data to Yes/No format
    df['Fireplace_new'] = df['Fireplace']
    df.loc[df['Fireplace_new'].isin(['0', '[]', '["0"]']), 'Fireplace_new'] = np.nan
    df.loc[df['Fireplace_new'].notna(), 'Fireplace_new'] = 'Yes'
    df.loc[~df['Fireplace_new'].notna(), 'Fireplace_new'] = 'No'
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_fireplace', value=df.to_json(orient='split'))
    
    return "Fireplace data cleaned"

def clean_heating(**kwargs):
    """Clean heating data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_fireplace', task_ids='clean_fireplace')
    df = pd.read_json(df_json, orient='split')
    
    # Define heating categories
    heating_categories = {
        'forced air': ['forced air', 'forced air-1', 'forced air-2', 'furnace'],
        'boiler': ['boiler', 'hot water', 'hot water radiator heat', 'steam', 'steam radiator'],
        'heat pump': ['central heat pump', 'heat pump', 'wall mounted heat pump'],
        'radiant': ['radiant', 'radiant heat', 'radiant ceiling', 'radiant floor', 
                    'radiant/infra-red heat', 'baseboard', 'baseboard heaters', 'electric baseboard units'],
        'fireplace': ['fireplace(s)', 'fireplace insert', 'wood stove', 'pellet stove', 'coal stove', 'stove'],
        'space heat': ['space heater', 'space heaters', 'wall furnace', 'floor furnace', 
                      'floor model furnace', 'overhead heaters', 'overhead unit heater', 'ductless'],
        'alt heat': ['geo thermal', 'geothermal', 'solar', 'gravity', 'gravity heat system', 
                     'oil', 'propane', 'propane gas', 'coal'],
        'no heat': ['no heat', 'none'],
        'other': ['mixed', 'combination', 'sep. hvac units'],
    }
    
    # Apply the categorization
    df['Heating_new'] = df['Heating']
    df['Heating_new'] = df['Heating_new'].apply(lambda x: check_type(x, heating_categories))
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_heating', value=df.to_json(orient='split'))
    
    return "Heating data cleaned"

def clean_flooring(**kwargs):
    """Clean flooring data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_heating', task_ids='clean_heating')
    df = pd.read_json(df_json, orient='split')
    
    # Define flooring categories
    flooring_categories = {
        'carpet': ['carpet', 'carpet over softwood', 'carpet over hardwood', 'carpeted', 'wall to wall carpet', 
                  'wall-to-wall carpet'],
        'wood': ['bamboo', 'engineered wood', 'engineered hardwood', 'hardwood', 'parquet', 'softwood', 'wood'],
        'tile': ['ceramic', 'ceramic tile', 'ceramic/porcelain', 'porcelain tile', 'non-ceramic tile', 'slate', 
                'stone', 'tile'],
        'vinyl': ['cushion/lino/vinyl', 'vinyl', 'vinyl plank'],
        'laminate': ['laminate', 'laminate flooring'],
        'concrete': ['concrete', 'concrete slab'],
        'other': ['basement slab', 'basement sub-floor', 'granite', 'heavy loading', 'linoleum', 'marble', 'mixed', 
                 'mixed flooring', 'see remarks', 'subfloor', 'other']
    }
    
    # Apply the categorization
    df['Flooring_new'] = df['Flooring']
    df['Flooring_new'] = df['Flooring_new'].apply(lambda x: check_type(x, flooring_categories))
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_flooring', value=df.to_json(orient='split'))
    
    return "Flooring data cleaned"

def clean_roof(**kwargs):
    """Clean roof data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_flooring', task_ids='clean_flooring')
    df = pd.read_json(df_json, orient='split')
    
    # Define roof categories
    roofing_categories = {
        'asphalt': ['asphalt', 'asphalt & gravel', 'asphalt rolled', 'asphalt shingle', 'asphalt shingles', 
                   'asphalt torch on', 'asphalt shingle', 'asphalt/gravel'],
        'cedar shake': ['cedar shake', 'cedar shakes'],
        'clay': ['clay tile'],
        'concrete': ['concrete', 'concrete tiles'],
        'fiberglass': ['fiberglass', 'fiberglass shingles', 'fibreglass shingle'],
        'flat': ['flat', 'flat torch membrane', 'membrane', 'epdm membrane'],
        'metal': ['metal', 'metal shingles', 'steel', 'tin'],
        'pine shake': ['pine shake', 'pine shakes'],
        'rubber': ['rubber'],
        'sbs': ['sbs roofing system'],
        'shake': ['shake'],
        'shingle': ['shingle', 'vinyl shingles'],
        'slate': ['slate'],
        'tar': ['tar & gravel', 'tar & gravel', 'tar &amp; gravel', 'tar/gravel'],
        'tile': ['tile'],
        'wood': ['wood', 'wood shingle', 'wood shingles'],
        'other': ['conventional', 'mixed', 'other'],
    }
    
    # Apply the categorization
    df['Roof_new'] = df['Roof']
    df['Roof_new'] = df['Roof_new'].apply(lambda x: check_type(x, roofing_categories))
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_roof', value=df.to_json(orient='split'))
    
    return "Roof data cleaned"

def clean_waterfront_sewer(**kwargs):
    """Clean waterfront and sewer data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_roof', task_ids='clean_roof')
    df = pd.read_json(df_json, orient='split')
    print(df.columns.tolist())
    # Clean waterfront data
    # df['Waterfront_new'] = df['Waterfront']
    # df.loc[df['Waterfront_new'].isna(), 'Waterfront_new'] = 'No'
    
    # Define sewer categories
    sewage_categories = {
        'municipal': ['municipal/community', 'municipal sewage system', 'sanitary sewer', 'sewer', 'sewer connected', 
                     'sewer to lot', 'sewer available', 'public sewer', 'attached to municipal'],
        'septic': ['septic tank', 'septic system', 'septic system: common', 'septic field', 'septic tank and field', 
                  'septic tank & mound', 'mound septic', 'septic tank & field', 'septic needed', 'engineered septic'],
        'private': ['private sewer', 'private sewer', 'holding tank', 'low pressure sewage sys', 'shared septic'],
        'alternative': ['aerobic treatment system', 'facultative lagoon', 'lagoon', 'outflow tank', 'open discharge', 
                       'liquid surface dis', 'pump', 'tank & straight discharge'],
        'none': ['no sewage system', 'outhouse', 'none'],
    }
    
    # Apply the sewer categorization
    df['Sewer_new'] = df['Sewer']
    df['Sewer_new'] = df['Sewer_new'].apply(lambda x: check_type(x, sewage_categories))
    df['Sewer_new'] = df['Sewer_new'].fillna('none')
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_waterfront_sewer', value=df.to_json(orient='split'))
    
    return "Waterfront and sewer data cleaned"

def add_additional_features(**kwargs):
    """Add additional features like Pool, Garden, etc."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_waterfront_sewer', task_ids='clean_waterfront_sewer')
    df = pd.read_json(df_json, orient='split')
    
    # Combine relevant columns for feature extraction
    mix_features = ['Basement', 'Exterior', 'Features', 'Fireplace', 'Garage', 'Heating', 'Parking']
    df['Combined'] = df[mix_features].astype(str).agg(','.join, axis=1)
    
    # Add Pool feature
    pool_features = ["swimming pool", "public swimming pool"]
    df['Pool'] = df['Combined'].apply(lambda x: check_keywords(x, pool_features))
    
    # Add Garden feature
    garden_features = ["vegetable garden", "garden", "fruit trees/shrubs", "private yard", 
                      "partially landscaped", "landscaped"]
    df['Garden'] = df['Combined'].apply(lambda x: check_keywords(x, garden_features))
    
    # Add View feature
    view_features = ["view downtown", "river view", "view city", "view lake", "lake view", 
                     "ravine view", "river valley view"]
    
    def check_view(features, keywords):
        try:
            cleaned_features = features.replace("[", "").replace("]", "").replace('"', '').replace("'", "").replace(";", ",")
            list_to_check = [f.strip().lower() for f in cleaned_features.split(',')]
            common = set(list_to_check) & set(keywords)
            if common: 
                return list(common)[0]  
        except AttributeError:
            return np.nan
        return np.nan
    
    df['View'] = df['Combined'].apply(lambda x: check_view(x, view_features))
    
    # Standardize view values
    df.loc[df['View'].isin(['view lake', 'lake view']), 'View'] = 'Lake'
    df.loc[df['View'].isin(['view downtown']), 'View'] = 'Downtown'
    df.loc[df['View'].isin(['view city']), 'View'] = 'City'
    df.loc[df['View'].isin(['river view']), 'View'] = 'River'
    df.loc[df['View'].isin(['ravine view', 'river valley view']), 'View'] = 'Valley'
    
    # Add Balcony feature
    balcony_features = ['balcony', 'balcony/deck', 'balcony/patio']
    df['Balcony'] = df['Combined'].apply(lambda x: check_keywords(x, balcony_features))
    
    # Store the updated DataFrame
    kwargs['ti'].xcom_push(key='data_with_additional_features', value=df.to_json(orient='split'))
    
    return "Additional features added"

def finalize_dataset(**kwargs):
    """Finalize the dataset by dropping unnecessary columns and renaming others."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='data_with_additional_features', task_ids='add_additional_features')
    df = pd.read_json(df_json, orient='split')
    
    # # Drop unnecessary columns
    # df = df.drop(columns=['streetAddress', 'postalCode', 'description', 'priceCurrency', 'Air_Conditioning', 
    #                      'Basement', 'Exterior', 'Features', 'Fireplace', 'Garage', 'Heating', 'MLS® #', 'Roof', 
    #                      'Sewer', 'Waterfront', 'Parking', 'Flooring', 'Fireplace_Features', 'Combined', 
    #                      'Subdivision', 'property_sqft', 'Square_Footage', 'Bath', 'Property Tax'])
    # Drop unnecessary columns
    df = df.drop(columns=['streetAddress', 'postalCode', 'description', 'priceCurrency', 
                         'Basement', 'Exterior', 'Features', 'Fireplace', 'Garage', 'Heating', 'Roof', 
                         'Sewer', 'Parking', 'Flooring', 'Fireplace_Features', 'Combined', 
                         'Subdivision', 'property_sqft', 'Square_Footage', 'Bath', 'Property_Tax'])
    
    # # Rename columns
    # df = df.rename(columns={'addressLocality': 'City', 
    #                        'addressRegion': 'Province', 
    #                        'latitude': 'Latitude', 
    #                        'longitude': 'Longitude', 
    #                        'price': 'Price',
    #                        'property_baths': 'Bathrooms', 
    #                        'property-beds': 'Bedrooms', 
    #                        'Square_Footage_new': 'Square_Footage', 
    #                        'Garage_new': 'Garage',    
    #                        'Parking_new': 'Parking',    
    #                        'Basement_new': 'Basement', 
    #                        'Exterior_new': 'Exterior', 
    #                        'Fireplace_new': 'Fireplace', 
    #                        'Heating_new': 'Heating', 
    #                        'Flooring_new': 'Flooring', 
    #                        'Roof_new': 'Roof', 
    #                        'Waterfront_new': 'Waterfront', 
    #                        'Sewer_new': 'Sewer'})
    # Rename columns
    df = df.rename(columns={'addressLocality': 'City', 
                           'addressRegion': 'Province', 
                           'latitude': 'Latitude', 
                           'longitude': 'Longitude', 
                           'price': 'Price',
                           'property_baths': 'Bathrooms', 
                           'property_beds': 'Bedrooms', 
                           'Square_Footage_new': 'Square_Footage', 
                           'Garage_new': 'Garage',    
                           'Parking_new': 'Parking',    
                           'Basement_new': 'Basement', 
                           'Exterior_new': 'Exterior', 
                           'Fireplace_new': 'Fireplace', 
                           'Heating_new': 'Heating', 
                           'Flooring_new': 'Flooring', 
                           'Roof_new': 'Roof', 
                           'Sewer_new': 'Sewer'})
    
    # Fix datatypes
    # Clean Square Footage
    df['Square_Footage'] = df['Square_Footage'].str.replace(',','')
    
    # Convert columns to numeric
    number_columns = ['Latitude', 'Longitude', 'Price', 'Bedrooms', 'Bathrooms', 'Acreage', 
                     'Square_Footage']
    
    for col in number_columns: 
        df[col] = df[col].astype(float)
    
    # Store the finalized DataFrame
    kwargs['ti'].xcom_push(key='finalized_data', value=df.to_json(orient='split'))
    
    return "Dataset finalized"

def remove_inconsistent_data(**kwargs):
    """Remove inconsistent data."""
    # Get the DataFrame from the previous task
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='finalized_data', task_ids='finalize_dataset')
    df = pd.read_json(df_json, orient='split')
    
    # Remove rows with missing essential data
    df = df.dropna(subset=["Bedrooms", "Bathrooms", "Square_Footage"])
    
    # Remove properties with unrealistically small square footage
    df = df[df["Square_Footage"] > 120]
    
    # Remove properties with unrealistically low prices
    df = df[df.Price >= 50_000]
    
    # Drop View column as decided in the original script
    df = df.drop(columns=['View'])
    
    # Store the cleaned DataFrame
    kwargs['ti'].xcom_push(key='cleaned_data', value=df.to_json(orient='split'))
    
    return "Inconsistent data removed"



def save_data(**kwargs):
    """Save the cleaned data to CSV."""
    from airflow.models import Variable
    
    try:
        # Get the DataFrame from the previous task
        ti = kwargs['ti']
        df_json = ti.xcom_pull(key='cleaned_data', task_ids='remove_inconsistent_data')
        df = pd.read_json(df_json, orient='split')
        
        # Get output directory from Airflow Variables
        output_dir = Variable.get("canada_housing_output_dir", 
                                 default_var='/opt/airflow/data/')
        
        # Create directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Define the output path
        output_path = os.path.join(output_dir, f'cleaned_canada.csv')
        
        # Log data quality metrics
        kwargs['ti'].xcom_push(key='final_row_count', value=len(df))
        kwargs['ti'].xcom_push(key='null_counts', value=df.isnull().sum().to_dict())
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        
        return f"Data saved to {output_path}"
    except Exception as e:
        logging.error(f"Error in save_data: {str(e)}")
        raise

# Add a data quality check task
def check_data_quality(**kwargs):
    """Perform data quality checks on the cleaned dataset."""
    try:
        ti = kwargs['ti']
        df_json = ti.xcom_pull(key='cleaned_data', task_ids='remove_inconsistent_data')
        df = pd.read_json(df_json, orient='split')
        
        # Check for remaining null values in critical columns
        critical_cols = ['Price', 'Bedrooms', 'Bathrooms', 'Square_Footage']
        null_counts = {col: df[col].isnull().sum() for col in critical_cols}
        
        # Check for outliers in Price
        q1 = df['Price'].quantile(0.25)
        q3 = df['Price'].quantile(0.75)
        iqr = q3 - q1
        price_outliers = df[(df['Price'] < (q1 - 1.5 * iqr)) | (df['Price'] > (q3 + 1.5 * iqr))].shape[0]
        
        # Log results
        kwargs['ti'].xcom_push(key='data_quality_null_counts', value=null_counts)
        kwargs['ti'].xcom_push(key='data_quality_price_outliers', value=price_outliers)
        
        if any(count > 0 for count in null_counts.values()):
            logging.warning(f"Found null values in critical columns: {null_counts}")
        
        if price_outliers > 0:
            logging.warning(f"Found {price_outliers} price outliers")
        
        return "Data quality checks completed"
    except Exception as e:
        logging.error(f"Error in check_data_quality: {str(e)}")
        raise

test_connection_task = PythonOperator(
    task_id='test_db_connection',
    python_callable=test_db_connection,
    provide_context=True,
    dag=dag,
)

task_load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

task_clean_square_footage = PythonOperator(
    task_id='clean_square_footage',
    python_callable=clean_square_footage,
    provide_context=True,
    dag=dag,
)

task_clean_acreage = PythonOperator(
    task_id='clean_acreage',
    python_callable=clean_acreage,
    provide_context=True,
    dag=dag,
)

task_clean_bathrooms = PythonOperator(
    task_id='clean_bathrooms',
    python_callable=clean_bathrooms,
    provide_context=True,
    dag=dag,
)

task_clean_garage_parking = PythonOperator(
    task_id='clean_garage_parking',
    python_callable=clean_garage_parking,
    provide_context=True,
    dag=dag,
)

task_clean_basement = PythonOperator(
    task_id='clean_basement',
    python_callable=clean_basement,
    provide_context=True,
    dag=dag,
)

task_clean_exterior = PythonOperator(
    task_id='clean_exterior',
    python_callable=clean_exterior,
    provide_context=True,
    dag=dag,
)

task_clean_fireplace = PythonOperator(
    task_id='clean_fireplace',
    python_callable=clean_fireplace,
    provide_context=True,
    dag=dag,
)

task_clean_heating = PythonOperator(
    task_id='clean_heating',
    python_callable=clean_heating,
    provide_context=True,
    dag=dag,
)

task_clean_flooring = PythonOperator(
    task_id='clean_flooring',
    python_callable=clean_flooring,
    provide_context=True,
    dag=dag,
)

task_clean_roof = PythonOperator(
    task_id='clean_roof',
    python_callable=clean_roof,
    provide_context=True,
    dag=dag,
)

task_clean_waterfront_sewer = PythonOperator(
    task_id='clean_waterfront_sewer',
    python_callable=clean_waterfront_sewer,
    provide_context=True,
    dag=dag,
)

task_add_additional_features = PythonOperator(
    task_id='add_additional_features',
    python_callable=add_additional_features,
    provide_context=True,
    dag=dag,
)

task_finalize_dataset = PythonOperator(
    task_id='finalize_dataset',
    python_callable=finalize_dataset,
    provide_context=True,
    dag=dag,
)

task_remove_inconsistent_data = PythonOperator(
    task_id='remove_inconsistent_data',
    python_callable=remove_inconsistent_data,
    provide_context=True,
    dag=dag,
)

# Add the new task
task_check_data_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag,
)

task_save_data = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    provide_context=True,
    dag=dag,
)

run_import_script = BashOperator(
    task_id='add_to_postgres',  # Đặt tên task là run_python_script
    bash_command='python3 /opt/airflow/code/import_to_postgres.py',  # Chạy file python import dữ liệu vào PostgreSQL
    dag=dag
)


test_connection_task >> task_load_data
# Set up task dependencies
task_load_data >> task_clean_square_footage >> task_clean_acreage >> task_clean_bathrooms
task_clean_bathrooms >> task_clean_garage_parking >> task_clean_basement >> task_clean_exterior
task_clean_exterior >> task_clean_fireplace >> task_clean_heating >> task_clean_flooring
task_clean_flooring >> task_clean_roof >> task_clean_waterfront_sewer >> task_add_additional_features
task_add_additional_features >> task_finalize_dataset >> task_remove_inconsistent_data >> task_check_data_quality >> task_save_data >> run_import_script
