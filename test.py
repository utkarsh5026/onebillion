import json
import pandas as pd
from datetime import datetime
from dateutil import parser
from enum import Enum
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# =====================================================================
# SNOWFLAKE DATA TYPE ENUM
# =====================================================================

class SnowflakeType(Enum):
    """Enum representing Snowflake data types with their default values"""
    STRING = ""
    NUMBER = 0
    TIMESTAMP = None
    BOOLEAN = False
    OBJECT = {}
    ARRAY = []

    def get_default(self):
        """Get the default value for this type"""
        if self == SnowflakeType.OBJECT:
            return {}
        elif self == SnowflakeType.ARRAY:
            return []
        else:
            return self.value

# =====================================================================
# UTILITY FUNCTIONS
# =====================================================================

def safe_get(data, *keys, data_type=None, default=None):
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
            if current is None:
                break
        else:
            current = None
            break

    if default is not None:
        fallback = default
    elif data_type is not None:
        fallback = data_type.get_default()
    else:
        fallback = ""

    if current is None:
        return fallback

    if data_type is None:
        return current

    try:
        if data_type == SnowflakeType.STRING:
            return str(current) if current is not None else fallback

        elif data_type == SnowflakeType.NUMBER:
            if isinstance(current, (int, float)):
                return current
            if isinstance(current, str):
                try:
                    return int(current)
                except ValueError:
                    return float(current)
            return fallback

        elif data_type == SnowflakeType.TIMESTAMP:
            if isinstance(current, datetime):
                return current
            if isinstance(current, str):
                # Try parsing if it's a string
                from dateutil import parser
                return parser.parse(current)
            return fallback

        elif data_type == SnowflakeType.BOOLEAN:
            # Convert to boolean
            if isinstance(current, bool):
                return current
            if isinstance(current, str):
                return current.lower() in ('true', '1', 'yes', 'y')
            return bool(current)

        elif data_type == SnowflakeType.OBJECT:
            # Ensure it's a dictionary
            if isinstance(current, dict):
                return current
            return fallback

        elif data_type == SnowflakeType.ARRAY:
            # Ensure it's a list
            if isinstance(current, list):
                return current
            return fallback

        else:
            # Unknown type, return current value
            return current

    except (ValueError, TypeError, Exception):
        # If type conversion fails, return the fallback
        return fallback

def parse_date(date_str, dayfirst=True):
    """Parse date string with multiple format support"""
    if not date_str or date_str == "":
        return None
    try:
        return parser.parse(str(date_str), dayfirst=dayfirst)
    except:
        return None

def extract_numeric(value):
    """Extract numeric value from string"""
    if pd.isna(value) or value == "":
        return None
    try:
        import re
        match = re.search(r"(-?\\d+\\.?\\d*)", str(value))
        return float(match.group(1)) if match else None
    except:
        return None

def flatten_dict(d, parent_key="", sep="_"):
    """Flatten nested dictionary"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key.upper(), str(v)))
    return dict(items)

# =====================================================================
# TABLE CREATION FUNCTIONS (MINIMAL SCHEMAS)
# =====================================================================

def create_master_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_MASTER_ID NUMBER,
            CLAIM_NUMBER STRING,
            FILENAME STRING,
            PROCESSING_TIMESTAMP TIMESTAMP
        )
    """).collect()

def create_hospital_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_HOSPITAL_ID NUMBER,
            DS_MASTER_ID NUMBER
        )
    """).collect()

def create_patient_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_PATIENT_ID NUMBER,
            DS_MASTER_ID NUMBER
        )
    """).collect()

def create_doctor_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_DOCTOR_ID NUMBER,
            DS_MASTER_ID NUMBER,
            DOCTOR_TYPE STRING
        )
    """).collect()

def create_diagnosis_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_DIAGNOSIS_ID NUMBER,
            DS_MASTER_ID NUMBER,
            DIAGNOSIS_TYPE STRING
        )
    """).collect()

def create_procedures_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_PROCEDURE_ID NUMBER,
            DS_MASTER_ID NUMBER
        )
    """).collect()

def create_medications_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_MEDICATION_ID NUMBER,
            DS_MASTER_ID NUMBER,
            MEDICATION_TYPE STRING
        )
    """).collect()

def create_investigations_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_INVESTIGATION_ID NUMBER,
            DS_MASTER_ID NUMBER,
            INVESTIGATION_TYPE STRING
        )
    """).collect()

def create_vitals_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_VITAL_ID NUMBER,
            DS_MASTER_ID NUMBER
        )
    """).collect()

def create_symptoms_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_SYMPTOM_ID NUMBER,
            DS_MASTER_ID NUMBER,
            SYMPTOM_TYPE STRING
        )
    """).collect()

def create_medical_codes_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_CODE_ID NUMBER,
            DS_MASTER_ID NUMBER,
            CODE_TYPE STRING
        )
    """).collect()

def create_congenital_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_CONGENITAL_ID NUMBER,
            DS_MASTER_ID NUMBER
        )
    """).collect()

def create_billing_table(session, table_name):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DS_BILLING_ID NUMBER,
            DS_MASTER_ID NUMBER
        )
    """).collect()

# =====================================================================
# CORRECTED SAVE FUNCTION - Matches Final Bill Procedure
# =====================================================================

def save_to_table(session, records, table_name):
    """
    Save records to table with proper column synchronization.
    Uses the same approach as the final bill procedure.
    """
    if not records:
        return 0
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    if df.empty:
        return 0
    
    # Get existing table columns
    try:
        existing_columns = [col.name.upper() for col in session.table(table_name).schema.fields]
    except Exception as e:
        print(f"Warning reading table schema: {e}")
        existing_columns = []
    
    # Get DataFrame columns
    new_columns = [col.upper() for col in df.columns]
    
    # Step 1: Add new columns to table if they do not exist
    for col_name in new_columns:
        if col_name not in existing_columns:
            alter_stmt = f\'\'ALTER TABLE {table_name} ADD COLUMN "{col_name}" STRING\'\'
            try:
                session.sql(alter_stmt).collect()
                existing_columns.append(col_name)
            except Exception as e:
                print(f"Warning adding column {col_name}: {e}")
    
    # Step 2: CRITICAL - Add missing columns to DataFrame
    # (columns that exist in table but not in DataFrame)
    for col_name in existing_columns:
        if col_name not in df.columns:
            df[col_name] = ""
    
    # Step 3: CRITICAL - Reorder DataFrame to match table column order
    df = df[existing_columns]
    
    # Step 4: Insert data
    session.create_dataframe(df).write.mode("append").save_as_table(table_name)
    
    return len(records)

# =====================================================================
# DATA EXTRACTION FUNCTIONS
# =====================================================================

def extract_master_data(claim_number, ds_data, metadata, master_id, filename, processing_ts):
    """Extract master record data"""
    doc_meta = safe_get(ds_data, "document_metadata", default={})
    cong_summary = safe_get(ds_data, "congenital_conditions_summary", default={})
    
    return {
        "DS_MASTER_ID": master_id,
        "CLAIM_NUMBER": str(claim_number),
        "FILENAME": filename,
        "PROCESSING_TIMESTAMP": processing_ts,
        "PROCESSING_STATUS": safe_get(metadata, "processing_method"),
        "PROCESSING_METHOD": safe_get(doc_meta, "extraction_timestamp"),
        "TOTAL_PAGES": safe_get(doc_meta, "total_pages", default=0),
        "DOCUMENT_QUALITY": safe_get(doc_meta, "quality_assessment"),
        "HAS_CONGENITAL": safe_get(cong_summary, "has_congenital_conditions", default=False),
        "TOTAL_CONGENITAL_CONDITIONS": safe_get(cong_summary, "total_congenital_conditions", default=0),
        "INTERNAL_CONGENITAL_COUNT": safe_get(cong_summary, "internal_congenital_count", default=0),
        "EXTERNAL_CONGENITAL_COUNT": safe_get(cong_summary, "external_congenital_count", default=0)
    }

def extract_hospital_data(ds_data, master_id, hospital_id):
    """Extract hospital details"""
    hosp = safe_get(ds_data, "hospital_details", default={})
    
    # Flatten all hospital fields
    flat_hosp = flatten_dict(hosp) if hosp else {}
    
    # Add system fields
    flat_hosp["DS_HOSPITAL_ID"] = hospital_id
    flat_hosp["DS_MASTER_ID"] = master_id
    
    return flat_hosp

def extract_patient_data(ds_data, master_id, patient_id):
    """Extract patient information"""
    patient = safe_get(ds_data, "patient_information", default={})
    
    # Flatten all patient fields
    flat_patient = flatten_dict(patient) if patient else {}
    
    # Add system fields
    flat_patient["DS_PATIENT_ID"] = patient_id
    flat_patient["DS_MASTER_ID"] = master_id
    
    return flat_patient

def extract_doctor_data(ds_data, master_id, session):
    """Extract doctor/consultant information"""
    doctors_list = []
    doc_details = safe_get(ds_data, "doctor_details", default={})
    
    # Consultant
    consultant = safe_get(doc_details, "consultant", default={})
    if consultant and safe_get(consultant, "name"):
        doctor_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
        flat_doc = flatten_dict(consultant)
        flat_doc["DS_DOCTOR_ID"] = doctor_id
        flat_doc["DS_MASTER_ID"] = master_id
        flat_doc["DOCTOR_TYPE"] = "CONSULTANT"
        doctors_list.append(flat_doc)
    
    # Discharge summary signing doctor
    signatures = safe_get(ds_data, "document_signatures", default={})
    signing_doc = safe_get(signatures, "discharge_summary_signed_by", default={})
    if signing_doc and safe_get(signing_doc, "doctor_name"):
        doctor_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
        flat_doc = flatten_dict(signing_doc)
        flat_doc["DS_DOCTOR_ID"] = doctor_id
        flat_doc["DS_MASTER_ID"] = master_id
        flat_doc["DOCTOR_TYPE"] = "SIGNING_DOCTOR"
        doctors_list.append(flat_doc)
    
    return doctors_list

def extract_diagnosis_data(ds_data, master_id, session):
    """Extract diagnosis information"""
    diagnosis_list = []
    diagnosis_section = safe_get(ds_data, "diagnosis", default={})
    
    diagnosis_types = ["primary_diagnosis", "secondary_diagnosis", "provisional_diagnosis", 
                       "final_diagnosis", "differential_diagnosis"]
    
    for diag_type in diagnosis_types:
        diagnoses = safe_get(diagnosis_section, diag_type, default=[])
        if not isinstance(diagnoses, list):
            diagnoses = [diagnoses] if diagnoses else []
        
        for diag in diagnoses:
            if not diag:
                continue
            diag_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
            
            # Flatten diagnosis
            flat_diag = flatten_dict(diag)
            flat_diag["DS_DIAGNOSIS_ID"] = diag_id
            flat_diag["DS_MASTER_ID"] = master_id
            flat_diag["DIAGNOSIS_TYPE"] = diag_type.replace("_", " ").upper()
            
            diagnosis_list.append(flat_diag)
    
    return diagnosis_list

def extract_procedures_data(ds_data, master_id, session):
    """Extract procedures/surgeries information"""
    procedures_list = []
    treatment = safe_get(ds_data, "treatment_and_procedures", default={})
    
    procedures = safe_get(treatment, "procedures_performed", default=[])
    if not isinstance(procedures, list):
        procedures = [procedures] if procedures else []
    
    for proc in procedures:
        if not proc:
            continue
        proc_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
        
        flat_proc = flatten_dict(proc)
        flat_proc["DS_PROCEDURE_ID"] = proc_id
        flat_proc["DS_MASTER_ID"] = master_id
        
        procedures_list.append(flat_proc)
    
    return procedures_list

def extract_medications_data(ds_data, master_id, session):
    """Extract medications information"""
    medications_list = []
    meds_section = safe_get(ds_data, "medications_prescribed", default={})
    
    med_types = ["discharge_medications", "inpatient_medications"]
    
    for med_type in med_types:
        medications = safe_get(meds_section, med_type, default=[])
        if not isinstance(medications, list):
            medications = [medications] if medications else []
        
        for med in medications:
            if not med:
                continue
            med_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
            
            flat_med = flatten_dict(med)
            flat_med["DS_MEDICATION_ID"] = med_id
            flat_med["DS_MASTER_ID"] = master_id
            flat_med["MEDICATION_TYPE"] = med_type.replace("_", " ").upper()
            
            medications_list.append(flat_med)
    
    return medications_list

def extract_investigations_data(ds_data, master_id, session):
    """Extract investigations/tests information"""
    investigations_list = []
    inv_section = safe_get(ds_data, "investigations", default={})
    
    # Laboratory tests
    lab_tests = safe_get(inv_section, "laboratory_tests", default=[])
    if not isinstance(lab_tests, list):
        lab_tests = [lab_tests] if lab_tests else []
    
    for test in lab_tests:
        if not test:
            continue
        inv_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
        
        flat_test = flatten_dict(test)
        flat_test["DS_INVESTIGATION_ID"] = inv_id
        flat_test["DS_MASTER_ID"] = master_id
        flat_test["INVESTIGATION_TYPE"] = "LABORATORY"
        
        investigations_list.append(flat_test)
    
    # Imaging studies
    imaging = safe_get(inv_section, "imaging_studies", default=[])
    if not isinstance(imaging, list):
        imaging = [imaging] if imaging else []
    
    for img in imaging:
        if not img:
            continue
        inv_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
        
        flat_img = flatten_dict(img)
        flat_img["DS_INVESTIGATION_ID"] = inv_id
        flat_img["DS_MASTER_ID"] = master_id
        flat_img["INVESTIGATION_TYPE"] = "IMAGING"
        
        investigations_list.append(flat_img)
    
    return investigations_list

def extract_vitals_data(ds_data, master_id, session):
    """Extract vital signs information"""
    vitals_list = []
    initial_assess = safe_get(ds_data, "initial_assessment", default={})
    vital_signs = safe_get(initial_assess, "initial_vital_signs", default={})
    
    vital_types = ["temperature", "pulse", "bp_systolic", "bp_diastolic", 
                   "respiratory_rate", "spo2", "weight", "height"]
    
    for vital_key in vital_types:
        vital_data = safe_get(vital_signs, vital_key, default={})
        if not vital_data:
            continue
        
        vital_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
        
        flat_vital = flatten_dict(vital_data)
        flat_vital["DS_VITAL_ID"] = vital_id
        flat_vital["DS_MASTER_ID"] = master_id
        flat_vital["VITAL_TYPE"] = vital_key.upper()
        
        vitals_list.append(flat_vital)
    
    return vitals_list

def extract_symptoms_data(ds_data, master_id, session):
    """Extract symptoms information"""
    symptoms_list = []
    symptoms_section = safe_get(ds_data, "symptoms", default={})
    
    symptom_types = ["primary_symptoms", "secondary_symptoms"]
    
    for symptom_type in symptom_types:
        symptoms = safe_get(symptoms_section, symptom_type, default=[])
        if not isinstance(symptoms, list):
            symptoms = [symptoms] if symptoms else []
        
        for symptom in symptoms:
            if not symptom:
                continue
            symptom_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
            
            flat_symptom = flatten_dict(symptom)
            flat_symptom["DS_SYMPTOM_ID"] = symptom_id
            flat_symptom["DS_MASTER_ID"] = master_id
            flat_symptom["SYMPTOM_TYPE"] = symptom_type.replace("_", " ").upper()
            
            symptoms_list.append(flat_symptom)
    
    return symptoms_list

def extract_medical_codes_data(ds_data, master_id, session):
    """Extract all medical codes (ICD-10, SNOMED, LOINC, CPT, PCS)"""
    codes_list = []
    codes_section = safe_get(ds_data, "medical_codes", default={})
    
    code_types = {
        "icd_10_codes": "ICD-10",
        "snomed_ct_codes": "SNOMED-CT",
        "loinc_codes": "LOINC",
        "cpt_codes": "CPT",
        "pcs_codes": "PCS"
    }
    
    for code_key, code_type in code_types.items():
        codes = safe_get(codes_section, code_key, default=[])
        if not isinstance(codes, list):
            codes = [codes] if codes else []
        
        for code in codes:
            if not code:
                continue
            code_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
            
            flat_code = flatten_dict(code)
            flat_code["DS_CODE_ID"] = code_id
            flat_code["DS_MASTER_ID"] = master_id
            flat_code["CODE_TYPE"] = code_type
            
            codes_list.append(flat_code)
    
    return codes_list

def extract_congenital_data(ds_data, master_id, session):
    """Extract congenital conditions information"""
    congenital_list = []
    cong_summary = safe_get(ds_data, "congenital_conditions_summary", default={})
    
    conditions = safe_get(cong_summary, "congenital_conditions_list", default=[])
    if not isinstance(conditions, list):
        conditions = [conditions] if conditions else []
    
    for condition in conditions:
        if not condition:
            continue
        cong_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
        
        flat_cond = flatten_dict(condition)
        flat_cond["DS_CONGENITAL_ID"] = cong_id
        flat_cond["DS_MASTER_ID"] = master_id
        
        congenital_list.append(flat_cond)
    
    return congenital_list

def extract_billing_data(ds_data, master_id, session):
    """Extract billing information"""
    billing_list = []
    billing_section = safe_get(ds_data, "billing_summary", default={})
    
    if not safe_get(billing_section, "bill_present", default=False):
        return billing_list
    
    line_items = safe_get(billing_section, "line_items", default=[])
    if not isinstance(line_items, list):
        line_items = [line_items] if line_items else []
    
    for item in line_items:
        if not item:
            continue
        billing_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
        
        flat_bill = flatten_dict(item)
        flat_bill["DS_BILLING_ID"] = billing_id
        flat_bill["DS_MASTER_ID"] = master_id
        
        # Add bill header info
        flat_bill["BILL_NUMBER"] = safe_get(billing_section, "bill_number")
        flat_bill["BILL_DATE"] = safe_get(billing_section, "bill_date")
        flat_bill["CURRENCY"] = safe_get(billing_section, "currency")
        
        billing_list.append(flat_bill)
    
    return billing_list

# =====================================================================
# MAIN PROCESSING FUNCTION
# =====================================================================

def main(session: Session, filename: str):
    """Main procedure to process discharge summaries"""
    
    schema = "BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC"
    tables = {
        "master": f"{schema}.DS_MASTER",
        "hospital": f"{schema}.DS_HOSPITAL",
        "patient": f"{schema}.DS_PATIENT",
        "doctor": f"{schema}.DS_DOCTOR",
        "diagnosis": f"{schema}.DS_DIAGNOSIS",
        "procedures": f"{schema}.DS_PROCEDURES",
        "medications": f"{schema}.DS_MEDICATIONS",
        "investigations": f"{schema}.DS_INVESTIGATIONS",
        "vitals": f"{schema}.DS_VITALS",
        "symptoms": f"{schema}.DS_SYMPTOMS",
        "medical_codes": f"{schema}.DS_MEDICAL_CODES",
        "congenital": f"{schema}.DS_CONGENITAL",
        "billing": f"{schema}.DS_BILLING"
    }
    
    # Create all tables
    create_master_table(session, tables["master"])
    create_hospital_table(session, tables["hospital"])
    create_patient_table(session, tables["patient"])
    create_doctor_table(session, tables["doctor"])
    create_diagnosis_table(session, tables["diagnosis"])
    create_procedures_table(session, tables["procedures"])
    create_medications_table(session, tables["medications"])
    create_investigations_table(session, tables["investigations"])
    create_vitals_table(session, tables["vitals"])
    create_symptoms_table(session, tables["symptoms"])
    create_medical_codes_table(session, tables["medical_codes"])
    create_congenital_table(session, tables["congenital"])
    create_billing_table(session, tables["billing"])
    
    # Fetch data from source table
    df_input = session.table("BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.DSICHARGE_SUMMARY_2") \\
        .select(col("VARIANT_COL")).to_pandas()
    
    if df_input.empty:
        return f"No data found in DSICHARGE_SUMMARY_2 table"
    
    processing_ts = datetime.now()
    
    # Counters
    total_records = 0
    master_records = []
    hospital_records = []
    patient_records = []
    doctor_records = []
    diagnosis_records = []
    procedures_records = []
    medications_records = []
    investigations_records = []
    vitals_records = []
    symptoms_records = []
    medical_codes_records = []
    congenital_records = []
    billing_records = []
    
    # Process each JSON record
    for idx, row in df_input.iterrows():
        try:
            json_data = row["VARIANT_COL"]
            if isinstance(json_data, str):
                data = json.loads(json_data)
            else:
                data = json_data
            
            # Process each claim in the JSON
            for claim_number, claim_data in data.items():
                results = safe_get(claim_data, "results", default={})
                metadata = safe_get(claim_data, "metadata", default={})
                
                # Check if discharge_summary exists
                ds_data = safe_get(results, "discharge_summary")
                if not ds_data:
                    continue
                
                # Generate unique master ID
                master_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
                
                # Extract and store master data
                master_data = extract_master_data(claim_number, ds_data, metadata, master_id, filename, processing_ts)
                master_records.append(master_data)
                
                # Extract hospital data
                hospital_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
                hospital_data = extract_hospital_data(ds_data, master_id, hospital_id)
                hospital_records.append(hospital_data)
                
                # Extract patient data
                patient_id = session.sql("SELECT BAGIC_EDL_DB.BAGIC_EDL_NIRIKHSA_NONPROD_SC.UNIQUE_ID_SEQ.nextval").collect()[0][0]
                patient_data = extract_patient_data(ds_data, master_id, patient_id)
                patient_records.append(patient_data)
                
                # Extract all other entities
                doctor_records.extend(extract_doctor_data(ds_data, master_id, session))
                diagnosis_records.extend(extract_diagnosis_data(ds_data, master_id, session))
                procedures_records.extend(extract_procedures_data(ds_data, master_id, session))
                medications_records.extend(extract_medications_data(ds_data, master_id, session))
                investigations_records.extend(extract_investigations_data(ds_data, master_id, session))
                vitals_records.extend(extract_vitals_data(ds_data, master_id, session))
                symptoms_records.extend(extract_symptoms_data(ds_data, master_id, session))
                medical_codes_records.extend(extract_medical_codes_data(ds_data, master_id, session))
                congenital_records.extend(extract_congenital_data(ds_data, master_id, session))
                billing_records.extend(extract_billing_data(ds_data, master_id, session))
                
                total_records += 1
        
        except Exception as e:
            print(f"Error processing record {idx}: {str(e)}")
            continue
    
    # Save all records to tables using the corrected save function
    counts = {
        "master": save_to_table(session, master_records, tables["master"]),
        "hospital": save_to_table(session, hospital_records, tables["hospital"]),
        "patient": save_to_table(session, patient_records, tables["patient"]),
        "doctor": save_to_table(session, doctor_records, tables["doctor"]),
        "diagnosis": save_to_table(session, diagnosis_records, tables["diagnosis"]),
        "procedures": save_to_table(session, procedures_records, tables["procedures"]),
        "medications": save_to_table(session, medications_records, tables["medications"]),
        "investigations": save_to_table(session, investigations_records, tables["investigations"]),
        "vitals": save_to_table(session, vitals_records, tables["vitals"]),
        "symptoms": save_to_table(session, symptoms_records, tables["symptoms"]),
        "medical_codes": save_to_table(session, medical_codes_records, tables["medical_codes"]),
        "congenital": save_to_table(session, congenital_records, tables["congenital"]),
        "billing": save_to_table(session, billing_records, tables["billing"])
    }
    
    return f"""✅ Discharge summary data successfully processed!
    