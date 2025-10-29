import pandas as pd
import numpy as np
from datetime import datetime

def validate_datasets():
    """
    Comprehensive validation script for Phase 2 datasets
    Checks data quality, consistency, and business logic
    """
    
    print("=" * 70)
    print("🔍 PROJECT PERFORMANCE DATA VALIDATION REPORT")
    print("=" * 70)
    print(f"Validation Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Load datasets
    try:
        marketing = pd.read_csv('marketing_projects.csv')
        operations = pd.read_csv('operations_projects.csv')
        it = pd.read_csv('it_projects.csv')
        print("✅ All CSV files loaded successfully\n")
    except FileNotFoundError as e:
        print(f"❌ ERROR: Could not find CSV files. Please run generate_project_data.py first.")
        print(f"   Missing file: {e.filename}\n")
        return False
    
    all_data = pd.concat([marketing, operations, it], ignore_index=True)
    
    # Store validation results
    validation_passed = True
    issues = []
    
    print("-" * 70)
    print("1️⃣  SCHEMA VALIDATION")
    print("-" * 70)
    
    expected_columns = [
        'Project_ID', 'Department', 'Project_Name', 'Manager', 
        'Start_Date', 'End_Date', 'Planned_Cost', 'Actual_Cost',
        'Planned_Completion', 'Actual_Completion', 'Status',
        'Tasks_Total', 'Tasks_Completed', 'Planned_Hours', 'Actual_Hours'
    ]
    
    for name, df in [('Marketing', marketing), ('Operations', operations), ('IT', it)]:
        missing_cols = set(expected_columns) - set(df.columns)
        extra_cols = set(df.columns) - set(expected_columns)
        
        if missing_cols or extra_cols:
            print(f"❌ {name}: Schema mismatch")
            if missing_cols:
                print(f"   Missing columns: {missing_cols}")
                issues.append(f"{name} missing columns: {missing_cols}")
            if extra_cols:
                print(f"   Extra columns: {extra_cols}")
                issues.append(f"{name} has extra columns: {extra_cols}")
            validation_passed = False
        else:
            print(f"✅ {name}: All {len(expected_columns)} columns present")
    
    print(f"\nTotal records: {len(all_data)} projects across 3 departments")
    
    print("\n" + "-" * 70)
    print("2️⃣  DATA INTEGRITY CHECKS")
    print("-" * 70)
    
    # Check 2.1: Unique Project IDs
    print("\n🔑 Project ID Uniqueness:")
    all_ids = all_data['Project_ID'].tolist()
    duplicate_ids = all_data[all_data.duplicated(subset=['Project_ID'], keep=False)]['Project_ID'].tolist()
    
    if duplicate_ids:
        print(f"❌ Found {len(duplicate_ids)} duplicate Project IDs: {set(duplicate_ids)}")
        issues.append(f"Duplicate Project IDs found: {set(duplicate_ids)}")
        validation_passed = False
    else:
        print(f"✅ All {len(all_ids)} Project IDs are unique")
    
    # Check 2.2: ID Ranges
    print("\n🔢 Project ID Ranges:")
    marketing_ids = marketing['Project_ID']
    operations_ids = operations['Project_ID']
    it_ids = it['Project_ID']
    
    print(f"   Marketing: {marketing_ids.min()} - {marketing_ids.max()} (Expected: 1001-1020)")
    print(f"   Operations: {operations_ids.min()} - {operations_ids.max()} (Expected: 2001-2020)")
    print(f"   IT: {it_ids.min()} - {it_ids.max()} (Expected: 3001-3020)")
    
    # Check 2.3: Missing Values
    print("\n❓ Missing Values Check:")
    missing_summary = all_data.isnull().sum()
    total_missing = missing_summary.sum()
    
    if total_missing > 0:
        print(f"❌ Found {total_missing} missing values:")
        for col, count in missing_summary[missing_summary > 0].items():
            print(f"   {col}: {count} missing")
            issues.append(f"{col} has {count} missing values")
        validation_passed = False
    else:
        print(f"✅ No missing values detected")
    
    print("\n" + "-" * 70)
    print("3️⃣  DATE VALIDATION")
    print("-" * 70)
    
    # Convert dates
    all_data['Start_Date'] = pd.to_datetime(all_data['Start_Date'], errors='coerce')
    all_data['End_Date'] = pd.to_datetime(all_data['End_Date'], errors='coerce')
    
    # Check 3.1: Date Format
    invalid_start = all_data['Start_Date'].isnull().sum()
    invalid_end = all_data['End_Date'].isnull().sum()
    
    if invalid_start > 0 or invalid_end > 0:
        print(f"❌ Invalid date formats found:")
        print(f"   Start_Date: {invalid_start} invalid")
        print(f"   End_Date: {invalid_end} invalid")
        issues.append(f"Invalid date formats: Start({invalid_start}), End({invalid_end})")
        validation_passed = False
    else:
        print(f"✅ All dates in valid format")
    
    # Check 3.2: Date Logic (End > Start)
    all_data['Duration_Days'] = (all_data['End_Date'] - all_data['Start_Date']).dt.days
    invalid_duration = all_data[all_data['Duration_Days'] <= 0]
    
    if len(invalid_duration) > 0:
        print(f"❌ Found {len(invalid_duration)} projects with End_Date <= Start_Date:")
        print(invalid_duration[['Project_ID', 'Project_Name', 'Start_Date', 'End_Date']])
        issues.append(f"{len(invalid_duration)} projects have invalid date ranges")
        validation_passed = False
    else:
        print(f"✅ All projects have End_Date > Start_Date")
    
    # Check 3.3: Reasonable Duration
    print(f"\n📅 Project Duration Statistics:")
    print(f"   Average: {all_data['Duration_Days'].mean():.1f} days")
    print(f"   Min: {all_data['Duration_Days'].min()} days")
    print(f"   Max: {all_data['Duration_Days'].max()} days")
    
    print("\n" + "-" * 70)
    print("4️⃣  BUSINESS LOGIC VALIDATION")
    print("-" * 70)
    
    # Check 4.1: Cost Validation
    print("\n💰 Cost Validation:")
    negative_planned = all_data[all_data['Planned_Cost'] <= 0]
    negative_actual = all_data[all_data['Actual_Cost'] <= 0]
    
    if len(negative_planned) > 0 or len(negative_actual) > 0:
        print(f"❌ Found negative or zero costs:")
        print(f"   Planned_Cost: {len(negative_planned)} invalid")
        print(f"   Actual_Cost: {len(negative_actual)} invalid")
        issues.append("Negative or zero costs detected")
        validation_passed = False
    else:
        print(f"✅ All costs are positive")
    
    # Cost Variance Analysis
    all_data['Cost_Variance_Pct'] = ((all_data['Actual_Cost'] - all_data['Planned_Cost']) / all_data['Planned_Cost'] * 100)
    extreme_variance = all_data[abs(all_data['Cost_Variance_Pct']) > 50]
    
    print(f"\n📊 Cost Variance Statistics:")
    print(f"   Average Variance: {all_data['Cost_Variance_Pct'].mean():.2f}%")
    print(f"   Min Variance: {all_data['Cost_Variance_Pct'].min():.2f}%")
    print(f"   Max Variance: {all_data['Cost_Variance_Pct'].max():.2f}%")
    
    if len(extreme_variance) > 0:
        print(f"⚠️  Warning: {len(extreme_variance)} projects have >50% cost variance")
    
    # Check 4.2: Completion Validation
    print("\n🎯 Completion Validation:")
    invalid_planned = all_data[(all_data['Planned_Completion'] < 0) | (all_data['Planned_Completion'] > 100)]
    invalid_actual = all_data[(all_data['Actual_Completion'] < 0) | (all_data['Actual_Completion'] > 100)]
    
    if len(invalid_planned) > 0 or len(invalid_actual) > 0:
        print(f"❌ Invalid completion percentages:")
        print(f"   Planned_Completion: {len(invalid_planned)} out of range")
        print(f"   Actual_Completion: {len(invalid_actual)} out of range")
        issues.append("Completion percentages out of 0-100 range")
        validation_passed = False
    else:
        print(f"✅ All completion percentages within 0-100%")
    
    # Check 4.3: Task Validation
    print("\n✔️  Task Validation:")
    invalid_tasks = all_data[all_data['Tasks_Completed'] > all_data['Tasks_Total']]
    
    if len(invalid_tasks) > 0:
        print(f"❌ {len(invalid_tasks)} projects have more completed tasks than total:")
        print(invalid_tasks[['Project_ID', 'Project_Name', 'Tasks_Total', 'Tasks_Completed']])
        issues.append(f"{len(invalid_tasks)} projects have invalid task counts")
        validation_passed = False
    else:
        print(f"✅ All task counts are logical (Completed ≤ Total)")
    
    # Check 4.4: Status Validation
    print("\n🚦 Status Validation:")
    valid_statuses = ['Not Started', 'In Progress', 'Completed', 'Delayed', 'On Hold']
    invalid_status = all_data[~all_data['Status'].isin(valid_statuses)]
    
    if len(invalid_status) > 0:
        print(f"❌ Found {len(invalid_status)} invalid status values:")
        print(f"   Invalid statuses: {invalid_status['Status'].unique()}")
        issues.append("Invalid status values found")
        validation_passed = False
    else:
        print(f"✅ All statuses are valid")
    
    status_dist = all_data['Status'].value_counts()
    print(f"\n📈 Status Distribution:")
    for status, count in status_dist.items():
        percentage = (count / len(all_data)) * 100
        print(f"   {status}: {count} ({percentage:.1f}%)")
    
    # Check 4.5: Hours Validation
    print("\n⏱️  Hours Validation:")
    negative_hours = all_data[(all_data['Planned_Hours'] <= 0) | (all_data['Actual_Hours'] < 0)]
    
    if len(negative_hours) > 0:
        print(f"❌ Found {len(negative_hours)} projects with invalid hours")
        issues.append("Negative or zero hours detected")
        validation_passed = False
    else:
        print(f"✅ All hour values are valid")
    
    print("\n" + "-" * 70)
    print("5️⃣  DEPARTMENT-SPECIFIC VALIDATION")
    print("-" * 70)
    
    for dept, df, expected_range in [
        ('Marketing', marketing, (10000, 50000)),
        ('Operations', operations, (15000, 75000)),
        ('IT', it, (20000, 100000))
    ]:
        print(f"\n{dept} Department:")
        print(f"   Projects: {len(df)}")
        print(f"   Cost Range: ${df['Planned_Cost'].min():,.0f} - ${df['Planned_Cost'].max():,.0f}")
        print(f"   Expected Range: ${expected_range[0]:,} - ${expected_range[1]:,}")
        
        out_of_range = df[(df['Planned_Cost'] < expected_range[0]) | (df['Planned_Cost'] > expected_range[1])]
        if len(out_of_range) > 0:
            print(f"   ⚠️  {len(out_of_range)} projects outside expected cost range")
    
    # Final Summary
    print("\n" + "=" * 70)
    print("📋 VALIDATION SUMMARY")
    print("=" * 70)
    
    if validation_passed:
        print("\n✅ ✅ ✅  ALL VALIDATIONS PASSED  ✅ ✅ ✅")
        print("\n🎉 Data quality is excellent! Ready for Phase 3 (Database Creation)")
        print(f"\nDataset Statistics:")
        print(f"   Total Projects: {len(all_data)}")
        print(f"   Departments: 3")
        print(f"   Total Planned Budget: ${all_data['Planned_Cost'].sum():,.2f}")
        print(f"   Total Actual Budget: ${all_data['Actual_Cost'].sum():,.2f}")
        print(f"   Average Completion: {all_data['Actual_Completion'].mean():.1f}%")
    else:
        print("\n❌ ❌ ❌  VALIDATION FAILED  ❌ ❌ ❌")
        print(f"\n⚠️  Found {len(issues)} issue(s):")
        for i, issue in enumerate(issues, 1):
            print(f"   {i}. {issue}")
        print("\n🔧 Please fix these issues before proceeding to Phase 3")
    
    print("\n" + "=" * 70)
    
    return validation_passed

# Run validation
if __name__ == "__main__":
    validate_datasets()