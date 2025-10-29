import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Set random seed for reproducibility
random.seed(42)
np.random.seed(42)

# Department configurations
DEPT_CONFIG = {
    'Marketing': {
        'managers': ['John Smith', 'Sarah Lee', 'Akash Verma', 'Emma Watson'],
        'cost_range': (10000, 50000),
        'duration_range': (14, 84),  # 2-12 weeks
        'project_types': [
            'Q1 Product Launch', 'Social Media Campaign', 'Brand Awareness Drive',
            'Content Marketing Strategy', 'Email Marketing Automation', 
            'Market Research Study', 'Influencer Partnership Program',
            'Website Redesign', 'SEO Optimization Project', 'Customer Survey Initiative',
            'Trade Show Participation', 'Video Marketing Campaign', 
            'Rebranding Initiative', 'PR Campaign Launch', 'Lead Generation Program',
            'Mobile App Launch', 'Partnership Marketing', 'Event Marketing Strategy',
            'Loyalty Program Rollout', 'Digital Ad Campaign'
        ]
    },
    'Operations': {
        'managers': ['Priya Mehta', 'Ravi Patel', 'Aditi Nair', 'Vikram Singh'],
        'cost_range': (15000, 75000),
        'duration_range': (28, 112),  # 4-16 weeks
        'project_types': [
            'Warehouse Optimization', 'Supply Chain Integration', 'Quality Control Enhancement',
            'Inventory Management System', 'Vendor Management Platform',
            'Process Automation Initiative', 'Logistics Network Redesign',
            'Cost Reduction Program', 'Safety Compliance Upgrade', 
            'Equipment Maintenance Overhaul', 'Distribution Center Expansion',
            'Lean Manufacturing Implementation', 'Six Sigma Project',
            'Fleet Management System', 'Production Line Upgrade',
            'Sustainability Initiative', 'Waste Reduction Program',
            'Supplier Diversification', 'Demand Forecasting System', 
            'Cold Chain Management'
        ]
    },
    'IT': {
        'managers': ['Neha Sharma', 'Karan Gupta', 'Rohit Singh', 'Anjali Desai'],
        'cost_range': (20000, 100000),
        'duration_range': (42, 168),  # 6-24 weeks
        'project_types': [
            'Cloud Migration Project', 'Cybersecurity Enhancement', 'ERP System Upgrade',
            'Data Center Modernization', 'Mobile App Development',
            'API Integration Platform', 'DevOps Pipeline Implementation',
            'Disaster Recovery Setup', 'Network Infrastructure Upgrade',
            'CRM System Implementation', 'Business Intelligence Dashboard',
            'Software License Optimization', 'Database Migration',
            'Microservices Architecture', 'AI/ML Model Deployment',
            'Zero Trust Security Model', 'Backup System Overhaul',
            'Legacy System Decommission', 'IoT Platform Development',
            'Blockchain Integration'
        ]
    }
}

def generate_realistic_project(dept_name, project_id, project_name, config):
    """Generate a single realistic project with proper business logic"""
    
    # Select random manager
    manager = random.choice(config['managers'])
    
    # Generate dates
    start_date = datetime(2024, random.randint(7, 12), random.randint(1, 28))
    duration_days = random.randint(*config['duration_range'])
    end_date = start_date + timedelta(days=duration_days)
    
    # Generate costs with realistic variance
    planned_cost = random.randint(*config['cost_range'])
    cost_variance = random.uniform(-0.15, 0.20)  # -15% to +20%
    actual_cost = round(planned_cost * (1 + cost_variance), 2)
    
    # Generate task metrics
    tasks_total = random.randint(30, 80)
    
    # Determine status and completion realistically
    status_roll = random.random()
    if status_roll < 0.40:  # 40% completed
        status = 'Completed'
        planned_completion = 100
        actual_completion = 100
        tasks_completed = tasks_total
    elif status_roll < 0.75:  # 35% in progress
        status = 'In Progress'
        planned_completion = 100
        actual_completion = random.randint(50, 95)
        tasks_completed = int(tasks_total * (actual_completion / 100))
    elif status_roll < 0.85:  # 10% delayed
        status = 'Delayed'
        planned_completion = 100
        actual_completion = random.randint(70, 90)
        tasks_completed = int(tasks_total * (actual_completion / 100))
    elif status_roll < 0.95:  # 10% not started
        status = 'Not Started'
        planned_completion = 100
        actual_completion = 0
        tasks_completed = 0
    else:  # 5% on hold
        status = 'On Hold'
        planned_completion = 100
        actual_completion = random.randint(20, 60)
        tasks_completed = int(tasks_total * (actual_completion / 100))
    
    # Generate hours
    planned_hours = random.randint(400, 1200)
    if status == 'Completed':
        hours_variance = random.uniform(-0.10, 0.25)
    elif status == 'In Progress':
        hours_variance = random.uniform(0, actual_completion/100)
    else:
        hours_variance = random.uniform(0, 0.5)
    actual_hours = int(planned_hours * (1 + hours_variance))
    
    return {
        'Project_ID': project_id,
        'Department': dept_name,
        'Project_Name': project_name,
        'Manager': manager,
        'Start_Date': start_date.strftime('%Y-%m-%d'),
        'End_Date': end_date.strftime('%Y-%m-%d'),
        'Planned_Cost': planned_cost,
        'Actual_Cost': actual_cost,
        'Planned_Completion': planned_completion,
        'Actual_Completion': actual_completion,
        'Status': status,
        'Tasks_Total': tasks_total,
        'Tasks_Completed': tasks_completed,
        'Planned_Hours': planned_hours,
        'Actual_Hours': actual_hours
    }

def generate_department_dataset(dept_name, id_start, num_projects=20):
    """Generate complete dataset for a department"""
    config = DEPT_CONFIG[dept_name]
    projects = []
    
    # Use unique project names
    project_names = random.sample(config['project_types'], num_projects)
    
    for i in range(num_projects):
        project_id = id_start + i
        project = generate_realistic_project(
            dept_name, 
            project_id, 
            project_names[i],
            config
        )
        projects.append(project)
    
    df = pd.DataFrame(projects)
    return df

# Generate datasets for all three departments
print("🚀 Generating Project Performance Data...\n")

print("📊 Marketing Department (20 projects)...")
marketing_df = generate_department_dataset('Marketing', 1001, 20)
marketing_df.to_csv('marketing_projects.csv', index=False)
print(f"✅ Created: marketing_projects.csv ({len(marketing_df)} rows)\n")

print("📊 Operations Department (20 projects)...")
operations_df = generate_department_dataset('Operations', 2001, 20)
operations_df.to_csv('operations_projects.csv', index=False)
print(f"✅ Created: operations_projects.csv ({len(operations_df)} rows)\n")

print("📊 IT Department (20 projects)...")
it_df = generate_department_dataset('IT', 3001, 20)
it_df.to_csv('it_projects.csv', index=False)
print(f"✅ Created: it_projects.csv ({len(it_df)} rows)\n")

# Generate summary statistics
print("=" * 60)
print("📈 DATA GENERATION SUMMARY")
print("=" * 60)

all_depts = pd.concat([marketing_df, operations_df, it_df], ignore_index=True)

print(f"\n📁 Total Projects Generated: {len(all_depts)}")
print(f"📁 Total Departments: 3")
print(f"\n💰 Budget Statistics:")
print(f"   Total Planned Cost: ${all_depts['Planned_Cost'].sum():,.2f}")
print(f"   Total Actual Cost: ${all_depts['Actual_Cost'].sum():,.2f}")
print(f"   Overall Cost Variance: {((all_depts['Actual_Cost'].sum() - all_depts['Planned_Cost'].sum()) / all_depts['Planned_Cost'].sum() * 100):.2f}%")

print(f"\n📊 Status Distribution:")
status_counts = all_depts['Status'].value_counts()
for status, count in status_counts.items():
    print(f"   {status}: {count} ({count/len(all_depts)*100:.1f}%)")

print(f"\n🎯 Average Completion Rate: {all_depts['Actual_Completion'].mean():.1f}%")
print(f"⏱️  Average Planned Hours: {all_depts['Planned_Hours'].mean():.0f}")
print(f"⏱️  Average Actual Hours: {all_depts['Actual_Hours'].mean():.0f}")

print("\n" + "=" * 60)
print("✅ PHASE 2 COMPLETE - All datasets generated successfully!")
print("=" * 60)

# Display sample data
print("\n📋 SAMPLE DATA (First 5 Marketing Projects):")
print(marketing_df.head().to_string(index=False))

print("\n💾 Files created:")
print("   • marketing_projects.csv")
print("   • operations_projects.csv")
print("   • it_projects.csv")
print("\n🎉 Ready for Phase 3: Database Creation!")