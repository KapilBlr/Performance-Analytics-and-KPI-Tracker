import sqlite3
import pandas as pd
from datetime import datetime
import os

def create_database():
    """
    Complete SQLite database setup for Project Performance Analytics
    Creates database, tables, and imports all CSV data
    """
    
    print("=" * 70)
    print("🗄️  PROJECT PERFORMANCE DATABASE CREATION")
    print("=" * 70)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Database file path
    db_path = 'project_performance.db'
    
    # Check if database already exists
    if os.path.exists(db_path):
        response = input(f"⚠️  Database '{db_path}' already exists. Overwrite? (yes/no): ")
        if response.lower() != 'yes':
            print("❌ Operation cancelled by user.")
            return
        os.remove(db_path)
        print(f"🗑️  Removed existing database\n")
    
    # Create connection
    print(f"📁 Creating database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    print("✅ Database connection established\n")
    
    # =========================================================================
    # STEP 1: CREATE TABLES
    # =========================================================================
    print("-" * 70)
    print("STEP 1: Creating Database Schema")
    print("-" * 70)
    
    # Main projects table
    print("\n📊 Creating 'projects' table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            Project_ID INTEGER PRIMARY KEY,
            Department TEXT NOT NULL,
            Project_Name TEXT NOT NULL,
            Manager TEXT NOT NULL,
            Start_Date DATE NOT NULL,
            End_Date DATE NOT NULL,
            Planned_Cost REAL NOT NULL,
            Actual_Cost REAL NOT NULL,
            Planned_Completion INTEGER NOT NULL,
            Actual_Completion INTEGER NOT NULL,
            Status TEXT NOT NULL,
            Tasks_Total INTEGER NOT NULL,
            Tasks_Completed INTEGER NOT NULL,
            Planned_Hours INTEGER NOT NULL,
            Actual_Hours INTEGER NOT NULL,
            Created_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CHECK (Planned_Cost >= 0),
            CHECK (Actual_Cost >= 0),
            CHECK (Planned_Completion >= 0 AND Planned_Completion <= 100),
            CHECK (Actual_Completion >= 0 AND Actual_Completion <= 100),
            CHECK (Tasks_Completed <= Tasks_Total),
            CHECK (Status IN ('Not Started', 'In Progress', 'Completed', 'Delayed', 'On Hold'))
        )
    """)
    print("✅ 'projects' table created with constraints")
    
    # Departments reference table
    print("\n📊 Creating 'departments' table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            Department_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Department_Name TEXT UNIQUE NOT NULL,
            Budget_Min REAL,
            Budget_Max REAL,
            Created_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✅ 'departments' table created")
    
    # Managers reference table
    print("\n📊 Creating 'managers' table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS managers (
            Manager_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Manager_Name TEXT UNIQUE NOT NULL,
            Department TEXT NOT NULL,
            Email TEXT,
            Created_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✅ 'managers' table created")
    
    # KPI metrics view (calculated fields)
    print("\n📊 Creating 'kpi_metrics' view...")
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS kpi_metrics AS
        SELECT 
            Project_ID,
            Department,
            Project_Name,
            Manager,
            Status,
            
            -- Cost metrics
            Planned_Cost,
            Actual_Cost,
            (Actual_Cost - Planned_Cost) AS Cost_Variance,
            ROUND(((Actual_Cost - Planned_Cost) / Planned_Cost * 100), 2) AS Cost_Variance_Pct,
            
            -- Time metrics
            Start_Date,
            End_Date,
            julianday(End_Date) - julianday(Start_Date) AS Duration_Days,
            
            -- Progress metrics
            Planned_Completion,
            Actual_Completion,
            (Actual_Completion - Planned_Completion) AS Completion_Variance,
            
            -- Task metrics
            Tasks_Total,
            Tasks_Completed,
            ROUND((Tasks_Completed * 100.0 / Tasks_Total), 2) AS Task_Completion_Pct,
            
            -- Resource metrics
            Planned_Hours,
            Actual_Hours,
            (Actual_Hours - Planned_Hours) AS Hours_Variance,
            ROUND(((Actual_Hours - Planned_Hours) / Planned_Hours * 100), 2) AS Hours_Variance_Pct,
            
            -- Status flags
            CASE 
                WHEN Status = 'Completed' AND Actual_Completion = 100 THEN 'On-Time'
                WHEN Status = 'Delayed' THEN 'Delayed'
                ELSE 'In-Progress'
            END AS Delivery_Status,
            
            CASE 
                WHEN ABS((Actual_Cost - Planned_Cost) / Planned_Cost * 100) <= 10 THEN 'Within Budget'
                WHEN Actual_Cost > Planned_Cost THEN 'Over Budget'
                ELSE 'Under Budget'
            END AS Budget_Status
            
        FROM projects
    """)
    print("✅ 'kpi_metrics' view created (calculated KPIs)")
    
    conn.commit()
    
    # =========================================================================
    # STEP 2: POPULATE REFERENCE TABLES
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 2: Populating Reference Tables")
    print("-" * 70)
    
    # Insert departments
    print("\n📥 Inserting department data...")
    departments_data = [
        ('Marketing', 10000, 50000),
        ('Operations', 15000, 75000),
        ('IT', 20000, 100000)
    ]
    cursor.executemany("""
        INSERT INTO departments (Department_Name, Budget_Min, Budget_Max)
        VALUES (?, ?, ?)
    """, departments_data)
    print(f"✅ Inserted {len(departments_data)} departments")
    
    # Insert managers (we'll extract from CSV)
    print("\n📥 Extracting and inserting manager data...")
    
    conn.commit()
    
    # =========================================================================
    # STEP 3: IMPORT CSV DATA
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 3: Importing Project Data from CSV Files")
    print("-" * 70)
    
    csv_files = [
        ('marketing_projects.csv', 'Marketing'),
        ('operations_projects.csv', 'Operations'),
        ('it_projects.csv', 'IT')
    ]
    
    total_imported = 0
    
    for csv_file, dept in csv_files:
        print(f"\n📂 Processing: {csv_file}")
        
        try:
            # Read CSV
            df = pd.read_csv(csv_file)
            print(f"   📊 Loaded {len(df)} records")
            
            # Import to projects table
            df.to_sql('projects', conn, if_exists='append', index=False)
            print(f"   ✅ Imported {len(df)} projects to database")
            
            # Extract unique managers
            managers = df[['Manager', 'Department']].drop_duplicates()
            for _, row in managers.iterrows():
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO managers (Manager_Name, Department, Email)
                        VALUES (?, ?, ?)
                    """, (row['Manager'], row['Department'], f"{row['Manager'].lower().replace(' ', '.')}@company.com"))
                except:
                    pass
            
            total_imported += len(df)
            
        except FileNotFoundError:
            print(f"   ❌ ERROR: File '{csv_file}' not found!")
            print(f"   ℹ️  Please run 'generate_project_data.py' first")
        except Exception as e:
            print(f"   ❌ ERROR importing {csv_file}: {str(e)}")
    
    conn.commit()
    
    # =========================================================================
    # STEP 4: CREATE INDEXES FOR PERFORMANCE
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 4: Creating Indexes for Query Performance")
    print("-" * 70)
    
    indexes = [
        ("idx_department", "projects", "Department"),
        ("idx_manager", "projects", "Manager"),
        ("idx_status", "projects", "Status"),
        ("idx_dates", "projects", "Start_Date, End_Date")
    ]
    
    for idx_name, table, columns in indexes:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns})")
        print(f"✅ Created index: {idx_name}")
    
    conn.commit()
    
    # =========================================================================
    # STEP 5: VERIFY DATA
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 5: Database Verification")
    print("-" * 70)
    
    # Count records
    cursor.execute("SELECT COUNT(*) FROM projects")
    project_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM departments")
    dept_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM managers")
    manager_count = cursor.fetchone()[0]
    
    print(f"\n📊 Record Counts:")
    print(f"   Projects: {project_count}")
    print(f"   Departments: {dept_count}")
    print(f"   Managers: {manager_count}")
    
    # Department distribution
    cursor.execute("""
        SELECT Department, COUNT(*) as Count
        FROM projects
        GROUP BY Department
    """)
    dept_dist = cursor.fetchall()
    
    print(f"\n📈 Projects by Department:")
    for dept, count in dept_dist:
        print(f"   {dept}: {count}")
    
    # Status distribution
    cursor.execute("""
        SELECT Status, COUNT(*) as Count
        FROM projects
        GROUP BY Status
        ORDER BY Count DESC
    """)
    status_dist = cursor.fetchall()
    
    print(f"\n🚦 Projects by Status:")
    for status, count in status_dist:
        print(f"   {status}: {count}")
    
    # Budget summary
    cursor.execute("""
        SELECT 
            SUM(Planned_Cost) as Total_Planned,
            SUM(Actual_Cost) as Total_Actual,
            ROUND(AVG(Actual_Cost - Planned_Cost), 2) as Avg_Variance
        FROM projects
    """)
    budget = cursor.fetchone()
    
    print(f"\n💰 Budget Summary:")
    print(f"   Total Planned: ${budget[0]:,.2f}")
    print(f"   Total Actual: ${budget[1]:,.2f}")
    print(f"   Average Variance: ${budget[2]:,.2f}")
    
    # =========================================================================
    # STEP 6: SAMPLE QUERIES
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 6: Sample Query Results")
    print("-" * 70)
    
    print("\n📊 Top 5 Most Expensive Projects:")
    cursor.execute("""
        SELECT Project_ID, Project_Name, Department, Actual_Cost, Status
        FROM projects
        ORDER BY Actual_Cost DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1][:30]:<30} | {row[2]:<12} | ${row[3]:>10,.2f} | {row[4]}")
    
    print("\n📊 KPI Summary by Department:")
    cursor.execute("""
        SELECT 
            Department,
            COUNT(*) as Total_Projects,
            ROUND(AVG(Actual_Completion), 1) as Avg_Completion,
            ROUND(AVG(Cost_Variance_Pct), 2) as Avg_Cost_Variance_Pct,
            SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END) as Completed
        FROM kpi_metrics
        GROUP BY Department
    """)
    print(f"   {'Department':<12} | {'Projects':<8} | {'Avg Comp%':<10} | {'Cost Var%':<12} | {'Completed':<9}")
    print(f"   {'-'*12}-+-{'-'*8}-+-{'-'*10}-+-{'-'*12}-+-{'-'*9}")
    for row in cursor.fetchall():
        print(f"   {row[0]:<12} | {row[1]:<8} | {row[2]:<10} | {row[3]:<12} | {row[4]:<9}")
    
    # Close connection
    conn.close()
    
    # =========================================================================
    # COMPLETION SUMMARY
    # =========================================================================
    print("\n" + "=" * 70)
    print("✅ DATABASE CREATION COMPLETE!")
    print("=" * 70)
    
    print(f"\n📁 Database File: {os.path.abspath(db_path)}")
    print(f"📊 Total Projects Imported: {project_count}")
    print(f"🗂️  Tables Created: 4 (projects, departments, managers, kpi_metrics)")
    print(f"📇 Indexes Created: {len(indexes)}")
    
    print("\n🔌 Connection String for Power BI:")
    print(f"   SQLite Database: {os.path.abspath(db_path)}")
    
    print("\n📚 Available Tables & Views:")
    print("   • projects - Main project data")
    print("   • departments - Department reference")
    print("   • managers - Manager reference")
    print("   • kpi_metrics - Calculated KPIs (VIEW)")
    
    print("\n🎯 Next Steps:")
    print("   1. Test queries using 'test_database_queries.py'")
    print("   2. Connect Power BI to this database")
    print("   3. Start building dashboards!")
    
    print("\n" + "=" * 70)
    print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

# Run the database creation
if __name__ == "__main__":
    try:
        create_database()
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        print("Please check that CSV files exist and try again.")