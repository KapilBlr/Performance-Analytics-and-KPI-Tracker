import sqlite3
import pandas as pd
from datetime import datetime

def test_database_queries():
    """
    Comprehensive test suite for database queries
    Validates data and demonstrates useful analytics queries
    """
    
    print("=" * 70)
    print("🧪 DATABASE QUERY TESTING SUITE")
    print("=" * 70)
    print(f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Connect to database
    try:
        conn = sqlite3.connect('project_performance.db')
        print("✅ Connected to database: project_performance.db\n")
    except Exception as e:
        print(f"❌ ERROR: Could not connect to database")
        print(f"   {str(e)}")
        print("\n   Please run 'create_database.py' first!")
        return
    
    # =========================================================================
    # TEST 1: Basic Table Queries
    # =========================================================================
    print("-" * 70)
    print("TEST 1: Basic Table Queries")
    print("-" * 70)
    
    queries = {
        "Total Projects": "SELECT COUNT(*) FROM projects",
        "Total Departments": "SELECT COUNT(*) FROM departments",
        "Total Managers": "SELECT COUNT(*) FROM managers",
        "Completed Projects": "SELECT COUNT(*) FROM projects WHERE Status = 'Completed'",
        "In Progress Projects": "SELECT COUNT(*) FROM projects WHERE Status = 'In Progress'"
    }
    
    for desc, query in queries.items():
        result = pd.read_sql(query, conn).iloc[0, 0]
        print(f"✓ {desc:<25}: {result}")
    
    # =========================================================================
    # TEST 2: KPI Calculations
    # =========================================================================
    print("\n" + "-" * 70)
    print("TEST 2: Key Performance Indicators (KPIs)")
    print("-" * 70)
    
    # On-Time Delivery %
    kpi_query = """
    SELECT 
        ROUND(
            (SUM(CASE WHEN Status = 'Completed' AND Actual_Completion = 100 THEN 1 ELSE 0 END) * 100.0) 
            / COUNT(*), 2
        ) as On_Time_Delivery_Pct
    FROM projects
    """
    on_time = pd.read_sql(kpi_query, conn).iloc[0, 0]
    print(f"\n📊 On-Time Delivery Rate: {on_time}%")
    
    # Average Cost Variance
    kpi_query = """
    SELECT 
        ROUND(AVG((Actual_Cost - Planned_Cost) / Planned_Cost * 100), 2) as Avg_Cost_Variance_Pct
    FROM projects
    """
    cost_var = pd.read_sql(kpi_query, conn).iloc[0, 0]
    print(f"💰 Average Cost Variance: {cost_var}%")
    
    # Resource Utilization
    kpi_query = """
    SELECT 
        ROUND((SUM(Actual_Hours) * 100.0) / SUM(Planned_Hours), 2) as Resource_Utilization_Pct
    FROM projects
    """
    resource_util = pd.read_sql(kpi_query, conn).iloc[0, 0]
    print(f"⚙️  Resource Utilization: {resource_util}%")
    
    # Task Completion Rate
    kpi_query = """
    SELECT 
        ROUND((SUM(Tasks_Completed) * 100.0) / SUM(Tasks_Total), 2) as Task_Completion_Rate
    FROM projects
    """
    task_rate = pd.read_sql(kpi_query, conn).iloc[0, 0]
    print(f"✔️  Task Completion Rate: {task_rate}%")
    
    # Average Project Delay
    kpi_query = """
    SELECT 
        ROUND(AVG(julianday(End_Date) - julianday(Start_Date)), 1) as Avg_Duration_Days
    FROM projects
    WHERE Status = 'Delayed'
    """
    avg_delay = pd.read_sql(kpi_query, conn).iloc[0, 0]
    if avg_delay:
        print(f"⏰ Average Project Duration (Delayed): {avg_delay} days")
    
    # =========================================================================
    # TEST 3: Department Analysis
    # =========================================================================
    print("\n" + "-" * 70)
    print("TEST 3: Department Performance Analysis")
    print("-" * 70)
    
    dept_query = """
    SELECT 
        Department,
        COUNT(*) as Total_Projects,
        SUM(Planned_Cost) as Total_Budget,
        SUM(Actual_Cost) as Total_Spent,
        ROUND(AVG(Actual_Completion), 1) as Avg_Completion,
        SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END) as Completed_Projects,
        ROUND((SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 1) as Completion_Rate
    FROM projects
    GROUP BY Department
    ORDER BY Total_Budget DESC
    """
    
    dept_df = pd.read_sql(dept_query, conn)
    print("\n📊 Department Performance Summary:")
    print(dept_df.to_string(index=False))
    
    # =========================================================================
    # TEST 4: Manager Performance
    # =========================================================================
    print("\n" + "-" * 70)
    print("TEST 4: Manager Performance Ranking")
    print("-" * 70)
    
    manager_query = """
    SELECT 
        Manager,
        Department,
        COUNT(*) as Projects_Managed,
        ROUND(AVG(Actual_Completion), 1) as Avg_Completion,
        ROUND(AVG((Actual_Cost - Planned_Cost) / Planned_Cost * 100), 2) as Avg_Cost_Variance_Pct,
        SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END) as Completed
    FROM projects
    GROUP BY Manager, Department
    ORDER BY Avg_Completion DESC
    LIMIT 10
    """
    
    manager_df = pd.read_sql(manager_query, conn)
    print("\n🏆 Top Performing Managers:")
    print(manager_df.to_string(index=False))
    
    # =========================================================================
    # TEST 5: Budget Analysis
    # =========================================================================
    print("\n" + "-" * 70)
    print("TEST 5: Budget Performance Analysis")
    print("-" * 70)
    
    budget_query = """
    SELECT 
        CASE 
            WHEN ((Actual_Cost - Planned_Cost) / Planned_Cost * 100) <= -10 THEN 'Significantly Under Budget'
            WHEN ((Actual_Cost - Planned_Cost) / Planned_Cost * 100) <= 0 THEN 'Under Budget'
            WHEN ((Actual_Cost - Planned_Cost) / Planned_Cost * 100) <= 10 THEN 'Within Budget'
            ELSE 'Over Budget'
        END as Budget_Category,
        COUNT(*) as Project_Count,
        ROUND(AVG((Actual_Cost - Planned_Cost) / Planned_Cost * 100), 2) as Avg_Variance_Pct
    FROM projects
    GROUP BY Budget_Category
    ORDER BY Avg_Variance_Pct
    """
    
    budget_df = pd.read_sql(budget_query, conn)
    print("\n💵 Budget Performance Distribution:")
    print(budget_df.to_string(index=False))
    
    # =========================================================================
    # TEST 6: Status Timeline
    # =========================================================================
    print("\n" + "-" * 70)
    print("TEST 6: Project Status Distribution")
    print("-" * 70)
    
    status_query = """
    SELECT 
        Status,
        COUNT(*) as Count,
        ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM projects), 1) as Percentage,
        ROUND(AVG(Actual_Completion), 1) as Avg_Completion
    FROM projects
    GROUP BY Status
    ORDER BY Count DESC
    """
    
    status_df = pd.read_sql(status_query, conn)
    print("\n🚦 Status Breakdown:")
    print(status_df.to_string(index=False))
    
    # =========================================================================
    # TEST 7: Risk Analysis
    # =========================================================================
    print("\n" + "-" * 70)
    print("TEST 7: Risk & Problem Projects")
    print("-" * 70)
    
    risk_query = """
    SELECT 
        Project_ID,
        Project_Name,
        Department,
        Status,
        ROUND(((Actual_Cost - Planned_Cost) / Planned_Cost * 100), 2) as Cost_Variance_Pct,
        Actual_Completion
    FROM projects
    WHERE 
        (Status = 'Delayed' OR Status = 'On Hold')
        OR ((Actual_Cost - Planned_Cost) / Planned_Cost * 100) > 20
        OR Actual_Completion < 50
    ORDER BY Cost_Variance_Pct DESC
    LIMIT 10
    """
    
    risk_df = pd.read_sql(risk_query, conn)
    print(f"\n⚠️  High-Risk Projects ({len(risk_df)} found):")
    if len(risk_df) > 0:
        print(risk_df.to_string(index=False))
    else:
        print("   No high-risk projects found!")
    
    # =========================================================================
    # TEST 8: Using KPI Metrics View
    # =========================================================================
    print("\n" + "-" * 70)
    print("TEST 8: KPI Metrics View (Calculated Fields)")
    print("-" * 70)
    
    kpi_view_query = """
    SELECT 
        Department,
        COUNT(*) as Projects,
        ROUND(AVG(Cost_Variance_Pct), 2) as Avg_Cost_Var,
        ROUND(AVG(Duration_Days), 1) as Avg_Duration,
        ROUND(AVG(Task_Completion_Pct), 1) as Avg_Task_Completion,
        COUNT(CASE WHEN Budget_Status = 'Within Budget' THEN 1 END) as Within_Budget_Count
    FROM kpi_metrics
    GROUP BY Department
    """
    
    kpi_view_df = pd.read_sql(kpi_view_query, conn)
    print("\n📈 Department KPIs (from view):")
    print(kpi_view_df.to_string(index=False))
    
    # =========================================================================
    # TEST 9: Time-Based Analysis
    # =========================================================================
    print("\n" + "-" * 70)
    print("TEST 9: Time-Based Project Analysis")
    print("-" * 70)
    
    time_query = """
    SELECT 
        strftime('%Y-%m', Start_Date) as Month,
        COUNT(*) as Projects_Started,
        ROUND(AVG(julianday(End_Date) - julianday(Start_Date)), 1) as Avg_Duration_Days
    FROM projects
    GROUP BY strftime('%Y-%m', Start_Date)
    ORDER BY Month
    """
    
    time_df = pd.read_sql(time_query, conn)
    print("\n📅 Projects Started by Month:")
    print(time_df.to_string(index=False))
    
    # =========================================================================
    # TEST 10: Export Test Data
    # =========================================================================
    print("\n" + "-" * 70)
    print("TEST 10: Export Consolidated Data")
    print("-" * 70)
    
    export_query = """
    SELECT * FROM kpi_metrics
    """
    
    consolidated_df = pd.read_sql(export_query, conn)
    consolidated_df.to_csv('consolidated_projects.csv', index=False)
    print(f"\n💾 Exported consolidated data:")
    print(f"   File: consolidated_projects.csv")
    print(f"   Records: {len(consolidated_df)}")
    print(f"   Columns: {len(consolidated_df.columns)}")
    
    # Close connection
    conn.close()
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 70)
    print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    
    print("\n📊 Database is ready for:")
    print("   • Power BI connection")
    print("   • Python automation scripts")
    print("   • Excel integration")
    print("   • API development")
    
    print("\n🎯 Recommended Next Steps:")
    print("   1. Connect Power BI to 'project_performance.db'")
    print("   2. Use 'kpi_metrics' view for dashboard visuals")
    print("   3. Create automated reporting scripts")
    print("   4. Set up weekly data refresh")
    
    print("\n" + "=" * 70)

# Run tests
if __name__ == "__main__":
    test_database_queries()