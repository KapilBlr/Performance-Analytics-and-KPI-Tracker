---

## 📊 Key Performance Indicators (KPIs)

### 1. **Timeliness Metrics**
- **On-Time Delivery %**: Projects completed on or before deadline
- **Average Duration**: Mean project completion time

### 2. **Financial Metrics**
- **Cost Variance %**: Difference between planned and actual budget
- **Budget Utilization**: Actual vs. planned expenditure

### 3. **Efficiency Metrics**
- **Task Completion Rate**: Percentage of completed tasks
- **Resource Utilization**: Team workload distribution

### 4. **Quality Metrics**
- **Project Status Distribution**: Completed, In Progress, Delayed, On Hold
- **Department Performance**: Comparative analysis across units

---

## 🚀 Features

### Data Integration
- ✅ Automated data collection from 3 departments (Marketing, Operations, IT)
- ✅ ETL pipeline for data cleaning, transformation, and validation
- ✅ Consolidated database with 60+ project records

### Analytics & Visualization
- ✅ Interactive Power BI dashboards with 4 pages
- ✅ 10+ calculated DAX measures for real-time KPI tracking
- ✅ Cross-departmental filtering and drill-down capabilities
- ✅ Visual analytics: KPI cards, bar charts, donut charts, line charts, data tables

### Automation
- ✅ Weekly automated data refresh pipeline
- ✅ Scheduled ETL execution via Task Scheduler
- ✅ One-click dashboard refresh

---

## 📈 Dashboard Pages

### Page 1: Executive Overview
- High-level KPIs across all departments
- Status distribution by department
- Monthly completion trends
- Top 10 projects by duration

### Page 2: Marketing Dashboard
- Marketing-specific KPIs
- Campaign performance metrics
- Budget vs. actual analysis

### Page 3: Operations Dashboard
- Supply chain project tracking
- Logistics performance metrics
- Quality control initiatives

### Page 4: IT Dashboard
- Technical project monitoring
- Infrastructure upgrade tracking
- System implementation progress

---

## 🔧 Installation & Setup

### Prerequisites
```bash
Python 3.8+
MySQL / PostgreSQL
Power BI Desktop
```

### Step 1: Clone Repository
```bash
git clone https://github.com/yourusername/project-performance-analytics.git
cd project-performance-analytics
```

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Database Setup
```bash
# Create database
mysql -u root -p < database/schema.sql

# Or run Python setup
python database/database_setup.py
```

### Step 4: Configure Connection
Edit `config.py`:
```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'project_performance'
}
```

### Step 5: Run ETL Pipeline
```bash
python scripts/etl_pipeline.py
```

### Step 6: Open Power BI Dashboard
```bash
# Open dashboards/executive_dashboard.pbix in Power BI Desktop
# Click "Refresh" to load latest data
```

---

## 📊 Sample Data Schema

### consolidated_projects Table

| Column | Type | Description |
|--------|------|-------------|
| Project_ID | INT | Unique project identifier |
| Department | VARCHAR | Marketing, Operations, or IT |
| Project_Name | VARCHAR | Project title |
| Manager | VARCHAR | Project manager name |
| Status | VARCHAR | Completed, In Progress, Not Started, On Hold |
| Planned_Cost | FLOAT | Budgeted amount |
| Actual_Cost | FLOAT | Actual expenditure |
| Cost_Variance_Pct | FLOAT | Budget variance percentage |
| Start_Date | DATE | Project start date |
| End_Date | DATE | Project end date |
| Duration_Days | INT | Project duration |
| Task_Completion_Pct | FLOAT | Percentage of tasks completed |
| Delivery_Status | VARCHAR | On-Time or In-Progress |
| Budget_Status | VARCHAR | Within Budget, Over Budget, Under Budget |

---

## 🎯 Key Insights Generated

### Departmental Performance
- **Marketing**: 20 projects, 35% completed, 11.5% avg cost variance
- **Operations**: 20 projects, 45% completed, 5.2% avg cost variance
- **IT**: 20 projects, 40% completed, 2.8% avg cost variance

### Overall Metrics
- **On-Time Delivery**: 82.4% (target: 85%)
- **Budget Compliance**: 68% within budget
- **Average Project Duration**: 75 days

---

## 🔄 Automation Workflow