@echo off
REM ============================================
REM Project Performance ETL - Weekly Automation
REM Runs every Monday to update project data
REM ============================================

echo ========================================
echo Project Performance ETL Pipeline
echo Started: %date% %time%
echo ========================================

REM Change to script directory
cd /d %~dp0

REM Activate virtual environment (if using one)
REM call venv\Scripts\activate

REM Run the ETL pipeline
python etl_pipeline.py

REM Check if script executed successfully
if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo ETL Pipeline completed successfully!
    echo Completed: %date% %time%
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ERROR: ETL Pipeline failed!
    echo Error code: %errorlevel%
    echo Check etl_pipeline.log for details
    echo ========================================
)

REM Keep window open for 5 seconds to see results
timeout /t 5

exit /b %errorlevel%