# -*- coding: utf-8 -*-
import gradio as gr
import snowflake.connector
import pandas as pd
from datetime import datetime
import re
import os

# ========== SNOWFLAKE FUNCTIONS ==========
def get_snowflake_connection(user, password, account):
    """Establish connection to Snowflake"""
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            authenticator='snowflake'
        )
        return conn, "✅ Successfully connected!"
    except Exception as e:
        return None, f"❌ Connection failed: {str(e)}"

def disconnect_snowflake(conn):
    """Close Snowflake connection"""
    if conn:
        conn.close()
    return None, "🔌 Disconnected successfully"

def get_databases(conn):
    """Get list of databases"""
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        return [row[1] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error getting databases: {str(e)}")
        return []

def get_schemas(conn, database):
    """Get schemas for specific database"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        return [row[1] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error getting schemas: {str(e)}")
        return []

def get_tables(conn, database, schema):
    """Get tables for specific schema"""
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
        return ["All"] + [row[1] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error getting tables: {str(e)}")
        return ["All"]

def clone_schema(conn, source_db, source_schema, target_schema):
    """Clone schema with improved error handling and reporting"""
    cursor = conn.cursor()
    try:
        # First check if source schema exists
        cursor.execute(f"SHOW SCHEMAS LIKE '{source_schema}' IN DATABASE {source_db}")
        if not cursor.fetchall():
            return False, f"❌ Source schema {source_db}.{source_schema} doesn't exist", pd.DataFrame()

        # Execute clone command
        cursor.execute(
            f"CREATE OR REPLACE SCHEMA {source_db}.{target_schema} "
            f"CLONE {source_db}.{source_schema}"
        )

        # Verify clone was successful
        cursor.execute(f"SHOW SCHEMAS LIKE '{target_schema}' IN DATABASE {source_db}")
        if not cursor.fetchall():
            return False, f"❌ Clone failed - target schema not created", pd.DataFrame()

        # Get list of cloned tables
        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{source_schema}")
        source_tables = [row[1] for row in cursor.fetchall()]

        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{target_schema}")
        clone_tables = [row[1] for row in cursor.fetchall()]

        # Create summary DataFrame
        df_tables = pd.DataFrame({
            'Database': source_db,
            'Source Schema': source_schema,
            'Clone Schema': target_schema,
            'Source Tables': len(source_tables),
            'Cloned Tables': len(clone_tables),
            'Status': '✅ Success' if len(source_tables) == len(clone_tables) else '⚠️ Partial Success'
        }, index=[0])

        return True, f"✅ Successfully Mirrored Schema {source_db}.{source_schema} to {source_db}.{target_schema}", df_tables
    except Exception as e:
        return False, f"❌ Clone failed: {str(e)}", pd.DataFrame()

def compare_table_differences(conn, db_name, source_schema, clone_schema):
    """Compare tables between schemas"""
    cursor = conn.cursor()

    query = f"""
    WITH source_tables AS (
        SELECT table_name
        FROM {db_name}.information_schema.tables
        WHERE table_schema = '{source_schema}'
    ),
    clone_tables AS (
        SELECT table_name
        FROM {db_name}.information_schema.tables
        WHERE table_schema = '{clone_schema}'
    )
    SELECT
        COALESCE(s.table_name, c.table_name) AS table_name,
        CASE
            WHEN s.table_name IS NULL THEN 'Missing in source - Table Dropped'
            WHEN c.table_name IS NULL THEN 'Missing in clone - Table Added'
            ELSE 'Present in both'
        END AS difference
    FROM source_tables s
    FULL OUTER JOIN clone_tables c ON s.table_name = c.table_name
    WHERE s.table_name IS NULL OR c.table_name IS NULL
    ORDER BY difference, table_name;
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return pd.DataFrame(results, columns=['Table', 'Difference'])

def compare_column_differences(conn, db_name, source_schema, clone_schema):
    """Compare columns and data types between schemas"""
    cursor = conn.cursor()

    # Get common tables
    common_tables_query = f"""
    SELECT s.table_name
    FROM {db_name}.information_schema.tables s
    JOIN {db_name}.information_schema.tables c
        ON s.table_name = c.table_name
    WHERE s.table_schema = '{source_schema}'
    AND c.table_schema = '{clone_schema}';
    """

    cursor.execute(common_tables_query)
    common_tables = [row[0] for row in cursor.fetchall()]

    column_diff_data = []
    datatype_diff_data = []

    for table in common_tables:
        # Get source table description
        cursor.execute(f"DESCRIBE TABLE {db_name}.{source_schema}.{table}")
        source_desc = cursor.fetchall()
        source_cols = {row[0]: row[1] for row in source_desc}

        # Get clone table description
        cursor.execute(f"DESCRIBE TABLE {db_name}.{clone_schema}.{table}")
        clone_desc = cursor.fetchall()
        clone_cols = {row[0]: row[1] for row in clone_desc}

        # Get all unique column names
        all_columns = set(source_cols.keys()).union(set(clone_cols.keys()))

        for col in all_columns:
            source_exists = col in source_cols
            clone_exists = col in clone_cols

            if not source_exists:
                column_diff_data.append({
                    'Table': table,
                    'Column': col,
                    'Difference': 'Missing in source - Column Dropped',
                    'Source Data Type': None,
                    'Clone Data Type': clone_cols.get(col)
                })
            elif not clone_exists:
                column_diff_data.append({
                    'Table': table,
                    'Column': col,
                    'Difference': 'Missing in clone - Column Added',
                    'Source Data Type': source_cols.get(col),
                    'Clone Data Type': None
                })
            else:
                # Column exists in both - check data type
                if source_cols[col] != clone_cols[col]:
                    datatype_diff_data.append({
                        'Table': table,
                        'Column': col,
                        'Source Data Type': source_cols[col],
                        'Clone Data Type': clone_cols[col],
                        'Difference': 'Data Type Changed'
                    })

    # Create DataFrames
    column_diff_df = pd.DataFrame(column_diff_data)
    if not column_diff_df.empty:
        column_diff_df = column_diff_df[['Table', 'Column', 'Difference', 'Source Data Type', 'Clone Data Type']]

    datatype_diff_df = pd.DataFrame(datatype_diff_data)
    if not datatype_diff_df.empty:
        datatype_diff_df = datatype_diff_df[['Table', 'Column', 'Source Data Type', 'Clone Data Type', 'Difference']]

    return column_diff_df, datatype_diff_df

def validate_kpis(conn, database, source_schema, target_schema):
    """Validate KPIs between source and clone schemas"""
    cursor = conn.cursor()
    results = []

    try:
        # Fetch all KPI definitions
        kpi_query = f"SELECT KPI_ID, KPI_NAME, KPI_VALUE FROM {database}.{source_schema}.ORDER_KPIS"
        cursor.execute(kpi_query)
        kpis = cursor.fetchall()

        if not kpis:
            return pd.DataFrame(), "⚠️ No KPIs found in ORDER_KPIS table."

        # First verify both schemas have the ORDER_DATA table
        try:
            cursor.execute(f"SELECT 1 FROM {database}.{source_schema}.ORDER_DATA LIMIT 1")
            source_has_table = True
        except:
            source_has_table = False

        try:
            cursor.execute(f"SELECT 1 FROM {database}.{target_schema}.ORDER_DATA LIMIT 1")
            target_has_table = True
        except:
            target_has_table = False

        if not source_has_table or not target_has_table:
            error_msg = "ORDER_DATA table missing in "
            if not source_has_table and not target_has_table:
                error_msg += "both schemas"
            elif not source_has_table:
                error_msg += "source schema"
            else:
                error_msg += "target schema"

            for kpi_id, kpi_name, kpi_sql in kpis:
                results.append({
                    'KPI ID': kpi_id,
                    'KPI Name': kpi_name,
                    'Source Value': f"ERROR: {error_msg}",
                    'Clone Value': f"ERROR: {error_msg}",
                    'Difference': "N/A",
                    'Diff %': "N/A",
                    'Status': "❌ Error"
                })
            return pd.DataFrame(results), "❌ Validation failed - missing ORDER_DATA table"

        for kpi_id, kpi_name, kpi_sql in kpis:
            try:
                # For source schema - replace only unqualified table names
                source_query = kpi_sql.replace('FROM order_data', f'FROM {database}.{source_schema}.ORDER_DATA')\
                                    .replace('FROM ORDER_DATA', f'FROM {database}.{source_schema}.ORDER_DATA')
                cursor.execute(source_query)
                result_source = cursor.fetchone()[0] if cursor.rowcount > 0 else None
            except Exception as e:
                result_source = f"QUERY_ERROR: {str(e)}"

            try:
                # For target schema - replace only unqualified table names
                clone_query = kpi_sql.replace('FROM order_data', f'FROM {database}.{target_schema}.ORDER_DATA')\
                                   .replace('FROM ORDER_DATA', f'FROM {database}.{target_schema}.ORDER_DATA')
                cursor.execute(clone_query)
                result_clone = cursor.fetchone()[0] if cursor.rowcount > 0 else None
            except Exception as e:
                result_clone = f"QUERY_ERROR: {str(e)}"

            # Calculate differences if possible
            diff = "N/A"
            pct_diff = "N/A"
            status = "⚠️ Mismatch"

            try:
                if (isinstance(result_source, (int, float)) and isinstance(result_clone, (int, float))):
                    diff = float(result_source) - float(result_clone)
                    pct_diff = (diff / float(result_source)) * 100 if float(result_source) != 0 else float('inf')
                    status = '✅ Match' if diff == 0 else '⚠️ Mismatch'
                elif str(result_source) == str(result_clone):
                    status = '✅ Match'
            except:
                pass

            results.append({
                'KPI ID': kpi_id,
                'KPI Name': kpi_name,
                'Source Value': result_source,
                'Clone Value': result_clone,
                'Difference': diff if not isinstance(diff, float) else round(diff, 2),
                'Diff %': f"{round(pct_diff, 2)}%" if isinstance(pct_diff, float) else pct_diff,
                'Status': status
            })

        df = pd.DataFrame(results)
        return df, "✅ KPI validation completed"

    except Exception as e:
        return pd.DataFrame(), f"❌ KPI validation failed: {str(e)}"

# ===== TEST CASE VALIDATION FUNCTIONS =====
def verify_table_access(conn, database, schema, table_name):
    cursor = conn.cursor()
    try:
        cursor.execute(f'SELECT 1 FROM {database}.{schema}.{table_name} LIMIT 1')
        return True
    except Exception as e:
        print(f"Access verification failed for {table_name}: {str(e)}")
        return False

def get_test_case_tables(conn, database, schema):
    """Get distinct tables from test cases table with error handling"""
    try:
        cursor = conn.cursor()
        # First verify TEST_CASES table exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM {database}.information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_name = 'TEST_CASES'
        """)
        if cursor.fetchone()[0] == 0:
            print(f"TEST_CASES table not found in {database}.{schema}")
            return ["All"]  # Return "All" even if no table exists
        
        # Now get distinct tables
        cursor.execute(f"""
            SELECT DISTINCT TABLE_NAME 
            FROM {database}.{schema}.TEST_CASES 
            WHERE TABLE_NAME IS NOT NULL
            ORDER BY TABLE_NAME
        """)
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Found tables: {tables}")
        return ["All"] + tables  # Add "All" option
    except Exception as e:
        print(f"Error getting test case tables: {str(e)}")
        return ["All"]

def get_test_cases(conn, database, schema, table):
    """Get test cases for specific table with error handling"""
    try:
        cursor = conn.cursor()
        
        # First verify the TEST_CASES table exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM {database}.information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_name = 'TEST_CASES'
        """)
        if cursor.fetchone()[0] == 0:
            print(f"TEST_CASES table not found in {database}.{schema}")
            return []

        # Now fetch test cases
        if table == "All":
            query = f"""
                SELECT 
                    TEST_CASE_ID,
                    TEST_ABBREVIATION,
                    TABLE_NAME,
                    TEST_DESCRIPTION,
                    SQL_CODE,
                    EXPECTED_RESULT
                FROM {database}.{schema}.TEST_CASES
                ORDER BY TEST_CASE_ID
            """
        else:
            query = f"""
                SELECT 
                    TEST_CASE_ID,
                    TEST_ABBREVIATION,
                    TABLE_NAME,
                    TEST_DESCRIPTION,
                    SQL_CODE,
                    EXPECTED_RESULT
                FROM {database}.{schema}.TEST_CASES
                WHERE TABLE_NAME = '{table}'
                ORDER BY TEST_CASE_ID
            """
        
        cursor.execute(query)
        cases = cursor.fetchall()
        print(f"Found {len(cases)} test cases for {database}.{schema}.{table}")
        return cases
        
    except Exception as e:
        print(f"Error getting test cases: {str(e)}")
        return []

def validate_test_cases(conn, database, schema, test_cases):
    if not test_cases:
        return pd.DataFrame(), "⚠️ No test cases selected", gr.Button(visible=False)

    cursor = conn.cursor()
    results = []

    for case in test_cases:
        test_id, abbrev, table_name, desc, sql, expected = case
        expected = str(expected).strip()

        # Verify table access first
        if not verify_table_access(conn, database, schema, table_name):
            results.append({
                'TEST CASE': abbrev,
                'CATEGORY': table_name,
                'EXPECTED RESULT': expected,
                'ACTUAL RESULT': f"ACCESS DENIED: No permissions on {table_name}",
                'STATUS': "❌ PERMISSION ERROR"
            })
            continue

        try:
            # Modify SQL to use fully qualified names
            qualified_sql = re.sub(
                rf'\b{table_name}\b', 
                f'{database}.{schema}.{table_name}', 
                sql,
                flags=re.IGNORECASE
            )
            
            cursor.execute(qualified_sql)
            result = cursor.fetchone()
            actual_result = str(result[0]) if result else "0"

            results.append({
                'TEST CASE': abbrev,
                'CATEGORY': table_name,
                'EXPECTED RESULT': expected,
                'ACTUAL RESULT': actual_result,
                'STATUS': "✅ PASS" if actual_result == expected else "❌ FAIL"
            })

        except Exception as e:
            error_msg = str(e).split('\n')[0]
            results.append({
                'TEST CASE': abbrev,
                'CATEGORY': table_name,
                'EXPECTED RESULT': expected,
                'ACTUAL RESULT': f"QUERY ERROR: {error_msg}",
                'STATUS': "❌ EXECUTION ERROR"
            })

    df = pd.DataFrame(results)
    return df, "✅ Validation completed", gr.Button(visible=True)    

# ===== GRADIO APP =====
with gr.Blocks(title="DeploySure Suite", theme=gr.themes.Soft()) as app:
    # Add company logo and header
    gr.HTML("""
    <div style="display: flex; flex-direction: column; align-items: center; margin-bottom: 10px; height: 10; width: 50;">
        <img src="data:image/png;base64,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">
        <h1 style="text-align: center; margin-top: 10px;">DeploySure Suite</h1>
    </div>
    """)

    # Session state
    conn_state = gr.State()
    current_db = gr.State()
    combined_report_state = gr.State()
    validation_type = gr.State(value="schema")  # Track current validation type
    test_case_data = gr.State([])

    # ===== SNOWFLAKE-STYLE LOGIN SECTION =====
    with gr.Tab("🔐 Login"):
        with gr.Row():
            # Center column - compact login form
            with gr.Column(scale=1, min_width=300):  # Reduced min_width
                gr.Markdown("""
                <div style="text-align: center; margin-bottom: 15px;">
                <h2 style="margin: 0; font-size: 18px;">Sign in to Snowflake</h2>
                </div>
                """)

                with gr.Group():
                    user = gr.Textbox(
                        label="Username",
                        placeholder="your_username",
                        elem_classes=["snowflake-input"]
                    )
                    password = gr.Textbox(
                        label="Password",
                        type="password",
                        placeholder="••••••••",
                        elem_classes=["snowflake-input"]
                    )
                    account = gr.Textbox(
                        label="Account",
                        placeholder="account.region",
                        elem_classes=["snowflake-input"]
                    )

                with gr.Row():
                    login_btn = gr.Button(
                        "Connect",
                        variant="primary",
                        elem_classes=["snowflake-button"],
                        scale=1
                    )
                    disconnect_btn = gr.Button(
                        "Disconnect",
                        variant="secondary",
                        visible=False,
                        elem_classes=["snowflake-button"],
                        scale=1
                    )

            status = gr.Textbox(
                label="Status",
                interactive=False,
                visible=False,
                container=False,
                elem_classes=["status-box"]
            )

    # ===== MIRROR SCHEMA TAB =====
    with gr.Tab("⎘ MirrorSchema", visible=False) as mirror_tab:
        with gr.Row():
            with gr.Column(scale=1):
                gr.Markdown("### Source Selection")
                source_db = gr.Dropdown(label="Source Database", interactive=True)
                source_schema = gr.Dropdown(label="Source Schema", interactive=True)
                target_schema = gr.Textbox(label="MirrorSchema Name", interactive=True, placeholder="Enter MirrorSchema name")
                clone_btn = gr.Button("Execute MirrorSchema", variant="primary")

            with gr.Column(scale=2):
                clone_output = gr.Textbox(label="Status", interactive=False)

    # ===== DRIFTWATCH TAB =====
    with gr.Tab("🔍 DriftWatch", visible=False) as driftwatch_tab:
        # Validation type selection dropdown
        validation_type_dropdown = gr.Dropdown(
            label="Validation Type",
            choices=["Schema Validation", "KPI Validation", "Test Case Validation"],
            value="Schema Validation",
            interactive=True
        )

        # ===== SCHEMA VALIDATION SECTION =====
        with gr.Column(visible=True) as schema_validation_section:
            with gr.Row():
                with gr.Column(scale=1):
                    gr.Markdown("### Schema Validation Configuration")
                    val_db = gr.Dropdown(label="Database")
                    val_source_schema = gr.Dropdown(label="Source Schema")
                    val_target_schema = gr.Dropdown(label="Target Schema")
                    validate_btn = gr.Button("Execute DriftWatch", variant="primary")

                with gr.Column(scale=2):
                    gr.Markdown("### ChangeLens / Schema Validation Report")
                    with gr.Tabs():
                        with gr.Tab("Table Differences"):
                            table_diff_output = gr.Dataframe(interactive=False)
                            table_download_btn = gr.Button("📥 Download Table Differences", visible=False)
                            table_download = gr.File(label="Download Table Differences", visible=False)
                        with gr.Tab("Column Differences"):
                            column_diff_output = gr.Dataframe(interactive=False)
                            column_download_btn = gr.Button("📥 Download Column Differences", visible=False)
                            column_download = gr.File(label="Download Column Differences", visible=False)
                        with gr.Tab("Data Type Differences"):
                            datatype_diff_output = gr.Dataframe(interactive=False)
                            datatype_download_btn = gr.Button("📥 Download Data Type Differences", visible=False)
                            datatype_download = gr.File(label="Download Data Type Differences", visible=False)

                    val_status = gr.Textbox(label="Status", interactive=False)
                    schema_download_btn = gr.Button("📥 Download Schema Validation Report", visible=False)
                    schema_download = gr.File(label="Download Schema Validation Report", visible=False)

        # ===== KPI VALIDATION SECTION =====
        with gr.Column(visible=False) as kpi_validation_section:
            with gr.Row():
                with gr.Column(scale=1):
                    gr.Markdown("### KPI Validation Configuration")
                    kpi_db = gr.Dropdown(label="Database")
                    kpi_source_schema = gr.Dropdown(label="Source Schema")
                    kpi_target_schema = gr.Dropdown(label="Target Schema")

                    # KPI Selection Checkboxes
                    with gr.Group():
                        gr.Markdown("### Select KPIs to Validate")
                        kpi_select_all = gr.Checkbox(label="Select All", value=True)
                        with gr.Row():
                            kpi_total_orders = gr.Checkbox(label="Total Orders", value=True)
                            kpi_total_revenue = gr.Checkbox(label="Total Revenue", value=True)
                            kpi_avg_order = gr.Checkbox(label="Average Order Value", value=True)
                        with gr.Row():
                            kpi_max_order = gr.Checkbox(label="Maximum Order Value", value=True)
                            kpi_min_order = gr.Checkbox(label="Minimum Order Value", value=True)
                            kpi_completed = gr.Checkbox(label="Completed Orders", value=True)
                        with gr.Row():
                            kpi_cancelled = gr.Checkbox(label="Cancelled Orders", value=True)
                            kpi_april_orders = gr.Checkbox(label="Orders in April 2025", value=True)
                            kpi_unique_customers = gr.Checkbox(label="Unique Customers", value=True)

                    kpi_validate_btn = gr.Button("Execute DriftWatch", variant="primary")

                with gr.Column(scale=2):
                    gr.Markdown("### ChangeLens / KPI Validation Report")
                    kpi_output = gr.Dataframe(
                        interactive=False,
                        wrap=True
                    )
                    kpi_status = gr.Textbox(label="Status", interactive=False)
                    kpi_download_btn = gr.Button("📥 Download KPI Validation Report", visible=False)
                    kpi_download = gr.File(label="Download KPI Validation Report", visible=False)

        # ===== TEST CASE VALIDATION SECTION =====
        with gr.Column(visible=False) as test_case_validation_section:
            with gr.Row():
                with gr.Column(scale=1):
                    gr.Markdown("### Test Automation")
                    tc_db = gr.Dropdown(label="Database")
                    tc_schema = gr.Dropdown(label="Schema")
                    tc_table = gr.Dropdown(
                        label="Catagory",
                        value="All"
                    )
            
                    with gr.Group():
                        gr.Markdown("### Select Test Cases")
                        tc_select_all = gr.Checkbox(
                            label="Select All", 
                            value=True,
                            info="Toggle all test cases for the selected table",
                        )        
                        tc_test_cases = gr.CheckboxGroup(
                            label="Available Test Cases",
                            choices=[],
                            interactive=True
                        )
            
                    tc_validate_btn = gr.Button("Execute DriftWatch", variant="primary")

                with gr.Column(scale=2):
                    gr.Markdown("### ChangeLens / Test Automation Report")                 
                    tc_output = gr.Dataframe(
                        interactive=False,
                        wrap=True
                    )
                    tc_status = gr.Textbox(label="Status", interactive=False)
                    tc_download_btn = gr.Button("📥 Download Test Report", visible=False)        
                    tc_download = gr.File(label="Download Test Report", visible=False)

    # Add custom CSS for styling
    app.css = """
    /* Base Styles */
    body {
        font-family: 'Segoe UI', Arial, sans-serif;
        background-color: #f9fafb;
        color: #2c3e50;
        margin: 0;
        padding: 0;
        min-height: 100%;
        display: flex;
        flex-direction: column;
    }
    /* ✅ Force font globally without altering color or layout */
    *, *::before, *::after {
        font-family: 'Segoe UI', Arial, sans-serif !important;
    }

    /* Headings */
    h1, h2, h3, h4 {
        font-weight: 600;
        color: #2c3e50;
        text-align: center;
    }

    /* Links */
    a {
        color: #2563eb;
        text-decoration: none;
    }
    a:hover {
        text-decoration: underline;
    }

    /* Labels */
    .label, span.svelte-g2oxp3 {
        display: inline-block;
        font-weight: 600;
        font-size: 14px;
        color: #2c3e50;
        margin-bottom: 4px;
        background-color: #e5e7eb;
        padding: 2px 6px;
        border-radius: 4px;
    }

    /* Input Fields */
    .input, .textbox, .dropdown, .snowflake-input {
        width: 100%;
        padding: 12px;
        margin-bottom: 15px;
        border: 1px solid #ccc;
        border-radius: 6px;
        font-size: 14px;
        box-sizing: border-box;
        background-color: white;
    }
    /* Compact Login Form */
    .snowflake-input {
        width: 100%;
        padding: 8px 12px;
        margin-bottom: 10px;
        border: 1px solid #ddd;
        border-radius: 4px;
        font-size: 14px;
    }

    .snowflake-button {
        padding: 8px 12px;
        margin: 5px;
        font-size: 14px;
    }

    /* Center the login form */
    .gradio-container .tab {
        display: flex;
        justify-content: center;
    }

    /* Make the form container more compact */
    .gr-form {
        max-width: 320px;
        padding: 15px;
    }

    /* Adjust heading size */
    h2 {
        font-size: 18px !important;
        margin-bottom: 15px !important;
    }

    .snowflake-input:focus {
        border-color: #2563eb;
        outline: none;
        box-shadow: 0 0 0 2px rgba(0, 102, 204, 0.1);
    }

    /* Buttons */
    .button, .snowflake-button {
        width: 100%;
        padding: 10px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 15px;
        border: none;
        cursor: pointer;
        transition: background 0.3s ease;
    }

    .snowflake-button.primary {
        background-color: #2563eb;
        color: white;
    }

    .snowflake-button.primary:hover {
        background-color: #1d4ed8;
    }

    .snowflake-button.secondary {
        background-color: white;
        color: #2563eb;
        border: 1px solid #2563eb;
    }

    /* Status Box */
    .status-box {
        background-color: #f8f9fa;
        padding: 12px;
        border-radius: 6px;
        font-size: 14px;
        margin-top: 10px;
        border-left: 4px solid #4CAF50;
        border: 1px solid #ddd;
    }

    .status-box.error {
        border-left-color: #f44336 !important;
    }

    /* Form Container */
    .gr-form {
        max-width: 420px;
        margin: 0 auto;
        padding: 20px;
        background: white;
        border-radius: 8px;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.06);
    }

    /* Tabs and Layouts */
    .tab, .gr-tabs {
        padding: 20px;
        margin-top: 20px;
    }

    .gr-group {
        margin-bottom: 20px;
    }

    /* Dataframe Styling */
    .gr-dataframe {
        max-height: 500px;
        overflow-y: auto;
    }

    /* Tab Selection Override */
    .selected.svelte-1tcem6n.svelte-1tcem6n {
        background-color: transparent;
        color: #2563eb !important;
        font-weight: 600;
        padding: 8px 12px;
        border-bottom: 2px solid #2563eb;
    }

    /* Button Overrides */
    .primary.svelte-1ixn6qd {
        border: var(--button-border-width) solid #2563eb;
        background: #2563eb;
        color: var(--button-primary-text-color);
        box-shadow: var(--button-primary-shadow);
    }

    button.svelte-1ixn6qd, a.svelte-1ixn6qd {
        display: inline-flex;
        justify-content: center;
        align-items: center;
        transition: var(--button-transition);
        padding: var(--size-0-5) var(--size-2);
        text-align: center;
    }

    /* Full Width Elements */
    div.svelte-vt1mxs>*, div.svelte-vt1mxs>.form>* {
        width: var(--size-full);
    }

    /* Gradio Button Defaults */
    .gradio-container button, .gradio-container [role=button] {
        cursor: pointer;
    }

    /* Box Model Fix */
    .gradio-container, .gradio-container *, .gradio-container :before, .gradio-container :after {
        box-sizing: border-box;
        border-width: 0;
        border-style: solid;
    }

    /* Theme Variables */
    :root {
        --name: default;
        --primary-500: #2563eb !important;
        --secondary-500: #3b82f6;
        --secondary-600: #2563eb;
        --neutral-100: #f4f4f5;
        --neutral-300: #d4d4d8;
        --neutral-700: #3f3f46;
        --color-accent-copied: #2563eb !important;
        --spacing-xxl: 16px;
        --radius-lg: 8px;
        --text-xs: 10px;
        --bg: white;
        --col: #27272a;
        --bg-dark: #0f0f11;
        --col-dark: #f4f4f5;
    }
    
    /* Test Case Checkbox Styling */
    .gr-checkbox-group .gr-checkbox-item {
        background-color: #f5f5f5;
        border-radius: 4px;
        padding: 8px 12px;
        margin: 4px 0;
    }
    
    .gr-checkbox-group .gr-checkbox-item.selected {
        background-color: #e0e0e0 !important;
        border-left: 3px solid #555 !important;
    }
    
    .gr-checkbox-group .gr-checkbox-item:hover {
        background-color: #ebebeb;
    }
    """

    # Hidden elements for dynamic updates
    login_success = gr.Checkbox(visible=False)

    # ===== EVENT HANDLERS =====
    
    # Function to toggle between validation types
    def toggle_validation_type(validation_type):
        if validation_type == "Schema Validation":
            return (
                gr.Column(visible=True),  # schema_validation_section
                gr.Column(visible=False),  # kpi_validation_section
                gr.Column(visible=False),  # test_case_validation_section
                "schema"  # validation_type state
            )
        elif validation_type == "KPI Validation":
            return (
                gr.Column(visible=False),  # schema_validation_section
                gr.Column(visible=True),  # kpi_validation_section
                gr.Column(visible=False),  # test_case_validation_section
                "kpi"  # validation_type state
            )
        else:
            return (
                gr.Column(visible=False),  # schema_validation_section
                gr.Column(visible=False),  # kpi_validation_section
                gr.Column(visible=True),  # test_case_validation_section
                "test_case"  # validation_type state
            )

    validation_type_dropdown.change(
        toggle_validation_type,
        inputs=validation_type_dropdown,
        outputs=[schema_validation_section, kpi_validation_section, test_case_validation_section, validation_type]
    )

    # Login/logout handlers
    def handle_login(user, password, account):
        conn, msg = get_snowflake_connection(user, password, account)
        success = conn is not None
        return (
            conn,  # conn_state
            msg,   # status
            success,  # login_success
            gr.Tab(visible=success),  # mirror_tab
            gr.Tab(visible=success),  # driftwatch_tab
            gr.Button(visible=success),  # disconnect_btn
            gr.Button(visible=not success),  # login_btn
            gr.Textbox(visible=True)  # status visibility
        )

    def handle_logout(conn):
        conn, msg = disconnect_snowflake(conn)
        return (
            conn,  # conn_state
            msg,   # status
            False,  # login_success
            gr.Tab(visible=False),  # mirror_tab
            gr.Tab(visible=False),  # driftwatch_tab
            gr.Button(visible=False),  # disconnect_btn
            gr.Button(visible=True),  # login_btn
            gr.Textbox(visible=True)  # status visibility
        )

    login_btn.click(
        handle_login,
        inputs=[user, password, account],
        outputs=[conn_state, status, login_success, mirror_tab, driftwatch_tab, disconnect_btn, login_btn, status]
    )

    disconnect_btn.click(
        handle_logout,
        inputs=[conn_state],
        outputs=[conn_state, status, login_success, mirror_tab, driftwatch_tab, disconnect_btn, login_btn, status]
    )

    # ===== MIRROR SCHEMA TAB FUNCTIONS =====
    # Update available schemas when database changes
    def update_schemas(conn, db, source_schema):
        if conn and db:
            schemas = get_schemas(conn, db)
            suggested_name = f"{source_schema}_CLONE" if source_schema else ""
            return (
                gr.Dropdown(choices=schemas, interactive=True),  # source_schema
                gr.Textbox(value=suggested_name, interactive=True)  # target_schema
            )
        return (
            gr.Dropdown(interactive=False),
            gr.Textbox(interactive=False)
        )

    # Initialize UI when connection is established
    def init_mirror_ui(conn):
        if conn:
            dbs = get_databases(conn)
            return (
                gr.Dropdown(choices=dbs, interactive=True),  # source_db
                gr.Dropdown(interactive=False),             # source_schema
            )
        return (
            gr.Dropdown(interactive=False),
            gr.Dropdown(interactive=False),
        )

    # Event handlers
    source_db.change(
        update_schemas,
        inputs=[conn_state, source_db, source_schema],
        outputs=[source_schema, target_schema]
    )

    source_schema.change(
        update_schemas,
        inputs=[conn_state, source_db, source_schema],
        outputs=[source_schema, target_schema]
    )

    login_success.change(
        init_mirror_ui,
        inputs=[conn_state],
        outputs=[source_db, source_schema]
    )

    # Clone execution
    def execute_clone(conn, source_db, source_schema, target_schema):
        if not target_schema:
            return "❌ Please enter a target schema name"

        success, message, df = clone_schema(
            conn, source_db, source_schema, target_schema
        )

        return message

    clone_btn.click(
        execute_clone,
        inputs=[conn_state, source_db, source_schema, target_schema],
        outputs=[clone_output]
    )

    # ===== SCHEMA VALIDATION FUNCTIONS =====
    # Dynamic updates for validation tab
    def update_val_schemas(conn, db):
        if conn and db:
            schemas = get_schemas(conn, db)
            return (
                gr.Dropdown(choices=schemas),  # val_source_schema
                gr.Dropdown(choices=schemas)   # val_target_schema
            )
        return gr.Dropdown(), gr.Dropdown()

    def init_validation_ui(conn):
        if conn:
            dbs = get_databases(conn)
            return (
                gr.Dropdown(choices=dbs),  # val_db
                gr.Dropdown(),             # val_source_schema
                gr.Dropdown(),             # val_target_schema
            )
        return (
            gr.Dropdown(),
            gr.Dropdown(),
            gr.Dropdown(),
        )

    # Event handlers
    val_db.change(
        update_val_schemas,
        inputs=[conn_state, val_db],
        outputs=[val_source_schema, val_target_schema]
    )

    login_success.change(
        init_validation_ui,
        inputs=[conn_state],
        outputs=[val_db, val_source_schema, val_target_schema]
    )

    def run_validation(conn, db, source_schema, target_schema):
        try:
            # Compare tables
            table_diff = compare_table_differences(conn, db, source_schema, target_schema)

            # Compare columns and data types
            column_diff, datatype_diff = compare_column_differences(conn, db, source_schema, target_schema)

            # Combine all results into one DataFrame for download
            combined_df = pd.concat([
                table_diff.assign(Validation_Type="Table Differences"),
                column_diff.assign(Validation_Type="Column Differences"),
                datatype_diff.assign(Validation_Type="Data Type Differences")
            ])

            return (
                table_diff,
                column_diff,
                datatype_diff,
                "✅ Validation completed successfully!",
                gr.Button(visible=not table_diff.empty),
                gr.Button(visible=not column_diff.empty),
                gr.Button(visible=not datatype_diff.empty),
                combined_df,
                gr.Button(visible=not combined_df.empty)
            )
        except Exception as e:
            return (
                pd.DataFrame(),
                pd.DataFrame(),
                pd.DataFrame(),
                f"❌ Validation failed: {str(e)}",
                gr.Button(visible=False),
                gr.Button(visible=False),
                gr.Button(visible=False),
                pd.DataFrame(),
                gr.Button(visible=False)
            )

    validate_btn.click(
        run_validation,
        inputs=[conn_state, val_db, val_source_schema, val_target_schema],
        outputs=[
            table_diff_output,
            column_diff_output,
            datatype_diff_output,
            val_status,
            table_download_btn,
            column_download_btn,
            datatype_download_btn,
            combined_report_state,
            schema_download_btn
        ]
    )

    # Download handlers for individual reports
    def download_table_report(df):
        if df.empty:
            return None, gr.File(visible=False)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Table_Differences_{timestamp}.csv"
        df.to_csv(filename, index=False)
        return filename, gr.File(value=filename, visible=True, label="Download Table Differences")

    def download_column_report(df):
        if df.empty:
            return None, gr.File(visible=False)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Column_Differences_{timestamp}.csv"
        df.to_csv(filename, index=False)
        return filename, gr.File(value=filename, visible=True, label="Download Column Differences")

    def download_datatype_report(df):
        if df.empty:
            return None, gr.File(visible=False)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Datatype_Differences_{timestamp}.csv"
        df.to_csv(filename, index=False)
        return filename, gr.File(value=filename, visible=True, label="Download Data Type Differences")

    def download_schema_report(combined_df):
        if combined_df.empty:
            return None, gr.File(visible=False)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Schema_Validation_Report_{timestamp}.csv"
        combined_df.to_csv(filename, index=False)
        return filename, gr.File(value=filename, visible=True, label="Download Schema Validation Report")

    table_download_btn.click(
        download_table_report,
        inputs=table_diff_output,
        outputs=[table_download, table_download]
    )

    column_download_btn.click(
        download_column_report,
        inputs=column_diff_output,
        outputs=[column_download, column_download]
    )

    datatype_download_btn.click(
        download_datatype_report,
        inputs=datatype_diff_output,
        outputs=[datatype_download, datatype_download]
    )

    schema_download_btn.click(
        download_schema_report,
        inputs=combined_report_state,
        outputs=[schema_download, schema_download]
    )

    # ===== KPI VALIDATION FUNCTIONS =====
    # Dynamic updates for KPI tab
    def update_kpi_schemas(conn, db):
        if conn and db:
            schemas = get_schemas(conn, db)
            return (
                gr.Dropdown(choices=schemas),  # kpi_source_schema
                gr.Dropdown(choices=schemas)   # kpi_target_schema
            )
        return gr.Dropdown(), gr.Dropdown()

    def init_kpi_ui(conn):
        if conn:
            dbs = get_databases(conn)
            return (
                gr.Dropdown(choices=dbs),  # kpi_db
                gr.Dropdown(),             # kpi_source_schema
                gr.Dropdown(),             # kpi_target_schema
            )
        return (
            gr.Dropdown(),
            gr.Dropdown(),
            gr.Dropdown(),
        )

    # Event handlers
    kpi_db.change(
        update_kpi_schemas,
        inputs=[conn_state, kpi_db],
        outputs=[kpi_source_schema, kpi_target_schema]
    )

    login_success.change(
        init_kpi_ui,
        inputs=[conn_state],
        outputs=[kpi_db, kpi_source_schema, kpi_target_schema]
    )

    # Select All checkbox functionality
    def toggle_all_kpis(select_all):
        return (
            gr.Checkbox(value=select_all),  # total_orders
            gr.Checkbox(value=select_all),   # total_revenue
            gr.Checkbox(value=select_all),  # avg_order
            gr.Checkbox(value=select_all),   # max_order
            gr.Checkbox(value=select_all),   # min_order
            gr.Checkbox(value=select_all),  # completed
            gr.Checkbox(value=select_all),   # cancelled
            gr.Checkbox(value=select_all),   # april_orders
            gr.Checkbox(value=select_all)    # unique_customers
        )

    kpi_select_all.change(
        toggle_all_kpis,
        inputs=kpi_select_all,
        outputs=[
            kpi_total_orders, kpi_total_revenue, kpi_avg_order,
            kpi_max_order, kpi_min_order, kpi_completed,
            kpi_cancelled, kpi_april_orders, kpi_unique_customers
        ]
    )

    # Enhanced KPI validation function
    def validate_selected_kpis(conn, database, source_schema, target_schema, *kpi_selections):
        """Validate selected KPIs between source and clone schemas"""
        cursor = conn.cursor()
        results = []

        # KPI names mapping to IDs (adjust based on your actual KPI table)
        kpi_mapping = {
            "Total Orders": 1,
            "Total Revenue": 2,
            "Average Order Value": 3,
            "Max Order Value": 4,
            "Min Order Value": 5,
            "Completed Orders": 6,
            "Cancelled Orders": 7,
            "Orders in April 2025": 8,
            "Unique Customers": 9
        }

        # Get selected KPI IDs
        selected_kpis = [kpi_name for kpi_name, selected in zip(kpi_mapping.keys(), kpi_selections) if selected]

        if not selected_kpis:
            return pd.DataFrame(), "⚠️ No KPIs selected for validation", gr.Button(visible=False)

        try:
            # Fetch selected KPI definitions
            kpi_query = f"""
            SELECT KPI_ID, KPI_NAME, KPI_VALUE
            FROM {database}.{source_schema}.ORDER_KPIS
            WHERE KPI_NAME IN ({','.join([f"'{kpi}'" for kpi in selected_kpis])})
            """
            cursor.execute(kpi_query)
            kpis = cursor.fetchall()

            if not kpis:
                return pd.DataFrame(), "⚠️ No matching KPIs found in ORDER_KPIS table", gr.Button(visible=False)

            # First verify both schemas have the ORDER_DATA table
            try:
                cursor.execute(f"SELECT 1 FROM {database}.{source_schema}.ORDER_DATA LIMIT 1")
                source_has_table = True
            except:
                source_has_table = False

            try:
                cursor.execute(f"SELECT 1 FROM {database}.{target_schema}.ORDER_DATA LIMIT 1")
                target_has_table = True
            except:
                target_has_table = False

            if not source_has_table or not target_has_table:
                error_msg = "ORDER_DATA table missing in "
                if not source_has_table and not target_has_table:
                    error_msg += "both schemas"
                elif not source_has_table:
                    error_msg += "source schema"
                else:
                    error_msg += "target schema"

                for kpi_id, kpi_name, kpi_sql in kpis:
                    results.append({
                        'KPI ID': kpi_id,
                        'KPI Name': kpi_name,
                        'Source Value': f"ERROR: {error_msg}",
                        'Clone Value': f"ERROR: {error_msg}",
                        'Difference': "N/A",
                        'Diff %': "N/A",
                        'Status': "❌ Error"
                    })
                return pd.DataFrame(results), "❌ Validation failed - missing ORDER_DATA table", gr.Button(visible=False)

            for kpi_id, kpi_name, kpi_sql in kpis:
                try:
                    # More robust replacement that handles word boundaries and case
                    source_query = re.sub(r'\bORDER_DATA\b', f'{database}.{source_schema}.ORDER_DATA', kpi_sql, flags=re.IGNORECASE)
                    cursor.execute(source_query)
                    result_source = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                except Exception as e:
                    result_source = f"QUERY_ERROR: {str(e)}"

                try:
                    clone_query = re.sub(r'\bORDER_DATA\b', f'{database}.{target_schema}.ORDER_DATA', kpi_sql, flags=re.IGNORECASE)
                    cursor.execute(clone_query)
                    result_clone = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                except Exception as e:
                    result_clone = f"QUERY_ERROR: {str(e)}"

                # Calculate differences if possible
                diff = "N/A"
                pct_diff = "N/A"
                status = "⚠️ Mismatch"

                try:
                    if (isinstance(result_source, (int, float)) and isinstance(result_clone, (int, float))):
                        diff = float(result_source) - float(result_clone)
                        pct_diff = (diff / float(result_source)) * 100 if float(result_source) != 0 else float('inf')
                        status = '✅ Match' if diff == 0 else '⚠️ Mismatch'
                    elif str(result_source) == str(result_clone):
                        status = '✅ Match'
                except:
                    pass

                results.append({
                    'KPI ID': kpi_id,
                    'KPI Name': kpi_name,
                    'Source Value': result_source,
                    'Clone Value': result_clone,
                    'Difference': diff if not isinstance(diff, float) else round(diff, 2),
                    'Diff %': f"{round(pct_diff, 2)}%" if isinstance(pct_diff, float) else pct_diff,
                    'Status': status
                })

            df = pd.DataFrame(results)
            return df, "✅ KPI validation completed", gr.Button(visible=True)

        except Exception as e:
            return pd.DataFrame(), f"❌ KPI validation failed: {str(e)}", gr.Button(visible=False)

    kpi_validate_btn.click(
        validate_selected_kpis,
        inputs=[
            conn_state, kpi_db, kpi_source_schema, kpi_target_schema,
            kpi_total_orders, kpi_total_revenue, kpi_avg_order,
            kpi_max_order, kpi_min_order, kpi_completed,
            kpi_cancelled, kpi_april_orders, kpi_unique_customers
        ],
        outputs=[kpi_output, kpi_status, kpi_download_btn]
    )

    # Download handler
    def download_kpi_report(df):
        if df.empty:
            return None, gr.File(visible=False)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"KPI_Validation_Report_{timestamp}.csv"
        df.to_csv(filename, index=False)
        return filename, gr.File(value=filename, visible=True, label="Download KPI Validation Report")

    kpi_download_btn.click(
        download_kpi_report,
        inputs=kpi_output,
        outputs=[kpi_download, kpi_download]
    )

    # ===== TEST CASE VALIDATION FUNCTIONS =====
    # Test Case Validation UI Initialization
    def init_test_case_ui(conn):
        if conn:
            dbs = get_databases(conn)
            return (
                gr.Dropdown(choices=dbs),  # tc_db
                gr.Dropdown(),             # tc_schema
                gr.Dropdown(choices=["All"], value="All"),  # tc_table
            )
        return (
            gr.Dropdown(),
            gr.Dropdown(),
            gr.Dropdown(choices=["All"], value="All"),
        )

    login_success.change(
        init_test_case_ui,
        inputs=[conn_state],
        outputs=[tc_db, tc_schema, tc_table]
    )

    # Update schemas when database changes
    def update_tc_schemas(conn, db):
        if conn and db:
            schemas = get_schemas(conn, db)
            return gr.Dropdown(choices=schemas)  # tc_schema
        return gr.Dropdown()

    tc_db.change(
        update_tc_schemas,
        inputs=[conn_state, tc_db],
        outputs=tc_schema
    )

    # Update test cases when schema or table changes
    def update_test_case_components(conn, db, schema, table="All", select_all=False):
        """Update all test case components together"""
        print(f"Updating components for: {db}.{schema}.{table}")
        
        if not (conn and db and schema):
            print("Missing required parameters")
            return (
                gr.Dropdown(choices=["All"], value="All"),  # tc_table
                gr.CheckboxGroup(choices=[]),               # tc_test_cases
                [],                                         # test_case_data
                gr.Checkbox()                               # tc_select_all
            )

        try:
            # First get available tables
            tables = get_test_case_tables(conn, db, schema)
            print(f"Available tables: {tables}")
            
            # Then get test cases for selected table
            test_cases = get_test_cases(conn, db, schema, table)
            print(f"Found {len(test_cases)} test cases")
            
            # Format choices for checkbox group (without ID)
            choices = [f"{case[1]}" for case in test_cases]
            
            # If 'All' is selected, auto-select all test cases
            if table == 'All':
                select_all = True
                
            return (
                gr.Dropdown(choices=tables, value=table),  # tc_table
                gr.CheckboxGroup(
                    choices=choices,
                    value=choices if select_all else [],
                    label=f"Available Test Cases"
                ),
                test_cases,  # Store raw test case data
                gr.Checkbox(value=select_all, interactive=len(choices) > 0)
            )
            
        except Exception as e:
            print(f"Component update error: {str(e)}")
            return (
                gr.Dropdown(choices=["All"], value="All"),
                gr.CheckboxGroup(choices=[], label="Available Test Cases"),
                [],
                gr.Checkbox()
            )

    tc_schema.change(
        lambda conn, db, schema, table, select_all: update_test_case_components(conn, db, schema, table, select_all),
        inputs=[conn_state, tc_db, tc_schema, tc_table, tc_select_all],
        outputs=[tc_table, tc_test_cases, test_case_data, tc_select_all]
    )

    tc_table.change(
        lambda conn, db, schema, table, select_all: update_test_case_components(conn, db, schema, table, select_all),
        inputs=[conn_state, tc_db, tc_schema, tc_table, tc_select_all],
        outputs=[tc_table, tc_test_cases, test_case_data, tc_select_all]
    )

    # Toggle all test cases
    def toggle_all_test_cases(select_all, test_case_choices, test_case_data):
        """Toggle all test cases selection"""
        # Get the display names (without IDs)
        all_choices = [f"{case[1]}" for case in test_case_data]
        return (
            gr.CheckboxGroup(value=all_choices if select_all else []),
            select_all
        )

    tc_select_all.change(
        toggle_all_test_cases,
        inputs=[tc_select_all, tc_test_cases, test_case_data],
        outputs=[tc_test_cases, tc_select_all]
    )

    # Execute test case validation
    def execute_test_case_validation(conn, db, schema, selected_case_names, test_case_data):
        if not conn or not db or not schema:
            return pd.DataFrame(), "❌ Please select database and schema", gr.Button(visible=False)
        
        if not selected_case_names:
            return pd.DataFrame(), "⚠️ Please select at least one test case", gr.Button(visible=False)
        
        # Get selected test cases based on names (since we removed IDs from display)
        selected_test_cases = []
        for name in selected_case_names:
            # Find the matching test case in our stored data
            for case in test_case_data:
                if case[1] == name:  # Match on TEST_ABBREVIATION
                    selected_test_cases.append(case)
                    break
        
        if not selected_test_cases:
            return pd.DataFrame(), "⚠️ No valid test cases selected", gr.Button(visible=False)
        
        return validate_test_cases(conn, db, schema, selected_test_cases)

    tc_validate_btn.click(
        execute_test_case_validation,
        inputs=[conn_state, tc_db, tc_schema, tc_test_cases, test_case_data],
        outputs=[tc_output, tc_status, tc_download_btn]
    )

    # Download test case report with description
    def download_test_case_report(df, test_case_data, selected_case_names):
        if df.empty:
            return None, gr.File(visible=False)

        # Create a mapping of test abbreviations to descriptions
        desc_map = {case[1]: case[3] for case in test_case_data}
        
        # Add description column to the DataFrame
        df['DESCRIPTION'] = df['TEST CASE'].map(desc_map)
        
        # Reorder columns to put description early
        cols = ['TEST CASE', 'DESCRIPTION', 'CATEGORY', 'EXPECTED RESULT', 'ACTUAL RESULT', 'STATUS']
        df = df[cols]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Test_Case_Validation_Report_{timestamp}.csv"
        df.to_csv(filename, index=False)
        return filename, gr.File(value=filename, visible=True, label="Download Test Report")

    tc_download_btn.click(
        download_test_case_report,
        inputs=[tc_output, test_case_data, tc_test_cases],
        outputs=[tc_download, tc_download]
    )

# Launch the app
if __name__ == "__main__":
    try:
        from google.colab import output
        output.enable_custom_widget_manager()
        app.launch(share=True, inbrowser=True)
    except:
        # Fallback for non-Colab environments
        app.launch(debug=True, share=True)
# # Launch the app
# if __name__ == "__main__":
#     try:
#         from google.colab import output
#         output.enable_custom_widget_manager()
#         app.launch(server_name="0.0.0.0", server_port=7860,share=False, prevent_thread_lock=True)
#     except:
#         # Fallback for non-Colab environments
#         app.launch(server_name="0.0.0.0", server_port=7860,share=False, prevent_thread_lock=True)