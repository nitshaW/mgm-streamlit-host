-- to be deployed as a Streamlit App with: snowsql -c demo_conn -f deploy.sql
-- CREATE OR REPLACE DATABASE transaction_streamlit;

-- CREATE STAGE mystage;
use schema SALES_ANALYTICS.PUBLIC;

create or replace stage mgm_streamlit_stage;

PUT file:///Users/nitshawacinski/Desktop/mgm-streamlit-snowflake/Main.py @mgm_streamlit_stage overwrite=true auto_compress=false;
PUT file:///Users/nitshawacinski/Desktop/mgm-streamlit-snowflake/pages/*.py @mgm_streamlit_stage/pages overwrite=true auto_compress=false;

CREATE OR REPLACE STREAMLIT mgm_analytics
    ROOT_LOCATION = '@SALES_ANALYTICS.public.mgm_streamlit_stage'
    MAIN_FILE = '/Main.py'
    QUERY_WAREHOUSE = "DEV_XS";
