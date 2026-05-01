import streamlit as st
import subprocess
import datetime
import os
import sys

# Modern UI Configuration
st.set_page_config(page_title="Meta Ads Updater", page_icon="🚀", layout="centered")

st.title("📊 Meta Ads Data Updater")
st.markdown("Manually trigger the Meta Ads to Google Sheets pipeline. Select your custom date range below.")

with st.form("updater_form"):
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.date(2026, 2, 28))
    with col2:
        end_date = st.date_input("End Date", value=datetime.date.today())
        
    submit_button = st.form_submit_button(label="🚀 Run Pipeline", use_container_width=True)

if submit_button:
    if start_date > end_date:
        st.error("Start Date must be before or equal to End Date.")
    else:
        st.info(f"Starting pipeline from **{start_date}** to **{end_date}**... This may take several minutes.")
        
        # Setup environment variables for the subprocess
        env = os.environ.copy()
        env["BACKFILL_START_DATE"] = start_date.strftime("%Y-%m-%d")
        env["BACKFILL_END_DATE"] = end_date.strftime("%Y-%m-%d")
        
        script_path = os.path.join(os.path.dirname(__file__), "combine_pipeline_data.py")
        
        # Start the pipeline script
        process = subprocess.Popen(
            [sys.executable, script_path],
            env=env,
            cwd=os.path.dirname(__file__),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1 # Line buffered
        )
        
        full_logs = []
        with st.container():
            st.markdown("### 🖥️ Live Pipeline Logs")
            st.markdown("*Showing the last 50 lines to keep the browser responsive.*")
            log_area = st.empty()
            
            # Read stdout line by line in real-time
            for line in iter(process.stdout.readline, ''):
                full_logs.append(line.strip())
                display_logs = "\n".join(full_logs[-50:])
                log_area.code(display_logs, language="log")
                
            process.stdout.close()
            return_code = process.wait()
            
            if return_code == 0:
                st.success("✅ Pipeline completed successfully! Data is now updated.")
                st.balloons()
            else:
                st.error(f"❌ Pipeline failed with exit code {return_code}. Check the logs above.")
