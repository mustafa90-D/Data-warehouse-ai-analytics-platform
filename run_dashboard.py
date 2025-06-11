import subprocess
import sys
import os

def run_dashboard():
    """Run the Streamlit dashboard"""
    try:
        # Make sure we're in the project directory
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        
        # Run streamlit
        subprocess.run([sys.executable, "-m", "streamlit", "run", "dashboard.py"])
        
    except Exception as e:
        print(f"Error running dashboard: {e}")

if __name__ == "__main__":
    run_dashboard()