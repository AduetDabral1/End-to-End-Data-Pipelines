# ==========================================
# 1. Connect to the EC2 Machine
# ==========================================
ssh -i "airflow_key_pair.pem" ubuntu@<public_dns>.eu-north-1.compute.amazonaws.com

# ==========================================
# 2. Install Python and Create Virtual Env
# ==========================================
# Update OS packages and install the full Python distribution
sudo apt update
sudo apt install python3-full -y

# Create a virtual environment named 'airflow_env' in the home directory
# (This isolates our packages to protect Ubuntu's system stability)
python3 -m venv ~/airflow_env

# Activate the virtual environment (prompt will change to show `airflow_env`)
source ~/airflow_env/bin/activate

# ==========================================
# 3. Install Required Libraries
# ==========================================
# Safely install dependencies inside the activated environment
pip install apache-airflow pandas s3fs tweepy

# ==========================================
# 4. Initialize Airflow
# ==========================================
# Launch the Airflow server and related processes
airflow standalone

# NOTE: If there is an error retrieving the username/password in Airflow 3+, run:
airflow fab-db migrate
cat ~/airflow/simple_auth_manager_passwords.json.generated

# ==========================================
# 5. Open the AWS Security Group (Firewall)
# ==========================================
# BEFORE accessing the UI, we must allow web traffic to the EC2 instance.
# 1. Go to the AWS EC2 Console -> Instances -> Select your instance.
# 2. Click the 'Security' tab -> Click the Security Group.
# 3. Click 'Edit inbound rules' -> Add a new rule:
#    - Type: Custom TCP
#    - Port Range: 8080
#    - Source: 0.0.0.0/0 (or your specific IP)
# 4. Save rules.

# ==========================================
# 6. Access the UI
# ==========================================
# Navigate to http://<EC2_public_DNS>:8080 in your browser and log in.

# ==========================================
# 7. Configure Airflow and Create DAGs
# ==========================================
# Open a new terminal tab (or stop the server with Ctrl+C) and ensure venv is active
cd ~/airflow

# Edit the config to point to the new 'x_dags' folder
nano airflow.cfg  

# Create the new DAG directory and navigate into it
mkdir x_dags
cd x_dags

# Create the ETL script
nano x_etl.py  
# (Paste code -> Press Ctrl+X -> Press Y -> Press Enter to save)

# Create the DAG file
nano x_dag.py  
# (Paste code -> Press Ctrl+X -> Press Y -> Press Enter to save)

# ==========================================
# 8. AWS S3 IAM Configuration
# ==========================================
# 1. Create an S3 Bucket matching the name in your x_etl.py script.
# 2. Create an IAM Role with a strict `s3:PutObject` policy and attach it to the EC2.
# 3. In EC2 Instance Settings: Ensure IMDSv2 is set to "Optional" so the code can read the keys.

# ==========================================
# 9. Apply Changes
# ==========================================
# To apply new DAGs or code changes, restart the Airflow server:
# Press Ctrl+C in the terminal running Airflow, then start it again:
airflow standalone