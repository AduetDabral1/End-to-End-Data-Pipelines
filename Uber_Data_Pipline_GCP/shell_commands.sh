# Install Python and pip 

sudo apt-get update

sudo apt-get install python3-distutils

sudo apt-get install python3-apt

sudo apt-get install wget

wget https://bootstrap.pypa.io/get-pip.py

sudo apt update

sudo apt install python3-pip -y

# Create a virtual environment

sudo apt install -y python3-venv python3-full

python3 -m venv mage-env

source mage-env/bin/activate

pip install --upgrade pip

# Install Pandas

pip install pandas

# Install Mage

pip install mage-ai
mage --version

mage start uber_data_pipeline


# Install Google Cloud Library
pip install google-cloud
pip install google-cloud-bigquery
