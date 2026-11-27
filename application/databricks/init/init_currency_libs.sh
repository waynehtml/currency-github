pip config set global.trusted-host "fcp4lnexuap01.bocad.bank-banque-canada.ca"
pip config set global.index "https://fcp4lnexuap01.bocad.bank-banque-canada.ca/nexus/repository/pypi-all/"
pip config set global.index_url "https://fcp4lnexuap01.bocad.bank-banque-canada.ca/nexus/repository/pypi-all/simple"

pip install azure-mgmt-datafactory
pip install azureml-opendatasets
pip install pandas
pip install openpyxl