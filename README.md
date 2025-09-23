# 🏥 ETL Claims – LaSante  

This project implements an **ETL pipeline** in Python to process medical claim data from SQL Server.  

The pipeline includes:  
- **Extract** → retrieves base tables from SQL Server  
- **Transform** → cleans and combines into intermediate datasets (`invoice`, `claiminsurance`, `payments`, `wrap`)  
- **Load** → exports the final dataset (`claims`) into CSV files (chunks of 2000 rows)  

## 📂 Project structure  

ETL_Claims/  
│  
├── config.py                     # SQL Server connection parameters  
│  
├── extract/  
│   └── extract_data.py            # Extraction logic for source tables  
│  
├── transform/  
│   ├── transform_invoice.py       # Build invoice dataset  
│   ├── transform_claiminsurance.py # Clean ClaimInsurance dataset  
│   ├── transform_payments.py      # Classify and pivot payments  
│   ├── transform_wrap.py          # Classify wrap insurances  
│   └── transform_claims.py        # Final claims dataset  
│  
├── load/  
│   └── load_data.py               # Export CSV in 2000-row chunks  
│  
└── main.py                        # ETL orchestrator  

## ⚙️ Configuration  

- **Database connection** → set credentials inside `config.py`:  
```python
DB_SERVER = "your-server"
DB_DATABASE = "your-database"
DB_USER = "your-username"
DB_PASS = "your-password"

