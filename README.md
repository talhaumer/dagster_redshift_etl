# 🚀 Dagster Redshift ETL Pipeline

Welcome to the **Dagster Redshift ETL Pipeline** project!  
This repository provides a robust, production-ready data pipeline for extracting data from MySQL, transforming it with Python, and loading it into Amazon Redshift.  
It also includes a modular framework for integrating other data sources, such as CurrencyLayer, and is designed for clarity, maintainability, and scalability.

---

## 🌟 Features

- **Modern ETL with [Dagster](https://dagster.io/):** Orchestrate, schedule, and monitor your data workflows.
- **MySQL → Redshift:** Extract data from MySQL, transform with pandas, and load efficiently into Redshift.
- **SQL Query Management:** Store and version your SQL queries in a dedicated `queries/` folder.
- **Robust Logging:** Centralized, configurable logging for all pipeline steps.
- **Configurable Schedules:** Run your jobs on any schedule with easy configuration.
- **Extensible:** Add new data sources or destinations with minimal code changes.
- **Best Practices:** Modular code, docstrings, error handling, and environment variable support.

---

## 🗂️ Project Structure

```
dagster_redshift_etl/
│
├── dags/                  # Dagster pipeline definitions (ops/jobs)
│   └── user_etl_pipeline.py
├── jobs/                  # Dagster job definitions
│   └── mysql_to_redshift_job.py
├── resources/             # Data handlers and utilities
│   ├── mysql_utils.py
│   └── redshift_handler.py
├── schedules/             # Dagster schedule definitions
│   └── mysql_to_redshift_schedule.py
├── queries/               # SQL query files
│   └── daily_users.sql
├── utils/                 # Environment and query loaders, logging, etc.
│   ├── env_loader.py
│   └── query_loader.py
├── test/                  # Unit and integration tests
│   ├── test_mysql_utils.py
│   └── test_user_etl_pipeline.py
├── repository.py          # Dagster Definitions (jobs, schedules)
├── requirements.txt       # Python dependencies
└── README.md              # This file!
```

---

## ⚡ Quickstart

### 1. **Clone the Repository**

```sh
git clone https://github.com/yourusername/dagster_redshift_etl.git
cd dagster_redshift_etl
```

### 2. **Set Up Your Environment**

We recommend using [virtualenv](https://virtualenv.pypa.io/) or [venv](https://docs.python.org/3/library/venv.html):

```sh
python3 -m venv venv
source venv/bin/activate
```

### 3. **Install Dependencies**

```sh
pip install -r requirements.txt
```

### 4. **Configure Environment Variables**

Create a `.env` file in the project root (or set these in your environment):

```
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password
MYSQL_HOST=your_mysql_host
MYSQL_PORT=3306
MYSQL_DATABASE=your_mysql_db

REDSHIFT_USER=your_redshift_user
REDSHIFT_PASSWORD=your_redshift_password
REDSHIFT_HOST=your_redshift_host
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=your_redshift_db
REDSHIFT_SCHEMA=public
REDSHIFT_S3_BUCKET=your_s3_bucket
REDSHIFT_AWS_IAM_ROLE=your_redshift_iam_role
REDSHIFT_AWS_REGION=your_aws_region

S3_KEY=your_aws_access_key
S3_SECRET=your_aws_secret_key
```

### 5. **Set Up Your Queries**

Edit or add SQL files in the `queries/` folder.  
For example, `queries/daily_users.sql`:

```sql
SELECT * FROM accounts_python;
```

### 6. **Run Dagster**

Start the Dagster web UI:

```sh
dagster dev
```

Visit [http://localhost:3000](http://localhost:3000) to explore your jobs and schedules.

---

## 🛠️ Usage

- **Run a job manually:**  
  Use the Dagster UI or CLI:
  ```sh
  dagster job execute -f jobs/mysql_to_redshift_job.py -j mysql_to_redshift_job
  ```

- **Schedule jobs:**  
  Schedules are defined in `schedules/`.  
  Customize cron schedules as needed.

- **Add new queries:**  
  Place new `.sql` files in `queries/` and reference them in your schedule or job config.

---

## 🧩 Extending the Pipeline

- **Add a new data source:**  
  Create a new op in `dags/`, add a handler in `resources/`, and wire it into a job.
- **Add a new destination:**  
  Implement a handler in `resources/` and add a load op.
- **Add a new schedule:**  
  Define it in `schedules/` and register in `repository.py`.

---

## 🤖 Example: CurrencyLayer Integration

Want to fetch and store currency exchange rates?  
Check out the `currencylayer_etl_pipeline.py` example in `dags/` and the corresponding schedule in `schedules/`.

---

## 📝 Best Practices

- Use docstrings and comments for all functions and classes.
- Store secrets in environment variables or a secure vault.
- Keep SQL logic in the `queries/` folder for easy versioning.
- Use logging for all ETL steps and errors.

---

## ❤️ Contributing

Pull requests are welcome!  
Please open an issue first to discuss your ideas or report bugs.

---

## 👤 Maintainers

- [Talha Umer](https://github.com/talhaumer)
---

## 📄 License

This project is licensed under the MIT License.

---

## 🙏 Acknowledgements

- [Dagster](https://dagster.io/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [Pandas](https://pandas.pydata.org/)
- [Amazon Redshift](https://aws.amazon.com/redshift/)
- [CurrencyLayer](https://currencylayer.com/)

---

## 🎸 Humatone

> “Data is the new oil, but pipelines are the new refineries.  
> Build them with care, monitor them with love, and your insights will flow.”

---

Happy ETL-ing