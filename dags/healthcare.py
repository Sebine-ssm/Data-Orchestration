from __future__ import annotations
from airflow import DAG
from airflow.sdk import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import csv, os, shutil
from faker import Faker
import pandas as pd
import matplotlib.pyplot as plt
from psycopg2 import Error as DatabaseError
from airflow.providers.postgres.hooks.postgres import PostgresHook

OUTPUT_DIR = "/opt/airflow/data"
TARGET_TABLE = "healthcare_records"

with DAG(
    dag_id="healthcare_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "IDS706", "retries": 1, "retry_delay": timedelta(minutes=2)},
) as dag:

    # --- ETL TaskGroup ---
    with TaskGroup(
        group_id="etl_group", tooltip="Ingest, transform, merge, load"
    ) as etl_group:

        @task()
        def fetch_patients(output_dir: str = OUTPUT_DIR, quantity=50) -> str:
            fake = Faker()
            data = []
            for _ in range(quantity):
                data.append(
                    {
                        "patient_id": fake.uuid4(),
                        "first_name": fake.first_name(),
                        "last_name": fake.last_name(),
                        "date_of_birth": fake.date_of_birth(
                            minimum_age=18, maximum_age=90
                        ).strftime("%Y-%m-%d"),
                        "gender": fake.random_element(["Male", "Female", "Other"]),
                        "blood_type": fake.random_element(
                            ["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"]
                        ),
                        "email": fake.email(),
                        "phone": fake.phone_number(),
                        "address": fake.address().replace("\n", ", "),
                    }
                )
            path = os.path.join(output_dir, "patients.csv")
            os.makedirs(output_dir, exist_ok=True)
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            return path

        @task()
        def fetch_appointments(output_dir: str = OUTPUT_DIR, quantity=50) -> str:
            fake = Faker()
            departments = [
                "Cardiology",
                "Neurology",
                "Pediatrics",
                "Orthopedics",
                "Dermatology",
            ]
            data = []
            for _ in range(quantity):
                data.append(
                    {
                        "appointment_id": fake.uuid4(),
                        "doctor_name": "Dr. " + fake.name(),
                        "department": fake.random_element(departments),
                        "appointment_date": fake.date_between(
                            start_date="-30d", end_date="+60d"
                        ).strftime("%Y-%m-%d"),
                        "appointment_time": fake.time(pattern="%H:%M"),
                        "duration_minutes": fake.random_element([15, 30, 45, 60]),
                        "status": fake.random_element(
                            ["scheduled", "completed", "cancelled", "no-show"]
                        ),
                        "consultation_fee": round(fake.random.uniform(50.0, 300.0), 2),
                    }
                )
            path = os.path.join(output_dir, "appointments.csv")
            os.makedirs(output_dir, exist_ok=True)
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            return path

        @task()
        def merge_csvs(
            patients_path: str, appointments_path: str, output_dir: str = OUTPUT_DIR
        ) -> str:
            merged_path = os.path.join(output_dir, "merged_data.csv")
            with open(patients_path, encoding="utf-8") as f1, open(
                appointments_path, encoding="utf-8"
            ) as f2:
                patients = list(csv.DictReader(f1))
                appointments = list(csv.DictReader(f2))
            merged = []
            for i in range(min(len(patients), len(appointments))):
                p, a = patients[i], appointments[i]
                merged.append(
                    {
                        "patient_name": f"{p['first_name']} {p['last_name']}",
                        "patient_email": p["email"],
                        "blood_type": p["blood_type"],
                        "doctor_name": a["doctor_name"],
                        "department": a["department"],
                        "appointment_date": a["appointment_date"],
                        "status": a["status"],
                        "consultation_fee": a["consultation_fee"],
                    }
                )
            with open(merged_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=merged[0].keys())
                writer.writeheader()
                writer.writerows(merged)
            return merged_path

        @task()
        def load_csv_to_pg(
            conn_id: str,
            csv_path: str,
            table: str = "healthcare_records",
            append: bool = True,
        ) -> int:
            schema = "week8_demo"
            with open(csv_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                rows = [
                    tuple(r.get(col, "") or None for col in fieldnames) for r in reader
                ]
            if not rows:
                return 0
            create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
            create_table = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    {', '.join([f'{col} TEXT' for col in fieldnames])}
                );
            """
            delete_rows = f"DELETE FROM {schema}.{table};" if not append else None
            insert_sql = f"""
                INSERT INTO {schema}.{table} ({', '.join(fieldnames)})
                VALUES ({', '.join(['%s' for _ in fieldnames])});
            """
            hook = PostgresHook(postgres_conn_id=conn_id)
            conn = hook.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(create_schema)
                    cur.execute(create_table)
                    if delete_rows:
                        cur.execute(delete_rows)
                    cur.executemany(insert_sql, rows)
                    conn.commit()
                return len(rows)
            except DatabaseError:
                conn.rollback()
                return 0
            finally:
                conn.close()

        # ETL execution order
        p_file = fetch_patients()
        a_file = fetch_appointments()
        m_file = merge_csvs(p_file, a_file)
        loaded = load_csv_to_pg(conn_id="Postgres", csv_path=m_file, table=TARGET_TABLE)
        p_file >> a_file >> m_file >> loaded

    # --- Analysis TaskGroup ---
    with TaskGroup(
        group_id="analysis_group", tooltip="Analyze and visualize healthcare data"
    ) as analysis_group:

        @task()
        def analyze_healthcare_data():
            import pandas as pd
            import matplotlib.pyplot as plt

            merged_path = os.path.join(OUTPUT_DIR, "merged_data.csv")
            df = pd.read_csv(merged_path)
            plt.figure(figsize=(13, 8))
            plt.subplot(2, 2, 1)
            df["department"].value_counts().plot.bar(title="Appointments by Department")
            plt.subplot(2, 2, 2)
            df["status"].value_counts().plot.pie(
                autopct="%.0f%%", title="Appointment Status"
            )
            plt.subplot(2, 2, 3)
            df["blood_type"].value_counts().plot.barh(title="Blood Type Distribution")
            plt.subplot(2, 2, 4)
            df.groupby("department")["consultation_fee"].mean().plot.barh(
                title="Avg Fee by Department"
            )
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, "dashboard.png"))
            print("Dashboard saved to dashboard.png")
            return os.path.join(OUTPUT_DIR, "dashboard.png")

        dashboard = analyze_healthcare_data()

    # --- Cleanup ---
    @task()
    def cleanup_folder(folder_path: str = OUTPUT_DIR) -> None:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path) and filename.endswith(".csv"):
                os.remove(file_path)
        print("Cleaned up intermediate CSV files.")

    # --- DAG dependencies ---
    etl_group >> analysis_group >> cleanup_folder()
