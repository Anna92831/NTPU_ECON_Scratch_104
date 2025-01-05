from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Set non-interactive backend for matplotlib
matplotlib.use('Agg')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'parking_rate_anomaly_detection',
    default_args=default_args,
    description='A DAG to detect anomalies in parking rate data',
    schedule='@daily',
    catchup=False
)

# Paths to the local CSV files
data_files = [
    "/opt/airflow/dags/仁愛parkingrate.csv",
    "/opt/airflow/dags/國企parkingrate.csv",
    "/opt/airflow/dags/福悠parkingrate.csv"
]
output_csv_path = "/opt/airflow/output/anomalies_report.csv"
output_plot_path = "/opt/airflow/output/anomalies_plot.png"

def extract_data():
    """ Extract data from local CSV files and combine them into one DataFrame """
    dataframes = [pd.read_csv(file) for file in data_files]
    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df['time'] = pd.to_datetime(combined_df['time'], errors='coerce')
    combined_df['created_at'] = pd.to_datetime(combined_df['created_at'], errors='coerce')
    return combined_df

def detect_anomalies(**kwargs):
    """ Detect anomalies in the parking rate data, save to CSV, and generate a plot """
    df = extract_data()

    # Rule-based anomaly detection: parking_rate should be > 0.8 or < 0.2
    anomalies = df[(df['parking_rate'] > 0.8) | (df['parking_rate'] < 0.2)]

    # Save anomalies to CSV
    anomalies.to_csv(output_csv_path, index=False)
    print(f"Anomalies detected: {anomalies.shape[0]} rows saved to {output_csv_path}")

    # Plot anomalies
    plt.figure(figsize=(10, 6))
    plt.plot(df['time'], df['parking_rate'], label='Parking Rate', color='blue', alpha=0.5)
    plt.scatter(anomalies['time'], anomalies['parking_rate'], color='red', label='Anomalies')
    plt.xlabel('Time')
    plt.ylabel('Parking Rate')
    plt.title('Parking Rate with Anomalies Highlighted')
    plt.legend()
    plt.grid(True)

    # Save the plot as an image
    plt.savefig(output_plot_path)
    print(f"Anomalies plot saved to {output_plot_path}")

def send_email(**kwargs):
    """ Send an email with the anomalies report and plot as attachments """
    # Email configuration
    sender_email = "anna.hsu.831@gmail.com"
    receiver_email = "anna.hsu@uspace.city"
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    smtp_password = os.getenv("SMTP_PASSWORD")

    # Create the email
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = "Daily Parking Rate Anomalies Report"

    # Email body
    body = "Please find attached the daily parking rate anomalies report and plot."
    msg.attach(MIMEText(body, 'plain'))

    # Attach the CSV report
    with open(output_csv_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {os.path.basename(output_csv_path)}",
    )
    msg.attach(part)

    # Attach the PNG plot
    with open(output_plot_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {os.path.basename(output_plot_path)}",
    )
    msg.attach(part)

    # Send the email with error handling
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, smtp_password)
            server.send_message(msg)
        print(f"Email sent to {receiver_email} with anomalies report and plot.")
    except Exception as e:
        print(f"Failed to send email: {e}")

# Define tasks
anomaly_detection_task = PythonOperator(
    task_id='detect_anomalies',
    python_callable=detect_anomalies,
    dag=dag
)

email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_email,
    dag=dag
)

# Task dependencies
anomaly_detection_task >> email_task
