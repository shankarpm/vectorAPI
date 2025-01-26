import requests
import logging
import time
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from sqlalchemy import create_engine
from retrying import retry
from langchain.agents import initialize_agent, Tool
from langchain.llms import OpenAI
from langchain.tools.python.tool import PythonREPLTool

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("weather_monitor.log"), logging.StreamHandler()]
)

# Constants
OPENWEATHER_API_KEY = "YOUR_API_KEY"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
DATABASE_URI = "sqlite:///weather_data.db"
ALERT_EMAIL = "your_email@example.com"
SMTP_SERVER = "smtp.example.com"
SMTP_PORT = 587
SMTP_USER = "your_email@example.com"
SMTP_PASSWORD = "your_password"
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
CHECK_INTERVAL = 600  # 10 minutes

# Database setup
engine = create_engine(DATABASE_URI)

# Email alert function
def send_email_alert(subject, body):
    try:
        msg = MIMEMultipart()
        msg["From"] = SMTP_USER
        msg["To"] = ALERT_EMAIL
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        logging.info(f"Email alert sent: {subject}")
    except Exception as e:
        logging.error(f"Failed to send email alert: {e}")

# Fetch weather data function
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_weather(city):
    params = {
        "q": city,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric"
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        logging.info(f"Weather data fetched for {city}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data for {city}: {e}")
        raise

# Process weather data
def process_weather_data(data):
    try:
        processed_data = {
            "city": data["name"],
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather": data["weather"][0]["description"],
            "timestamp": pd.Timestamp.now()
        }
        logging.info(f"Weather data processed for {data['name']}")
        return processed_data
    except KeyError as e:
        logging.error(f"Error processing weather data: {e}")
        raise

# Save data to database
def save_to_database(data):
    try:
        df = pd.DataFrame([data])
        df.to_sql("weather", engine, if_exists="append", index=False)
        logging.info(f"Weather data saved to database for {data['city']}")
    except Exception as e:
        logging.error(f"Error saving data to database: {e}")
        raise

# Check for alerts
def check_for_alerts(data):
    try:
        if data["temperature"] > 35:
            send_email_alert(
                f"High Temperature Alert: {data['city']}",
                f"The temperature in {data['city']} is {data['temperature']}°C, which exceeds the threshold."
            )
        if data["humidity"] > 90:
            send_email_alert(
                f"High Humidity Alert: {data['city']}",
                f"The humidity in {data['city']} is {data['humidity']}%, which exceeds the threshold."
            )
    except Exception as e:
        logging.error(f"Error checking alerts: {e}")

# LangChain Agent Integration
llm = OpenAI(temperature=0.7)
python_tool = PythonREPLTool()

tools = [
    Tool(
        name="Fetch Weather Data",
        func=lambda city: fetch_weather(city),
        description="Fetches the weather data for a given city."
    ),
    Tool(
        name="Process Weather Data",
        func=lambda data: process_weather_data(data),
        description="Processes raw weather data into a structured format."
    ),
    Tool(
        name="Save Weather Data",
        func=lambda data: save_to_database(data),
        description="Saves the processed weather data to the database."
    ),
    Tool(
        name="Check Alerts",
        func=lambda data: check_for_alerts(data),
        description="Checks weather data for any alerts and sends notifications."
    )
]

agent = initialize_agent(tools, llm, agent="zero-shot-react-description", verbose=True)

# Database initialization
def initialize_database():
    try:
        df = pd.DataFrame(columns=["city", "temperature", "humidity", "weather", "timestamp"])
        df.to_sql("weather", engine, if_exists="replace", index=False)
        logging.info("Database initialized successfully")
    except Exception as e:
        logging.error(f"Error initializing database: {e}")

if __name__ == "__main__":
    logging.info("Starting weather monitoring system...")
    initialize_database()

    # Example: Using the LangChain agent to fetch, process, and save weather data for a city
    logging.info("Interacting with LangChain agent...")
    agent.run(
        "Fetch weather data for New York, process it, save it to the database, and check for alerts."
    )
