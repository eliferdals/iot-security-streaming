
# CyberSentry: Real-Time IoT Threat Monitoring with Kafka & Docker

This project simulates and analyzes real-time cybersecurity threats on IoT devices, focusing on DDoS and SSH brute-force attacks. It leverages Apache Kafka for real-time data streaming and Docker for containerized deployment.

## 🚀 Project Overview

- **Objective**: To simulate cyber-attacks and process threat data in real-time.
- **Stack**: Python, Kafka, Docker, Ubuntu
- **Target**: IoT devices with limited resources

## 🧱 Architecture

```
Producer (Python) --> Kafka Topic --> Consumer (Python)
        ^                               |
     Simulated                          v
    Attacks (DDoS, SSH)        Streaming Analyzer (Optional)
```

- `producer.py`: Generates and sends fake attack data to a Kafka topic.
- `consume.py`: Listens to the Kafka topic and logs incoming data.
- `streaming.py`: (Optional) Real-time stream processing & analytics.
- `docker-compose-kafka.yml`: Spins up Kafka and Zookeeper in Docker containers.

## ⚙️ How to Run

1. **Start Kafka & Zookeeper**

```bash
docker-compose -f docker-compose-kafka.yml up -d
```

2. **Run the Producer**

```bash
python producer.py
```

3. **Run the Consumer**

```bash
python consume.py
```

## 📦 Requirements

- Docker & Docker Compose
- Python 3.8+
- `pip install -r requirements.txt`

## 💡 Future Enhancements

- Integrate with Prefect or Airflow for scheduling
- Add real-time dashboard with Grafana
- Introduce machine learning for threat classification

## 📜 License

This project is open-source under the MIT License.

---

Made with ❤️ by Elif Erdal
