# ⚡ Smart Meter Real-Time Streaming Analytics

A live data pipeline that replays household electricity readings as a real-time
stream, detects anomalies with **IsolationForest**, and displays everything on
an auto-refreshing **Streamlit** dashboard.

```
CSV data  →  Kafka  →  Python processor  →  IsolationForest  →  PostgreSQL  →  Streamlit
```

## Architecture

| Layer             | Tool                                     |
|-------------------|------------------------------------------|
| Data source       | CSV replay (UCI smart-meter dataset)     |
| Streaming         | Apache Kafka                             |
| Stream processor  | Python (`collections.deque` window)      |
| ML algorithm      | `sklearn.ensemble.IsolationForest`       |
| Database          | PostgreSQL                               |
| Visualization     | Streamlit (auto-refresh every 2 s)       |

## Prerequisites

- Python 3.10+
- Apache Kafka (Homebrew: `brew install kafka`)
- PostgreSQL 14+
- pip packages:
  ```bash
  pip install kafka-python-ng pandas scikit-learn joblib psycopg2-binary streamlit
  ```

## Quick Start

### 1. Prepare the dataset
```bash
cd smartmeter
python prepare_data.py
```

### 2. Train the anomaly model (offline, once)
```bash
python train_model.py
```

### 3. Set up PostgreSQL
```bash
psql -U postgres -f setup_db.sql
```

### 4. Start Kafka (KRaft mode — no ZooKeeper needed for Kafka ≥ 3.3)
```bash
brew services start kafka
# or manually:
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

Create the topic:
```bash
kafka-topics --create --topic smartmeter-events --bootstrap-server 127.0.0.1:9092
```

### 5. Start the processor (Terminal 1)
```bash
python processor.py
```

### 6. Start the dashboard (Terminal 2)
```bash
streamlit run dashboard.py
```

### 7. Start the producer (Terminal 3)
```bash
python producer.py
```

### 8. Watch the demo!
Open the Streamlit URL (usually http://localhost:8501) and watch the power
curve draw itself with red anomaly markers appearing on spikes.

## Resetting for a fresh demo
```bash
python reset_tables.py
```

## File Listing

| File              | Purpose                                       |
|-------------------|-----------------------------------------------|
| `prepare_data.py` | Download & clean the UCI dataset → `data.csv`  |
| `train_model.py`  | Train IsolationForest → `anomaly_model.pkl`    |
| `producer.py`     | Replay CSV rows to Kafka                       |
| `processor.py`    | Kafka consumer → rolling avg + ML → PostgreSQL |
| `dashboard.py`    | Streamlit live dashboard                       |
| `setup_db.sql`    | PostgreSQL schema (database, user, tables)     |
| `reset_tables.py` | Truncate tables for a fresh run                |
