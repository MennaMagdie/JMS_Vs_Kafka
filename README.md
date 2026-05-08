# JMS vs Kafka

## Starting ActiveMQ

**Linux:**
```bash
./activemq start
```

**Windows:**
```bash
bin\activemq.bat start
```

Web Console: `http://localhost:8161/`
> credentials — username: `admin` , password: `admin`

---

## Compiling

**Linux:**
```bash
java -cp ".:../apache-activemq-6.2.5/activemq-all-6.2.5.jar:../apache-activemq-6.2.5/lib/optional/*" App
```
> Note: other classpath formats didn't work on Linux — check why

**Windows:**
```bash
javac -cp "...\apache-activemq-6.2.5\activemq-all-6.2.5.jar" App.java Consumer.java MaxThroughput.java
```
or
```bash
javac -cp "...\apache-activemq-6.2.5\lib\*" App.java Consumer.java MaxThroughput.java
```
> Note: `lib\*` was used because some libraries weren't read properly from `activemq-all.jar`

---

## Running

```bash
java -cp ".;...\apache-activemq-6.2.5\lib\*" App

java -cp ".;...\apache-activemq-6.2.5\lib\*" Consumer

java -cp ".;...\apache-activemq-6.2.5\lib\*" MaxThroughput
```

> **Windows** uses `.;` — **Linux** uses `.:`

---

## Running Order

### 1. Consume Response Time
Run `App` first, then `Consumer`

### 2. End-to-End Latency
Run `App` and `Consumer` concurrently
> Do **not** start `Consumer` before `App` — it will freeze waiting for messages

### 3. Max Throughput
Run `MaxThroughput` alone — it internally creates both a producer and consumer and measures throughput between them

---

## Max Throughput — JMeter Alternative

**Test Plan structure:**
```
Test Plan
└── Thread Group
    ├── JMS Point-to-Point (Sampler)
    ├── View Results Tree (Listener)
    └── Summary Report (Listener)
```

**Thread Group Settings:**

| Setting | Value | Reason |
|---|---|---|
| Threads | 1 | Simulate single producer |
| Ramp-up | 1s | Apply all load immediately |
| Loop count | 100 |

**Constant Throughput Timer** — set Target throughput in samples per minute (X msg/sec × 60):

| msg/sec | Timer value (per minute) |
|---|---|
| 100 | 6,000 |
| 200 | 12,000 |
| 400 | 24,000 |
| 800 | 48,000 |

![alt text](images/jms-producer.png)

![alt text](images/jms-producer-msgs.png)

![alt text](images/image-1.png)
