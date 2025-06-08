FROM python:3.11-slim

# 1️⃣ Basic hygiene
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 MPLBACKEND=Agg

WORKDIR /app

# 2️⃣ Dependences
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3️⃣ Job code
COPY process_iot.py .

# 4️⃣ Entrypoint
ENTRYPOINT ["python", "/app/process_iot.py"]
