# scheduler/Dockerfile

FROM python:3.11-slim

WORKDIR /app

# Копируем requirements.txt и ставим зависимости
COPY scheduler/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем ВСЮ ПАПКУ scheduler/ в /app/scheduler/
COPY scheduler/ ./scheduler/

# Устанавливаем /app как PYTHONPATH, чтобы Python видел модуль scheduler
ENV PYTHONPATH=/app

CMD ["python", "scheduler/main.py"]