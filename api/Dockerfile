FROM python:3.11-slim

WORKDIR /app

# Копируем requirements.txt из папки api
COPY ./api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем ПАПКУ api/ ЦЕЛИКОМ (не содержимое!)
COPY ./api/ ./api/

# Запускаем приложение
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]