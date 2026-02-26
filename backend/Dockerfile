# Используем официальный образ Python 3.13
FROM python:3.13-slim

# Устанавливаем рабочую директорию в контейнере
WORKDIR /backend

# Устанавливаем переменные окружения
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/backend:/backend/minimal_fastapi_skeleton

# Устанавливаем uv
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir uv

# Устанавливаем зависимости системы
RUN apt-get update && apt-get install -y \
    net-tools \
    telnet \
    curl \
    wget \
    dnsutils \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Копируем файлы зависимостей
COPY requirements.txt ./

# Устанавливаем зависимости с помощью uv в системное окружение
RUN uv pip install --system -r requirements.txt

# Копируем директории проекта
COPY . .

# Создаем директорию для логов
RUN mkdir -p logs

# Открываем порт 8002 для приложения
EXPOSE 8002

# Запускаем приложение с помощью uvicorn
CMD ["uvicorn", "minimal_fastapi_skeleton.app.app:app", "--host", "0.0.0.0", "--port", "8002", "--workers", "1"]