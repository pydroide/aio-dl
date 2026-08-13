FROM python:3.10-slim

# FFmpeg install karo taaki audio/video merge me koi error na aaye
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "aio-telegram-bot.py"]
