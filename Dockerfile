FROM python:3.12

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
# ponytail: .env must NEVER be baked into the image — pass secrets via docker run -e or docker-compose environment:

# Create necessary directories
RUN mkdir -p data logs

# Run the bot
CMD ["python", "src/main.py"]
