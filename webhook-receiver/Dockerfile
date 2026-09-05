FROM python:3.11-slim
RUN useradd --no-create-home --shell /bin/false webhook
WORKDIR /app
COPY --chown=webhook:webhook app.py .
USER webhook
EXPOSE 5001
CMD ["python", "app.py"]
