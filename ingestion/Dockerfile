FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    boto3==1.34.0 \
    "numpy<2" \
    pyarrow==14.0.2
COPY generate_data.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "generate_data.py"]
