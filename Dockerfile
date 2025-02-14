# Dockerfile
FROM python:3.11-slim
USER root

#copy & install req.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy local code to the container image.

WORKDIR $APP_HOME
COPY html_classifiers.py .
COPY db_quieries.py .
COPY trinity.py .
COPY temp_classes/async_api.py .
COPY temp_classes/base_query.py .
COPY temp_classes/confirm_targets.py .
COPY temp_classes/date_ops.py .
COPY temp_classes/parse_engine.py .
COPY main.py .


# Expose ports
EXPOSE 8080

# Call application
# (2 x $num_cores) + 1 ~= len(workers)
# CMD ["gunicorn", "--worker-class", "gthread", "-w", "9", "--threads", "100", "--timeout", "0", "--bind", "0.0.0.0:8080", "main:app"]
CMD ["python3", "main.py"]