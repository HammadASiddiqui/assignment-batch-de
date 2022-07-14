FROM python:latest
WORKDIR /code
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY app.py app.py
COPY api_auth.yaml api_auth.yaml
COPY db_creds.yaml db_creds.yaml
CMD ["python", "-u", "app.py"]