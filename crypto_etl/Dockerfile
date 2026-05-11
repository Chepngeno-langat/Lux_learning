FROM apache/airflow:2.8.1
USER airflow

COPY scripts/requirements.txt .

RUN pip install --no-cache-dir --progress-bar off -r requirements.txt

RUN sed -i 's/self.timer.start()/#self.timer.start()/' \
    /home/airflow/.local/lib/python3.8/site-packages/limits/storage/memory.py
