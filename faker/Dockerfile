FROM python:3.9-slim

WORKDIR /app

RUN pip install Faker==18.10.1

RUN pip install psycopg2-binary

COPY faker_data_generator.py .

CMD ["python", "-u", "faker_data_generator.py"]





