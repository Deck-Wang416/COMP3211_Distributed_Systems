FROM python:3.9

RUN pip install pymssql

COPY task1.py /task1.py

CMD ["python", "/task1.py"]
