FROM python:3.9

RUN pip install pymssql
RUN pip install matplotlib

COPY task1.py /task1.py
COPY task2.py /task2.py

# CMD ["python", "/task1.py"]
CMD ["python", "/task2.py"]
