FROM python:3.11-bullseye
WORKDIR /root
COPY requirements.txt ./
COPY ./src ./
RUN ["pip", "install", "-r", "requirements.txt"]
EXPOSE 5555
CMD ["uvicorn", "main:app", "--reload", "--host", "0.0.0.0","--port", "5555"]