FROM python:3-alpine3.15
WORKDIR /usr/src/app
COPY . .
RUN pip3 install -r requirements.txt

EXPOSE 3000
CMD python3 ./main.py

ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/src/app/service_account.json"