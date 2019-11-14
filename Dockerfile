FROM python:3

RUN pip install --upgrade pip
RUN pip install tweepy
RUN pip install --upgrade google-api-python-client
RUN pip install --upgrade google-api-python-client oauth2client
RUN pip install python-dateutil
RUN pip install --upgrade google-cloud-bigquery
RUN pip install --upgrade google-cloud-pubsub

ADD publisher.py /publisher.py
ADD pubsub-bq.py /pubsub-bq.py
ADD controller.py /controller.py

CMD python controller.py
