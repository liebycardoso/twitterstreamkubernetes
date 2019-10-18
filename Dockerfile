FROM python:3

RUN pip install --upgrade pip
RUN pip install tweepy
RUN pip install --upgrade google-api-python-client
RUN pip install python-dateutil

ADD publisher.py /publisher.py
ADD pubsub-bq.py /pubsub-bq.py
ADD controller.py /controller.py
ADD utils.py /utils.py

CMD python controller.py
