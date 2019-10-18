import os

script = os.environ['PROCESSINGSCRIPT']

if script == 'pubsub-bq':
    os.system("python pubsub-bq.py")
elif script == 'publisher':
    os.system("python publisher.py")
else:
    print("unknown script %s" % script)
