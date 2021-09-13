web: gunicorn -w 4 -k uvicorn.workers.UvicornWorker app:app
web: gunicorn main:app --port 5000
web: gunicorn celery -A models worker --loglevel=INFO --pool=prefork
