FROM python:3.9.7-slim
COPY load_balancer /
COPY commons /commons
RUN pip install -r requirements.txt
ENTRYPOINT ["/bin/sh"]