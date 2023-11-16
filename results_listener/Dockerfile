FROM python:3.9.7-slim
COPY results_listener /
COPY commons /commons
RUN pip install -r requirements.txt
ENTRYPOINT ["/bin/sh"]