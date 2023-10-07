FROM python:3.9.7-slim
COPY processors/dos_mas_rapidos /
COPY commons /commons
RUN pip install -r requirements.txt
ENTRYPOINT ["/bin/sh"]