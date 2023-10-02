FROM python:3.9.7-slim
COPY processors/tres_escalas_o_mas /
COPY commons /commons
RUN pip install -r requirements.txt
ENTRYPOINT ["/bin/sh"]