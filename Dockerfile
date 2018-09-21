FROM python:3.6-onbuild

RUN apt-get update && apt-get install -y --no-install-recommends \
		netcat-openbsd \
	&& rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["./main.py"]
