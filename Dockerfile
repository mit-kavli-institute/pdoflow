FROM thekevjames/nox
RUN apt-get update && apt-get install -y postgresql libpq-dev
CMD ["tail", "-f", "/dev/null"]
