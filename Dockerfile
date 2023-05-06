FROM alpine:3.7
RUN apk upgrade --update && apk add --no-cache python3 python3-dev gcc gfortran freetype-dev musl-dev libpng-dev g++ lapack-dev
RUN pip3 install schedule
RUN apk add --update docker openrc
RUN rc-update add docker default
RUN apk add openssh
CMD openssl version -a