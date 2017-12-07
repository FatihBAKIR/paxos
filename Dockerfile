FROM ubuntu:16.04

RUN mkdir /data

WORKDIR /data

COPY cmake-build-debug/paxos .

CMD ["/bin/bash"]