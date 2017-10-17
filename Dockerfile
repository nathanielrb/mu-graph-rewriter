FROM nathanielrb/mu-chicken-template

MAINTAINER "Nathaniel Rudavsky-Brody, <nathaniel.rudavsky@gmail.com>"

RUN cd /usr/src/app/s-sparql && \
            git checkout 086eb6c6663e4d348063e999fc1429bade78e1f3 && \
            chicken-install