FROM python:slim

RUN apt update && apt upgrade -y

# Create User
ARG user=dgp
ARG home=/home/$user

RUN addgroup docker
RUN adduser \
	--disabled-password \
	--home $home \
	--ingroup docker \
	$user

WORKDIR $home
USER $user

RUN echo "PS1='\[\033[38;5;27m\]\t\[$(tput sgr0)\] \[$(tput sgr0)\]\[\033[38;5;51m\]\u\[$(tput sgr0)\] \w \[$(tput sgr0)\]\[\033[38;5;11m\]\\$\[$(tput sgr0)\]\n\[$(tput sgr0)\]'" >> .bashrc

# Install Dependencies
RUN python3 -m pip install --no-cache-dir dgraphpandas

ENV ACCEPT_LICENSE=y
RUN curl https://get.dgraph.io -sSf | bash

ENTRYPOINT python -m dgraphpandas
# ENTRYPOINT dgraph
