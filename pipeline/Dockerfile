# Parent image
FROM ubuntu:18.04

# Certian installers have GUI's, this variable overide interactive installation.
ENV DEBIAN_FRONTEND=noninteractive

# Install system requirements
RUN ["apt", "update"]
RUN ["apt", "install", "-y", "gnupg2", "python3-pip", "software-properties-common", "git"]

# Ubuntu 18.04 repository doesn't include R(3.5.2). To fix this we need to add
# an external repository to install R from. First, we need to add GPG key:
RUN ["apt-key", "adv", "--keyserver", "keyserver.ubuntu.com", "--recv-keys", "E298A3A825C0D65DFD57CBB651716619E084DAB9"]
# Once we have the trusted key, we add the repository as follows:
RUN ["add-apt-repository", "deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/"]
RUN ["apt", "update"]
RUN ["apt", "install", "-y", "r-base"]


# Work from home directory
WORKDIR /home

# Move the directory into the corresponding directory in the container
ADD Beiwe-Analysis Beiwe-Analysis

# Install python requirements
RUN ["pip3", "install", "--upgrade", "pip"]
RUN ["pip3", "install", "-r", "Beiwe-Analysis/Pipeline/python3-requirements.txt"]

# Install R requirements
RUN ["Rscript", "--vanilla", "Beiwe-Analysis/Summary/install_required_packages.R"]
