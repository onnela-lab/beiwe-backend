<p align="left">
  <img width="264" height="99" src="https://github.com/onnela-lab/beiwe-backend/blob/main/beiwe-logo-color.png">
</p>

# Beiwe
The Onnela Lab at the Harvard T.H. Chan School of Public Health has developed the Beiwe (bee-we) research platform to collect smartphone-based high-throughput digital phenotyping data. The fully configurable open-source platform supports collection of a range of social, behavioral, and cognitive data, including spatial trajectories (via GPS), physical activity patterns (via accelerometer and gyroscope), social networks and communication dynamics (via call and text logs), and voice samples (via microphone). The platform consists of a front-end smartphone application for iOS (by Apple) and Android (by Google) devices and a back-end system, which supports a web-based study management portal for data processing and storage, based on Amazon Web Services (AWS) cloud computing infrastructure. Data analysis is increasingly identified as the main bottleneck; our data analysis platform, [Forest](https://github.com/onnela-lab/forest), makes sense of the data collected by Beiwe.

Beiwe can collect active data (participant input required) and passive data (participant input not required). Currently supported active data types for both Android and iOS include text surveys and audio recordings and their associated metadata. The questions, answers, and skip logic can be configured from the web-based dashboard. Passive data can be further divided into two groups: phone sensor data (e.g., GPS) and phone logs (e.g., communication logs). Beiwe collects raw sensor data and phone logs, which is crucial in scientific research settings. Beiwe has two front-end applications, one for Android (written in Java and Kotlin) and another for iOS (written in Swift & Objective-C). The Beiwe back-end, which is based on Amazon Web Services (AWS) cloud computing infrastructure, has been implemented primarily in Python 3.6, but also makes use of Django (for ORM) and Flask (for API and web server). It also uses several AWS services: primary S3 (for flat file storage), EC2 (servers), Elastic Beanstalk (scaling servers), and RDS (PostgreSQL).

Every aspect of data collection is fully customizable, including which sensors to sample, sampling frequency, addition of Gaussian noise to GPS location, use of Wi-Fi or cellular data for uploads, data upload frequency, and specification of surveys and their response options. Study participants simply download the Beiwe application from the app store and enter three pieces of information: a system-generated 8-character user ID, a system-generated temporary password, and an IP address of the back-end server. If no active data is being collected in the study (i.e., no surveys), this is the only time the participant will interact with the application. However, most studies make use of occasional self-reports or EMA, and some use the audio diary feature to collect rich data on lived experiences.

All Beiwe data is encrypted while stored on the phone awaiting upload and while in transit, and are re-encrypted for storage on the study server. During study registration, Beiwe provides the smartphone app with the public half of a 2048-bit RSA encryption key. With this key, the device can encrypt data, but only the server, which has the private key, can decrypt it. Thus, the Beiwe application cannot read its own temporarily stored data, and the study participant (or somebody else) cannot export the data. The RSA key is used to encrypt a symmetric Advanced Encryption Standard (AES) key for bulk encryption. These keys are generated as needed by the app and must be decrypted by the study server before data recovery. Data received by the cloud server is re-encrypted with the study master key and then stored.

Some of the data collected by Beiwe contain identifiers, such as phone numbers. The Beiwe app generates a unique cryptographic code, called a salt, during the Beiwe registration process, and then uses the salt to encrypt phone numbers and other similar identifiers. The salt never gets uploaded to the server and is known only to the phone for this purpose. Using the industry-standard SHA-256 (Secure Hash Algorithm) and PBKDF2 (Password-Based Key Derivation Function 2) algorithms, an identifier is transformed into an 88-character anonymized string that can then be used in data analysis.

A recent study found that 65% of medical studies were inconsistent when retested, and only 6% were completely reproducible. Reproducibility of studies using mobile devices may be even lower given the variability of devices, heterogeneity in their use, and lack of standardized methods for data analysis. All Beiwe study data collection settings, from sensors to surveys, are captured in a human readable JSON file. These files can be imported into Beiwe and exported out of Beiwe. To replicate a study, the investigator can simply upload an existing configuration file.

# Setup instructions

## Configuring SSL
Because Beiwe often deals with sensitive data covered under HIPAA, it's important to add an SSL certificate so that web traffic is encrypted with HTTPS.

The setup script [uses AWS Certificate Manager to generate an SSL certificate](http://docs.aws.amazon.com/acm/latest/userguide/gs-acm-request.html).  AWS Certificate Manager [will check that you control the domain by sending verification emails](http://docs.aws.amazon.com/acm/latest/userguide/gs-acm-validate.html) to the email addresses in the domain's WHOIS listing.

## Configuring Firebase
To initialize the Firebase SDK, [generate a private key file](https://firebase.google.com/docs/admin/setup#initialize-sdk).
Rename the file firebase_cloud_messaging_credentials.json and place it in the project root.

***

# Configuration settings

### Mandatory Settings

If any of these environment options are not provided, Beiwe will not run. Empty strings and None  are considered invalid.

```
    FLASK_SECRET_KEY - a unique, cryptographically secure string
    AWS_ACCESS_KEY_ID - AWS access key for S3
    AWS_SECRET_ACCESS_KEY - AWS secret key for S3
    S3_BUCKET - the bucket for storing app-generated data
    SYSADMIN_EMAILS - a comma separated list of email addresses for recipients of error reports. (whitespace before and after addresses will be ignored)
    RDS_DB_NAME - postgress database name (the name of the database inside of postgres)
    RDS_USERNAME - database username
    RDS_PASSWORD - database password
    RDS_HOSTNAME - database IP address or url
    S3_ACCESS_CREDENTIALS_USER - the user id for s3 access for your deployment
    S3_ACCESS_CREDENTIALS_KEY - the secret key for s3 access for your deployment
```

### Optional Settings
There are additional settings that you will find documented in the [config/settings.py](https://github.com/onnela-lab/beiwe-backend/blob/main/config/settings.py) file.

We _strongly_ recommend adding Sentry DSNs to all your Beiwe servers.  Without these there is very little data to work with when something goes wrong, and we won't be able to assist.

***

# Development setup
How to set up beiwe-backend running on a development machine (NOT a production instance!  For a production instance,
see https://github.com/onnela-lab/beiwe-backend/wiki/Deployment-Instructions---Scalable-Deployment)

#### Before starting:
While it is possible to run your development environment inside of the system Python environment, this practice is _strongly discouraged_.  We recommend familiarizing yourself with one of the following: Python's [venv](https://docs.python.org/3/tutorial/venv.html) library (basic virtual environments), [Pyenv](https://github.com/pyenv/pyenv) (allows for compiling particular target versions of Python, plus some quality-of-life command-line shell integrations), or [Conda](https://docs.conda.io/en/latest/) (another option, includes integrations with non-Python libraries).  Note also that the codebase expects at least Python version 3.6.

1. `sudo apt-get update; sudo apt-get install postgresql libpq-dev`
2. `pip install --upgrade pip setuptools wheel`
3. `pip install -r requirements.txt`
4. Create a file for your environment variables that contains at least these:
    ```
    export DOMAIN_NAME="localhost://8080"
    export FLASK_SECRET_KEY="asdf"
    export S3_BUCKET="a"
    export SYSADMIN_EMAILS="sysadmin@localhost"
    ```
    I usually store it at `private/environment.sh`.  Load up these environment variables by running `source private/environment.sh` at the Bash prompt.

For additional tips on running a local development enironment please see [this wiki page](https://github.com/onnela-lab/beiwe-backend/wiki/Tips-For-Local-Beiwe-Development).  If you are having difficulty getting started, or believe you could assist with any issues of documentation, please [post an issue with a documentation tag](https://github.com/onnela-lab/beiwe-backend/labels/documentation).

### Local Celery setup
**Update**: it is no longer necessary to use Celery for local testing, though you still need it to be installed in your Python environment in order to avoid import errors.  A full test of Celery requires the full setup below, including installing `rabbitmq`, but as long as the file for the rabbitmq host server IP and password (`manager_ip` in the root of the repository) is missing you will instead be presented with output similar to the example shell session below, indicating a that you are running in a _much_ more convenient single-threaded local testing mode:

```
In [1]: from services.celery_data_processing import *
task declared, args: (), kwargs:{'queue': 'data_processing'}
Instantiating a FalseCeleryApp for celery_process_file_chunks.
```

For those souls brave enough to run the entire broker queue and Celery task dispatch machinery locally, here are our best instructions.  Caveat: this configuration is based on a one that is known to work on Ubuntu 18.04, and is potentially incompatible with the version of RabbitMQ provided in Ubuntu 20.04. Also, due to the use of the system `service` command it is incompatible with the varient of Ubuntu for use on the Windows Subsystem for Linux.  Have at:

1. Install RabbitMQ (https://docs.celeryproject.org/en/latest/getting-started/backends-and-brokers/rabbitmq.html#broker-rabbitmq)
    1. Edit `/etc/rabbitmq/rabbitmq-env.conf` and add the line `NODE_PORT=50000`
    2. Restart RabbitMQ like this in the Bash shell: `time sudo service rabbitmq-server restart` (`time` isn't necessary, but it tells you that the command has finished, and how long the command took to execute... which can be... random and excessive?)
2. `pip install -r requirements_data_processing.txt` (this will install Celery using pip)
3. Create a file called `manager_ip` in the top level of your `beiwe-backend` repo, and enter these two lines in it.  Do not provide a trailing new-line character.
    ```
    127.0.0.1:50000
    [YOUR DESIRED PASSWORD]
    ```
    Where the password is the one you set when setting up RabbitMQ
4. run this command to create a user for rabbitmq: `rabbitmqctl add_user beiwe [INSERT THAT PASSWORD HERE]`
5. run this command to allow the beiwe user access to the appropriate queues: `sudo rabbitmqctl set_permissions -p / beiwe ".*" ".*" ".*"`
6. If you intend to test Firbase push notifications you will need to upload functional firebase credentials on the local website interface.
7. To execute push notification tasks run this command _while inside the root of the repo_: `celery -A services.celery_push_notifications worker -Q push_notifications --loglevel=info -Ofair --hostname=%%h_notifications --concurrency=20 --pool=threads`
8. To run data processing tasks run this command _while inside the root of the repo_: `celery -A services.celery_data_processing worker -Q data_processing --loglevel=info -Ofair --hostname=%%h_processing`
9. To run forest tasks run this comand _while inside the root of the repo_: `celery -A services.celery_forest worker -Q forest_queue --loglevel=info -Ofair --hostname=%%h_forest` (Forest is still in beta.)
10. Run this command to dispatch new tasks, which will then be consumed by the Celery processes, _while inside the root of the repo_. `python services/cron.py five_minutes`



### Forest

Warning: The Forest integration is still in beta; running Forest may cause significant data processing costs.
