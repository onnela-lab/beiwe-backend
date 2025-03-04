<div align="center">

<img width="33%" height="33%" src="beiwe-logo-color.png">

## Welcome to the Beiwe Platform
</div>
<!-- there has to be an extra line of whitespace here for the following paragraph to break properly -->

The Onnela Lab at the Harvard T.H. Chan School of Public Health has developed the Beiwe Research Platform to collect high-throughput smartphone-based digital phenotyping data. This highly configurable open-source platform supports collection of a range of social, behavioral, and cognitive data, including spatial trajectories (via GPS), physical activity patterns (via accelerometer and gyroscope), social networks and communication dynamics (via call and text logs), and voice samples (via microphone). The platform consists of a smartphone application for [iOS](https://github.com/onnela-lab/beiwe-ios) and [Android](https://github.com/onnela-lab/beiwe-android) devices, plus this repository - the back-end system that supports a web-based study management portal and tools for handling data processing, push notifications, and storage. Beiwe currently supports Amazon Web Services (AWS) cloud computing infrastructure and provides tools to assist in deploying and managing an instance of the Beiwe Platform. Data analysis is increasingly identified as the main bottleneck in digital phonotyping research; our data analysis platform, [Forest](https://github.com/onnela-lab/forest), makes sense of the data collected by Beiwe.

Beiwe can collect active data (participant input required) and passive data collected while the app is in the background. Currently supported active data types for both Android and iOS include textual surveys, audio recordings, and their associated metadata. The questions, answers, and skip logic can be configured on the website. Passive data include phone sensor data ([e.g., GPS, Accelerometer, etc.](https://github.com/onnela-lab/beiwe-backend/wiki/%5BResearchers%5D-Supported-Data-Streams)) and phone logs (e.g., communication logs). Beiwe collects raw sensor data and phone logs, which is crucial in scientific research settings. Beiwe has two participant frontend apps, one [for Android](https://github.com/onnela-lab/beiwe-android) (written in Java and Kotlin) and another [for iOS](https://github.com/onnela-lab/beiwe-ios) (written in Swift). The Beiwe back-end and runs on Python 3.12, and uses the Django webserver and ORM framework. The Platform uses several AWS services: primary S3 (for flat file storage), EC2 (servers), Elastic Beanstalk (load scaling), and RDS (PostgreSQL).

Every aspect of data collection is fully customizable, including which sensors to sample, sampling frequency, addition of Gaussian noise to GPS location, use of Wi-Fi or cellular data for uploads, data upload frequency, and specification of surveys and their response options. Study participants simply download the Beiwe application from the [Apple App Store](https://apps.apple.com/us/app/beiwe2/id1312962738), or on Android sideload the app via a link accessible form your Beiwe instance's website. To Register in a study participants enter three pieces of information: a system-generated 8-character user ID, a system-generated temporary password, and a URL address of your Beiwe website. If no active data is being collected in the study (i.e., no surveys), this is the only time the participant will interact with the application. However, most studies make use of occasional self-reports or EMA, and some use the audio diary feature to collect rich data on lived experiences.

<div align="center">

## Data Security and Privacy
</div>
<!-- there has to be an extra line of whitespace here for the following paragraph to break properly -->

All Beiwe data are encrypted while stored on the phone awaiting upload and while in transit, and are re-encrypted after upload to the study server. During study registration, Beiwe provides the smartphone app with the public half of a 2048-bit RSA encryption key. While the device can encrypt data, only the server, which has the private key, can decrypt it. As such, data stored by the app cannot be compromised. (The RSA key is used to encrypt a symmetric Advanced Encryption Standard (AES) key for bulk encryption. These keys are generated as needed by the app and are decrypted on the study server at time of upload. Data received by the cloud server is re-encrypted with the study's master encrytption key for long term storage.)

Some of the data collected by Beiwe contain identifiers, such as phone numbers. The Beiwe app generates a unique cryptographic code, called a salt, during the Beiwe registration process, and then uses the salt to hash phone numbers and other similar identifiers. The salt never gets uploaded to the server and is known only to the phone for this purpose. Using the industry-standard SHA-256 (Secure Hash Algorithm) and PBKDF2 (Password-Based Key Derivation Function 2) algorithms, an identifier is transformed into an 88-character anonymized string that can then be used in data analysis.

<div align="center">

## Reproducibility

</div>

A recent study found that 65% of medical studies were inconsistent when retested, and only 6% were completely reproducible. Reproducibility of studies using mobile devices may be even lower given the variability of devices, heterogeneity in their use, and lack of standardized methods for data analysis. All Beiwe study data collection settings, from sensors to surveys, are captured in a human readable JSON file. These files can be imported into Beiwe and exported out of Beiwe. To replicate a study, the investigator can simply upload an existing configuration file.

<div align="center">

Cite the code: <br> [![DOI](https://zenodo.org/badge/53344506.svg)](https://zenodo.org/badge/latestdoi/53344506)
</div>


# Setup Instructions

Please see the [this section Beiwe Backend Wiki landing page](https://github.com/onnela-lab/beiwe-backend/wiki#documentation-for-software-developers-and-sysadmins) for extensive documentation on how to deploy a Beiwe Backend instance.

### Expectations for System Administrators

This is an actively maintained but under-development platform with live applications that may evolve over the course of a study or Beiwe Backend instance. No features are expected to be removed, unless they never worked to begin with. The Backend is a rolling release, the apps have semantic version numbers. 

The platform contains a launch script, and we push out periodic updates to the launch script when there are platform-level infrastructure updates or major changes. The launch script manages initial deployment of Elastic Beanstalk environments, major EB platform-level updates, and basic data processing and push notification server management.

If you are running in a context where you have to add additional at-deployment-time software, for instance you are part of an institution that requires intrusion and malware detection software, we have a hook for you to accomplish this, but you will have to provide your own script, and we cannot provide direct support for that.

To run the launch script and manage your own deployment you are expected to have a _basic-to-moderate_ level of knowledge and ability with Python environments, AWS services and credential management, and a familiarity with CLI tools.

You also _must_ be able to use GIT to pull the latest code from the `main` branch of this repository.

Your intended pattern-of-work for maintaining an actual deployment is:
- (Ensure you [still] have the "AWSEBCLI" tool functional, this is described in the initial deployment instructions.)
- Blow away your old manager/worker data processing servers with the termination command via the launch script.
- Pull the up-to-date `main` branch.
- Use the AWSEBCLI `eb deploy` command to update the web servers with current `main` code.
- (`eb deploy` may also update the database schema, it must finish before you deploy a new manager/worker server.)
- Launch a new manager server with the command from the launch script. (and any workers, if you need them)

When there are major technical and migration operations _we provide a command in the launch script and detailed step-by-step instructions on the wiki_. For example the Python 3.8->3.12 Elastic Beanstalk platform update had a `-clone-environment` command a dedicated wiki page.

> [!TIP]
> When these items are under development there will be an open issue with the `Infrastructure` tag, you are welcomed and encouraged to ask questions and participate. Upon completion a *new* issue will be created with an `ANNOUNCEMENT` tag referencing the earlier issue.
> _Due to space limitations only the most recent critical Announcement issue can be pinned to the top of the issues page - but it will be pinned!_
> We recommend any system administrator either subscribe to the GitHub issues page for this repository, or set a periodic reminder to check in and manually watch any active Announcement or Infrastructure issues. Infrastructure issues will be closed when details are completed, announcements will be closed only after an extended amount of time has passed +).


### Configuring SSL/TLS
> [!IMPORTANT]
> Because Beiwe collects sensitive data that may interact with laws covering [PII](https://en.wikipedia.org/wiki/Personal_data) or [PHI](https://en.wikipedia.org/wiki/Protected_health_information) like [HIPAA](https://en.wikipedia.org/wiki/Health_Insurance_Portability_and_Accountability_Act) in the United States, the platform makes it a <b>fundamental requirement</b> that you to add an SSL certificate so that web traffic is encrypted. The platform will not be visible or accessible in any way without that SSL certificate. Please look through the [existing issues relating to SSL](https://github.com/onnela-lab/beiwe-backend/issues?q=is%3Aissue+ssl) while troubleshooting.

We recommend using [the AWS Certificate Manager] (http://docs.aws.amazon.com/acm/latest/userguide/gs-acm-request.html) service as that will integrate with AWS' Route53 service and centralize your domain management. The AWS Certificate Manager [will check that you control the domain by sending verification emails](http://docs.aws.amazon.com/acm/latest/userguide/gs-acm-validate.html) to the email addresses in the domain's WHOIS listing. If you are using an external certificate you will need to provide it to AWS' key management service, and then associate it with your Elastic Load Balancer.

After deployment is completed you will need to enable the port 443 forwarding on the Elastic Load Balancer for your Elastic Beanstalk environment. This is done in the AWS online console, in the EC2 service, in the Load Balancers section. You will need to add a listener for port 443, and then add a rule to forward traffic from port 443 to port 80 on the instances in your Elastic Beanstalk environment.


### Configuring Push Notifications
> [!WARNING]
> Push Notifications Credentials are currently <b>required</b> for the iOS app to have functional survey notifications, but they also unlock additional scheduling features for surveys on Android. Some other features of the platform require push notification credentials be present.

Due to technical limitations Onnela Lab must provide Push Notification Credentials directly, but cannot post them inside this repository. [You can find our platform maintainer's email address on this github issue](https://github.com/onnela-lab/beiwe-backend/issues/100), please email them to request push notification credentials and include with some basic contact information.

The apps send unique push notification "tokens" to your Beiwe Backend instance and _only_ your instance, these tokens are required in order to send out push notifications. No other Beiwe Backend instance can send push notifications to your study participants.


***

### Configuration And Settings

> [!IMPORTANT]
> ### Required Settings
> If any of these environment options are not provided Beiwe will not run.
> Empty strings and `None` are considered invalid.

```
FLASK_SECRET_KEY - a unique, cryptographically secure string
AWS_ACCESS_KEY_ID - AWS access key for S3
AWS_SECRET_ACCESS_KEY - AWS secret key for S3
S3_BUCKET - the bucket for storing app-generated data
RDS_DB_NAME - postgress database name (the name of the database inside of postgres)
RDS_USERNAME - database username
RDS_PASSWORD - database password
RDS_HOSTNAME - database IP address or url
S3_ACCESS_CREDENTIALS_USER - the user id for s3 access for your deployment
S3_ACCESS_CREDENTIALS_KEY - the secret key for s3 access for your deployment
SYSADMIN_EMAILS - (This item is in the process of being deprecated and has no effect)
```

### Optional Settings
There are additional settings that you will find documented directly in the [`config/settings.py`](config/settings.py) file.

If you find an issue in the [`config/django_settings.py`](config/django_settings.py) file or need to customize a variable that is not currently exposed, please open an issue on this repository.

> [!TIP]
> We _strongly_ recommend making a Sentry.io account and adding Sentry DSNs to all your Beiwe servers.  Without these, or at least a Python stack trace, there is very little data to work with when something goes wrong.


### Forest

The [Forest](https://github.com/onnela-lab/forest) integration for deriving useful real-world metrics from raw data is still under active development at Onnela Lab. [You can find documentation for Forest here.](https://forest.beiwe.org/en/latest/)

> [!Caution]
> The Oak and Jasmine Forest "Trees" require large amounts of storage space, it is possible for a data processing server to run out of free space mid-run, endangering stability and blocking the analysis from finishing. This occurs because the Accelerometer and Gyro data streams can collect huge quantities of data.
>
> We are actively working to reduce these requirements, currently the solution is to mount a large EBS volume on the data processing server and mount it over `/tmp/forest/` with permissive read/write settings.



# Development setup
How to set up beiwe-backend running on a development machine (NOT a production instance!  For a production instance,
see https://github.com/onnela-lab/beiwe-backend/wiki/Deployment-Instructions---Scalable-Deployment)

#### Before starting:
While it is possible to run your development environment inside of the system Python environment, this practice is _generally strongly discouraged_. Please familiarize yourself with one of the following: Python's [venv](https://docs.python.org/3/tutorial/venv.html) library (basic virtual environments), [Pyenv](https://github.com/pyenv/pyenv) (allows for compiling particular target versions of Python, plus some quality-of-life command-line shell integrations), or [Conda](https://docs.conda.io/en/latest/) (another option, includes integrations with non-Python libraries).  Note also that the codebase expets Python version 3.12.

#### Instructions assuming an Ubuntu platform:
Summary: the Beiwe Backend requires a PostgreSQL database, 

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

For additional tips on running a local development enironment please see the [Tips For Local Development](https://github.com/onnela-lab/beiwe-backend/wiki/Tips-For-Local-Beiwe-Development) wiki page.  If you are having difficulty getting started, or believe you could assist with any issues of documentation, please post an issue with a `documentation` tag.

### Local Celery and RabbitMQ
**Update**: it is no longer necessary to configure Celery and RabbitMQ just to run the tasks, though you still need it to be installed in your Python environment in order to avoid import errors.  A full test of Celery requires the full setup below, including installing `rabbitmq`, but as long as the file for the rabbitmq host server IP and password (declared in a `manager_ip` in the root of the repository) is missing you will instead be presented with output similar to the example shell session below. indicating a that you are running in a _much_ more convenient (and single-threaded) testing mode:

``` ipython
In [1]: from services.celery_data_processing import *
task declared, args: (), kwargs:{'queue': 'data_processing'}
Instantiating a FalseCeleryApp for celery_process_file_chunks.
```

The [Tips For Local Development](https://github.com/onnela-lab/beiwe-backend/wiki/Tips-For-Local-Beiwe-Development) page contains info on running the iPython Django database shell.

#### RabbitMQ setup
For those souls brave enough to run the entire broker queue and Celery task dispatch machinery locally, here are our best instructions. Also, due to the use of the system's `service` command it is incompatible with the varient of Ubuntu for use on the Windows Subsystem for Linux.  Have at:

1. Install RabbitMQ (https://docs.celeryproject.org/en/latest/getting-started/backends-and-brokers/rabbitmq.html#broker-rabbitmq)
    1. Edit `/etc/rabbitmq/rabbitmq-env.conf` and add the line `NODE_PORT=50000` and then a `RABBITMQ_DIST_PORT=50002`
    2. Restart RabbitMQ like this in the Bash shell: `time sudo service rabbitmq-server restart` (`time` isn't necessary, but it tells you that the command has finished, and how long the command took to execute... which can be... random and excessive?)
2. Create a file called `manager_ip` in the top level of your `beiwe-backend` repo, and enter these two lines in it.  Do not provide a trailing new-line character.
    ```
    127.0.0.1:50000
    [YOUR DESIRED PASSWORD]
    ```
    Where the password is the one you set when setting up RabbitMQ
3. run this command to create a user for rabbitmq: `rabbitmqctl add_user beiwe [INSERT THAT PASSWORD HERE]`
4. run this command to allow the beiwe user access to the appropriate queues: `sudo rabbitmqctl set_permissions -p / beiwe ".*" ".*" ".*"`
5. If you intend to test Firbase push notifications you will need to upload functional firebase credentials on the local website interface.
6. To execute push notification tasks run this command _while inside the root of the repo_: `celery -A services.celery_push_notifications worker -Q push_notifications --loglevel=info -Ofair --hostname=%%h_notifications --concurrency=20 --pool=threads`
7. To run data processing tasks run this command _while inside the root of the repo_: `celery -A services.celery_data_processing worker -Q data_processing --loglevel=info -Ofair --hostname=%%h_processing`
8. To run forest tasks run this comand _while inside the root of the repo_: `celery -A services.celery_forest worker -Q forest_queue --loglevel=info -Ofair --hostname=%%h_forest` (Forest is still in beta.)
9. Run this command to dispatch new tasks, which will then be consumed by the Celery processes, _while inside the root of the repo_. `python services/cron.py five_minutes`
