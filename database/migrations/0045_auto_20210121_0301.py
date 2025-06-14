# Generated by Django 2.2.14 on 2021-01-21 03:01

from django.db import migrations, models


# this migration used to use an external library called django-timezone-field it uses a charfield, it
# didn't work, we got rid of it, this (should) now be manually patched out.  That field type
# should just be a CharField with a max_length of 63.  The default value was 'America/New_York'.


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0044_auto_20210115_2300'),
    ]

    operations = [
        migrations.AddField(
            model_name='participant',
            name='push_notification_unreachable',
            field=models.SmallIntegerField(default=True),
        ),
        migrations.AddField(
            model_name='participant',
            name='timezone',
            field=models.CharField(default='America/New_York'),
        ),
    ]
