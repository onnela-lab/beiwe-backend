# Generated by Django 2.2.19 on 2021-07-06 19:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0058_auto_20210701_2222'),
    ]

    operations = [
        migrations.AddField(
            model_name='devicesettings',
            name='ambient_audio',
            field=models.BooleanField(default=False),
        ),
    ]
