# Generated by Django 4.2.11 on 2024-06-04 06:00

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0124_remove_apikey_has_tableau_api_permissions_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='participant',
            name='first_register_user',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
