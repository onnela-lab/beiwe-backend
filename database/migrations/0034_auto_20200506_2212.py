# Generated by Django 2.2.11 on 2020-05-06 22:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0033_auto_20200423_0101'),
    ]

    operations = [
        migrations.AlterField(
            model_name='lineencryptionerror',
            name='base64_decryption_key',
            field=models.TextField(),
        ),
    ]
