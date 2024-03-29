# Generated by Django 3.2.12 on 2022-03-30 18:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0072_auto_20220323_2322'),
    ]

    operations = [
        migrations.AlterField(
            model_name='forestparam',
            name='default',
            field=models.BooleanField(null=True, unique=True),
        ),
        migrations.AlterField(
            model_name='foresttask',
            name='forest_output_exists',
            field=models.BooleanField(null=True),
        ),
    ]
