# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [("stampede", "0001_initial")]

    operations = [
        migrations.AddField('Job','cpld',models.FloatField(null=True)),
        migrations.AddField('Job','Load_L1Hits',models.BigIntegerField(null=True)),
        migrations.AddField('Job','Load_L2Hits',models.BigIntegerField(null=True)),
        migrations.AddField('Job','Load_LLCHits',models.BigIntegerField(null=True)),
        
    ]
